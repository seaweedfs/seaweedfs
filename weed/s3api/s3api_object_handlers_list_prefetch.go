package s3api

import (
	"context"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/protobuf/proto"
)

const (
	listPrefetchWorkers = 8
	listPrefetchAhead   = 64
	listPrefetchWindow  = 32
	listPrefetchQueued  = 4096
	listPrefetchBytes   = 128 << 10
	listReadAheadBytes  = 1 << 20
)

// listPrefetcher reads directories ahead of a recursive listing walk. The walk enters
// directories one at a time, so a tree of many small directories costs one filer round
// trip per directory, in series. The prefetcher keeps several of those trips in flight:
// it reads the first window of each directory the walk will enter, nearest first in
// walk order ("<path>/" byte order), and hands the window over when the walk gets there.
// A directory the walk has passed is dropped. The windows held or in flight are capped
// at ahead, which starts at the worker count and doubles up to maxAhead while the walk
// keeps finding directories that end inside their first window; a larger directory
// keeps the walk busy on its own, and the cap falls back so a page ending inside it
// leaves few windows unread. A window also ends at listPrefetchBytes of entries, and
// the walk reads the rest of that directory itself.
type listPrefetcher struct {
	ctx      context.Context
	cancel   context.CancelFunc
	client   filer_pb.SeaweedFilerClient
	window   uint32
	maxAhead int
	workers  int

	running  sync.WaitGroup
	mu       sync.Mutex
	wake     *sync.Cond
	streamed *sync.Cond
	closed   bool
	started  bool
	ahead    int
	pos      string
	queued   []string
	windows  map[string]*prefetchedWindow
}

type prefetchedWindow struct {
	done    chan struct{}
	entries []*filer_pb.Entry
	more    bool
	err     error
}

func newListPrefetcher(ctx context.Context, client filer_pb.SeaweedFilerClient, maxKeys uint16) *listPrefetcher {
	p := &listPrefetcher{
		client:   client,
		window:   min(uint32(listPrefetchWindow), uint32(maxKeys)+2),
		maxAhead: min(listPrefetchAhead, int(maxKeys)),
		windows:  map[string]*prefetchedWindow{},
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.workers = min(listPrefetchWorkers, p.maxAhead)
	p.ahead = p.workers
	p.wake = sync.NewCond(&p.mu)
	p.streamed = sync.NewCond(&p.mu)
	return p
}

// listWalkDescends reports whether a recursive walk lists the directory's children.
func listWalkDescends(entry *filer_pb.Entry) bool {
	return entry.IsDirectory && entry.Name != s3_constants.MultipartUploadsFolder &&
		!strings.HasSuffix(entry.Name, s3_constants.VersionsFolder)
}

func (p *listPrefetcher) add(dir string) {
	if p == nil {
		return
	}
	key := dir + "/"
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || key <= p.pos || p.windows[key] != nil {
		return
	}
	i := sort.SearchStrings(p.queued, key)
	if i >= listPrefetchQueued || (i < len(p.queued) && p.queued[i] == key) {
		return
	}
	p.queued = append(p.queued, "")
	copy(p.queued[i+1:], p.queued[i:])
	p.queued[i] = key
	if len(p.queued) > listPrefetchQueued {
		p.queued = p.queued[:listPrefetchQueued]
	}
	if !p.started {
		p.started = true
		p.running.Add(p.workers)
		for range p.workers {
			go p.run()
		}
	}
	p.wake.Signal()
}

// take hands over the prefetched window for the walk's request, along with the limit
// the walk must compare the window against: a window shorter than that limit is the
// end of the directory. ok is false when the request has to go to the filer.
func (p *listPrefetcher) take(request *filer_pb.ListEntriesRequest) (entries []*filer_pb.Entry, limit uint32, ok bool) {
	if p == nil {
		return nil, 0, false
	}
	key := request.Directory + "/"
	p.mu.Lock()
	if key > p.pos {
		p.pos = key
		n := sort.SearchStrings(p.queued, key)
		if n < len(p.queued) && p.queued[n] == key {
			n++
		}
		p.queued = p.queued[n:]
		for k := range p.windows {
			if k < key {
				delete(p.windows, k)
			}
		}
	}
	w := p.windows[key]
	delete(p.windows, key)
	p.wake.Broadcast()
	p.mu.Unlock()

	if w == nil || request.Prefix != "" || request.StartFromFileName != "" {
		return nil, 0, false
	}
	select {
	case <-w.done:
	case <-p.ctx.Done():
		return nil, 0, false
	}
	if w.err != nil {
		return nil, 0, false
	}
	p.mu.Lock()
	if w.more {
		p.ahead = p.workers
	} else {
		p.ahead = min(p.ahead*2, p.maxAhead)
		p.wake.Broadcast()
	}
	p.mu.Unlock()
	entries, limit = w.entries, request.Limit
	if uint32(len(entries)) >= limit {
		entries = entries[:limit]
	} else if w.more {
		limit = uint32(len(entries))
	}
	return entries, limit, true
}

// close stops the reads and waits for them, so none outlives the filer client.
func (p *listPrefetcher) close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.closed = true
	p.wake.Broadcast()
	p.streamed.Broadcast()
	p.mu.Unlock()
	p.cancel()
	p.running.Wait()
}

func (p *listPrefetcher) run() {
	defer p.running.Done()
	for {
		p.mu.Lock()
		for !p.closed && (len(p.queued) == 0 || len(p.windows) >= p.ahead) {
			p.wake.Wait()
		}
		if p.closed {
			p.mu.Unlock()
			return
		}
		key := p.queued[0]
		p.queued = p.queued[1:]
		w := &prefetchedWindow{done: make(chan struct{})}
		p.windows[key] = w
		p.mu.Unlock()

		p.read(strings.TrimSuffix(key, "/"), w)
	}
}

func (p *listPrefetcher) read(dir string, w *prefetchedWindow) {
	defer close(w.done)
	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()
	stream, err := p.client.ListEntries(ctx, &filer_pb.ListEntriesRequest{Directory: dir, Limit: p.window})
	if err != nil {
		w.err = err
		return
	}
	for size := 0; ; {
		resp, recvErr := stream.Recv()
		if recvErr == io.EOF {
			w.more = uint32(len(w.entries)) == p.window
			break
		}
		if recvErr != nil {
			w.err = recvErr
			return
		}
		if resp.Entry == nil {
			continue
		}
		w.entries = append(w.entries, resp.Entry)
		if size += proto.Size(resp.Entry); size >= listPrefetchBytes {
			w.more = true
			break
		}
	}
	for _, entry := range w.entries {
		if listWalkDescends(entry) {
			p.add(dir + "/" + entry.Name)
		}
	}
}

// listAhead is the walk's own listing of a directory, read ahead of the walk: each
// directory the walk will descend into goes to the prefetcher as it arrives, so its
// siblings are read while the walk is inside the first. The walk still takes each
// entry as it arrives, and at most listReadAheadBytes of entries wait for it.
type listAhead struct {
	p       *listPrefetcher
	entries []*filer_pb.Entry
	sizes   []int
	size    int
	err     error
}

// list opens the walk's listing of a directory; descends picks the entries to prefetch.
func (p *listPrefetcher) list(ctx context.Context, client filer_pb.SeaweedFilerClient, request *filer_pb.ListEntriesRequest, descends func(*filer_pb.Entry) bool) (listEntriesStream, error) {
	if p == nil {
		return client.ListEntries(ctx, request)
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return client.ListEntries(ctx, request)
	}
	p.running.Add(1)
	p.mu.Unlock()
	stream, err := client.ListEntries(p.ctx, request)
	if err != nil {
		p.running.Done()
		return nil, err
	}
	a := &listAhead{p: p}
	go a.read(stream, request.Directory, descends)
	return a, nil
}

func (a *listAhead) read(stream listEntriesStream, dir string, descends func(*filer_pb.Entry) bool) {
	p := a.p
	defer p.running.Done()
	for {
		p.mu.Lock()
		for a.size >= listReadAheadBytes && !p.closed {
			p.streamed.Wait()
		}
		closed := p.closed
		p.mu.Unlock()
		var resp *filer_pb.ListEntriesResponse
		err := context.Canceled
		if !closed {
			resp, err = stream.Recv()
		}
		p.mu.Lock()
		if err != nil {
			a.err = err
			p.streamed.Broadcast()
			p.mu.Unlock()
			return
		}
		entry := resp.Entry
		if entry != nil {
			size := proto.Size(entry)
			a.entries = append(a.entries, entry)
			a.sizes = append(a.sizes, size)
			a.size += size
			p.streamed.Broadcast()
		}
		p.mu.Unlock()
		if entry != nil && descends(entry) {
			p.add(dir + "/" + entry.Name)
		}
	}
}

func (a *listAhead) Recv() (*filer_pb.ListEntriesResponse, error) {
	p := a.p
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(a.entries) == 0 && a.err == nil {
		p.streamed.Wait()
	}
	if len(a.entries) == 0 {
		return nil, a.err
	}
	entry := a.entries[0]
	a.entries[0] = nil
	a.entries = a.entries[1:]
	a.size -= a.sizes[0]
	a.sizes = a.sizes[1:]
	p.streamed.Broadcast()
	return &filer_pb.ListEntriesResponse{Entry: entry}, nil
}
