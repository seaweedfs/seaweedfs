//go:build !freebsd

package mount

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// proxyVolume is the filer's chunk proxy for a mount with
// VolumeServerAccess=filerProxy: chunk reads and uploads go to the filer's
// HTTP port, and the file ids for uploads come from its AssignVolume.
type proxyVolume struct {
	*g6ConditionalPeer
	mu      sync.Mutex
	stored  map[string][]byte
	assigns int
	commits int
	merges  int
}

func (p *proxyVolume) AssignVolume(context.Context, *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.assigns++
	return &filer_pb.AssignVolumeResponse{
		FileId:   fmt.Sprintf("9,%x00000001", p.assigns),
		Count:    1,
		Location: &filer_pb.Location{Url: "127.0.0.1:1", PublicUrl: "127.0.0.1:1"},
	}, nil
}

// CreateEntry commits like the filer: the chunks the previous entry named and
// the new one does not are deleted with the commit, as a vacuum leaves them.
// These tests write no chunk manifests, so the lists name every chunk.
func (p *proxyVolume) CreateEntry(ctx context.Context, r *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	p.g6ConditionalPeer.mu.Lock()
	var before []*filer_pb.FileChunk
	if p.entry != nil {
		before = p.entry.Chunks
	}
	p.g6ConditionalPeer.mu.Unlock()
	resp, err := p.g6ConditionalPeer.CreateEntry(ctx, r)
	if err != nil || resp.GetError() != "" {
		return resp, err
	}
	inBefore := make(map[string]bool, len(before))
	for _, c := range before {
		inBefore[c.GetFileIdString()] = true
	}
	kept := make(map[string]bool, len(r.GetEntry().GetChunks()))
	// maybeMergeChunks replaces every chunk the entry named.
	merged := len(before) > 0
	for _, c := range r.GetEntry().GetChunks() {
		kept[c.GetFileIdString()] = true
		merged = merged && !inBefore[c.GetFileIdString()]
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commits++
	if merged {
		p.merges++
	}
	for _, c := range before {
		if !kept[c.GetFileIdString()] {
			delete(p.stored, c.GetFileIdString())
		}
	}
	return resp, nil
}

func (p *proxyVolume) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fid := r.URL.Query().Get("proxyChunkId")
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		p.mu.Lock()
		body, ok := p.stored[fid]
		p.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(body))
		return
	}
	var data []byte
	var err error
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
		var part io.Reader
		if mr, mrErr := r.MultipartReader(); mrErr != nil {
			err = mrErr
		} else if part, err = mr.NextPart(); err == nil {
			data, err = io.ReadAll(part)
		}
	} else {
		data, err = io.ReadAll(r.Body)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	p.stored[fid] = data
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{"size": len(data), "eTag": "test"})
}

func (p *proxyVolume) counts() (commits, merges int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.commits, p.merges
}

// committed returns the filer's entry and the content its chunks hold, a
// newer chunk covering an older one.
func (p *proxyVolume) committed(t *testing.T) (*filer_pb.Entry, []byte) {
	t.Helper()
	p.g6ConditionalPeer.mu.Lock()
	entry := proto.Clone(p.entry).(*filer_pb.Entry)
	p.g6ConditionalPeer.mu.Unlock()
	chunks := append([]*filer_pb.FileChunk(nil), entry.Chunks...)
	sort.SliceStable(chunks, func(x, y int) bool { return chunks[x].ModifiedTsNs < chunks[y].ModifiedTsNs })
	content := make([]byte, entry.Attributes.FileSize)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range chunks {
		body, ok := p.stored[c.GetFileIdString()]
		if !ok || uint64(len(body)) != c.Size || c.Offset+int64(c.Size) > int64(len(content)) {
			t.Fatalf("the filer names a chunk that is not stored as named: %v", c)
		}
		copy(content[c.Offset:], body)
	}
	return entry, content
}

// startProxyFiler serves chunks with proxy and the filer API with filerServer.
// Its gRPC port is the HTTP port plus 10000, the default, so the plain
// host:port address names both: the chunk proxy builds upload URLs from the
// address string itself.
func startProxyFiler(t *testing.T, w *WFS, proxy http.Handler, filerServer filer_pb.SeaweedFilerServer) {
	t.Helper()
	for attempt := 0; ; attempt++ {
		srv := httptest.NewServer(proxy)
		_, portText, _ := net.SplitHostPort(srv.Listener.Addr().String())
		port, _ := strconv.Atoi(portText)
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+10000))
		if err != nil {
			srv.Close()
			if attempt < 50 {
				continue
			}
			t.Fatalf("no free gRPC port next to an HTTP port: %v", err)
		}
		t.Cleanup(srv.Close)
		server := pb.NewGrpcServer()
		filer_pb.RegisterSeaweedFilerServer(server, filerServer)
		go server.Serve(listener)
		t.Cleanup(server.Stop)
		w.option.FilerAddresses = []pb.ServerAddress{pb.ServerAddress(srv.Listener.Addr().String())}
		return
	}
}

// openProxyHandle opens /dir/file, which the filer vol holds as base, on a
// mount whose chunk requests go to proxy. Uploads run four at a time.
func openProxyHandle(t *testing.T, vol *proxyVolume, base *filer_pb.Entry, proxy http.Handler) (*WFS, *FileHandle, uint64) {
	t.Helper()
	w := newInvalidateTestWFS(t)
	w.option.CacheDirForWrite = t.TempDir()
	w.option.DefaultPermissions = true
	w.posixLocks = NewPosixLockTable()
	w.option.ConcurrentWriters = 4
	w.concurrentWriters = util.NewLimitedConcurrentExecutor(4)
	vol.g6ConditionalPeer = &g6ConditionalPeer{entry: proto.Clone(base).(*filer_pb.Entry)}
	startProxyFiler(t, w, proxy, vol)
	w.inodeToPath.Lookup("/dir", 1, true, false, 0, false)
	w.inodeToPath.Lookup("/dir/file", 1, false, false, 42, true)
	w.inodeToPath.MarkChildrenCached("/dir")
	if err := w.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/dir", proto.Clone(base).(*filer_pb.Entry)), 1); err != nil {
		t.Fatal(err)
	}
	out := &fuse.OpenOut{}
	if code := w.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 42}, Flags: syscall.O_RDWR}, out); code != fuse.OK {
		t.Fatal(code)
	}
	fh := w.GetHandle(FileHandleId(out.Fh))
	t.Cleanup(fh.dirtyPages.Destroy)
	return w, fh, out.Fh
}

// readView reads size bytes through the handle's read view, as a read that
// misses the dirty pages does.
func readView(t *testing.T, fh *FileHandle, size int) []byte {
	t.Helper()
	buf := make([]byte, size)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, _, err := fh.entryChunkGroup.ReadDataAt(ctx, int64(size), buf, 0)
	if err != nil && err != io.EOF {
		t.Fatalf("read through the open handle: %v", err)
	}
	return buf[:n]
}

// A merge replaces every chunk it reads, while uploads attach their chunks
// without the handle lock, so a flush merges only when no upload is pending.
// When it does merge, reads through the open handle must follow it: the
// commit deletes the replaced chunks.
func TestFlushMergeThroughChunkProxy(t *testing.T) {
	payload := func(seed uint32) []byte {
		b := make([]byte, 256)
		x := seed * 2654435761
		for i := range b {
			x = x*1103515245 + 12345
			b[i] = byte(x >> 16)
		}
		return b
	}
	chunk := func(key uint64, offset int64) *filer_pb.FileChunk {
		return &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: key, Cookie: 1}, Offset: offset, Size: 256, ModifiedTsNs: int64(key)}
	}
	// Three overlapping 256-byte chunks hold a 300-byte file: 768 bytes
	// stored, more than twice the content.
	a, b, c := chunk(1, 0), chunk(2, 22), chunk(3, 44)
	pa, pb, pc := payload(1), payload(2), payload(3)
	base := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{
		Inode: 42, Uid: 2000, Gid: 20, FileMode: 0644, FileSize: 300, Mtime: 10,
	}, Chunks: []*filer_pb.FileChunk{a, b, c}, CommitTsNs: 100}
	content := append(append(append([]byte{}, pa[:22]...), pb[:22]...), pc...)
	newVolume := func() *proxyVolume {
		return &proxyVolume{stored: map[string][]byte{
			a.GetFileIdString(): pa, b.GetFileIdString(): pb, c.GetFileIdString(): pc,
		}}
	}
	// flush publishes as doFlush does after its own drain, which it reaches
	// only for a handle with changes to publish.
	flush := func(t *testing.T, w *WFS, fh *FileHandle, publication *contentPublication) {
		t.Helper()
		lock := w.fhLockTable.AcquireLock("test flush", fh.fh, util.ExclusiveLock)
		fh.dirtyMetadata = true
		w.fhLockTable.ReleaseLock(fh.fh, lock)
		if err := w.flushMetadataToFiler(context.Background(), fh, "/dir", "file", 0, 0, nil, publication); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	t.Run("Quiet", func(t *testing.T) {
		vol := newVolume()
		w, fh, _ := openProxyHandle(t, vol, base, vol)
		flush(t, w, fh, &contentPublication{writes: fh.pageWrites.Load()})

		entry, committed := vol.committed(t)
		if len(entry.Chunks) != 1 || entry.Chunks[0].ModifiedTsNs != a.ModifiedTsNs {
			t.Fatalf("the quiet flush did not merge into one chunk stamped as the oldest data it holds: %v", entry.Chunks)
		}
		vol.mu.Lock()
		blobs := len(vol.stored)
		vol.mu.Unlock()
		if blobs != 1 || !bytes.Equal(committed, content) {
			t.Fatalf("after the merge %d chunks are stored, want only the merged one, with the file's content", blobs)
		}
		if got := readView(t, fh, len(content)); !bytes.Equal(got, content) {
			t.Fatalf("read %d bytes through the open handle after the merge, not the merged content", len(got))
		}
	})

	// An older page is still uploading when a newer page of the same bytes
	// has attached. The flush leaves the merge to a later one, and the older
	// page, attaching last, must not outrank the newer one.
	t.Run("UploadInFlight", func(t *testing.T) {
		vol := newVolume()
		var hold atomic.Bool
		uploading, release := make(chan struct{}), make(chan struct{})
		w, fh, handle := openProxyHandle(t, vol, base, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet && r.Method != http.MethodHead && hold.CompareAndSwap(true, false) {
				close(uploading)
				<-release
			}
			vol.ServeHTTP(rw, r)
		}))
		// Let a held upload go however the test ends: the proxy's cleanup,
		// which runs after this one, waits for its handlers.
		var released sync.Once
		releaseUpload := func() { released.Do(func() { close(release) }) }
		t.Cleanup(releaseUpload)
		await := func(done <-chan struct{}, what string) {
			t.Helper()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for %s", what)
			}
		}
		write := func(data []byte) {
			t.Helper()
			if n, code := w.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}, Fh: handle, Offset: 100, Size: uint32(len(data))}, data); int(n) != len(data) || code != fuse.OK {
				t.Fatalf("write: %d %v", n, code)
			}
		}
		pipeline := fh.dirtyPages.randomWriter.(*ChunkedDirtyPages).uploadPipeline
		drained := make(chan struct{}, 2)
		drain := func() { pipeline.FlushAll(); drained <- struct{}{} }

		// The flush's own drain ran before both writes.
		publication := &contentPublication{writes: fh.pageWrites.Load()}
		// Another flush drains the older page, whose upload hangs.
		write(bytes.Repeat([]byte{'o'}, 40))
		hold.Store(true)
		go drain()
		await(uploading, "the older page to upload")
		// A third drains the newer page, which uploads and attaches.
		newer := bytes.Repeat([]byte{'n'}, 40)
		write(newer)
		go drain()
		for deadline := time.Now().Add(5 * time.Second); len(fh.GetEntry().GetChunks()) < len(base.Chunks)+1; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the newer page did not attach")
			}
		}
		flush(t, w, fh, publication)
		releaseUpload()
		await(drained, "the drains")
		await(drained, "the drains")

		want := append([]byte{}, content...)
		copy(want[100:], newer)
		written := func(b []byte) []byte {
			if len(b) < 140 {
				return b
			}
			return b[100:140]
		}
		if got := readView(t, fh, len(want)); !bytes.Equal(got, want) {
			t.Fatalf("read %q at [100,140) through the open handle, want the newer page", written(got))
		}
		if _, merges := vol.counts(); merges != 0 {
			t.Fatal("the flush merged while an upload was in flight")
		}
		// With no upload left, the next flush merges.
		flush(t, w, fh, &contentPublication{writes: fh.pageWrites.Load()})
		if _, merges := vol.counts(); merges != 1 {
			t.Fatalf("%d merges once the uploads were done, want 1", merges)
		}
		if _, committed := vol.committed(t); !bytes.Equal(committed, want) {
			t.Fatalf("the filer holds %q at [100,140), want the newer page", written(committed))
		}
		if got := readView(t, fh, len(want)); !bytes.Equal(got, want) {
			t.Fatalf("read %q at [100,140) through the open handle after the merge, want the newer page", written(got))
		}
	})
}

// Writes, flushes and reads run together on one handle while overwrites keep
// its chunks bloated, so flushes merge when no upload is pending and leave the
// merge to a later one otherwise. Uploads take a random while,
// and each commit deletes the chunks it replaced. Every read must succeed
// promptly, and in the end the handle and the filer must both hold exactly
// what was written.
func TestFlushMergeRacesWithWritesAndReads(t *testing.T) {
	const writers, region, writesPerWriter = 4, 2048, 100
	size := writers * region

	// Each writer owns one region, which starts as one stored chunk.
	want := make([]byte, size)
	base := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{
		Inode: 42, Uid: 2000, Gid: 20, FileMode: 0644, FileSize: uint64(size), Mtime: 10,
	}, CommitTsNs: 100}
	vol := &proxyVolume{stored: map[string][]byte{}}
	for i := range writers {
		c := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: uint64(i + 1), Cookie: 1}, Offset: int64(i * region), Size: region, ModifiedTsNs: int64(i + 1)}
		body := want[i*region : (i+1)*region]
		for j := range body {
			body[j] = byte(rand.Uint32())
		}
		vol.stored[c.GetFileIdString()] = append([]byte(nil), body...)
		base.Chunks = append(base.Chunks, c)
	}
	w, fh, handle := openProxyHandle(t, vol, base, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			time.Sleep(rand.N(2 * time.Millisecond))
		}
		vol.ServeHTTP(rw, r)
	}))
	header := fuse.InHeader{NodeId: 42}

	// doFlush's steps: record the page count, drain the pages, publish if
	// dirty. doFlush reads the handle's flags without the handle lock, so
	// this drains through the pipeline and reads them under the lock.
	pages := fh.dirtyPages.randomWriter.(*ChunkedDirtyPages)
	flush := func() error {
		publication := &contentPublication{writes: fh.pageWrites.Load()}
		pages.uploadPipeline.FlushAll()
		if err := pages.LastError(); err != nil {
			return err
		}
		lock := w.fhLockTable.AcquireLock("test flush", fh.fh, util.SharedLock)
		dirty := fh.dirtyMetadata
		w.fhLockTable.ReleaseLock(fh.fh, lock)
		if !dirty {
			return nil
		}
		return w.flushMetadataToFiler(context.Background(), fh, "/dir", "file", 0, 0, nil, publication)
	}
	read := func(offset, n int) ([]byte, fuse.Status, time.Duration) {
		buf := make([]byte, n)
		start := time.Now()
		result, code := w.Read(nil, &fuse.ReadIn{InHeader: header, Fh: handle, Offset: uint64(offset), Size: uint32(n)}, buf)
		if code != fuse.OK {
			return nil, code, time.Since(start)
		}
		data, code := result.Bytes(buf)
		return data, code, time.Since(start)
	}

	// One write, flush and read first, so the first upload and the first
	// chunk read do not race each other through the clients' lazy setup.
	first := []byte("first write")
	if n, code := w.Write(nil, &fuse.WriteIn{InHeader: header, Fh: handle, Size: uint32(len(first))}, first); code != fuse.OK || int(n) != len(first) {
		t.Fatalf("first write: %d %v", n, code)
	}
	copy(want, first)
	if err := flush(); err != nil {
		t.Fatalf("first flush: %v", err)
	}
	if got, code, _ := read(0, size); code != fuse.OK || !bytes.Equal(got, want) {
		t.Fatalf("first read: %v", code)
	}

	stop := make(chan struct{})
	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}
	var background sync.WaitGroup
	// Several flushers, so one can drain while another publishes.
	for range 3 {
		background.Go(func() {
			for !stopped() {
				if err := flush(); err != nil {
					t.Errorf("flush: %v", err)
					return
				}
			}
		})
	}
	background.Go(func() {
		for !stopped() {
			offset := rand.IntN(size)
			n := 1 + rand.IntN(1024)
			if offset+n > size {
				n = size - offset
			}
			if _, code, took := read(offset, n); code != fuse.OK || took > 2*time.Second {
				t.Errorf("read [%d,%d): %v after %v", offset, offset+n, code, took)
				return
			}
		}
	})
	// Now and then the writers pause for a few commits, so the uploads finish
	// and a flush merges.
	var pause sync.RWMutex
	background.Go(func() {
		for !stopped() {
			time.Sleep(rand.N(10 * time.Millisecond))
			pause.Lock()
			commits, merges := vol.counts()
			for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
				if c, m := vol.counts(); c >= commits+4 || m > merges {
					break
				}
			}
			pause.Unlock()
		}
	})

	var writes sync.WaitGroup
	for i := range writers {
		writes.Go(func() {
			mine := want[i*region : (i+1)*region]
			for range writesPerWriter {
				n := 1 + rand.IntN(region/2)
				at := rand.IntN(region - n + 1)
				data := make([]byte, n)
				for j := range data {
					data[j] = byte(rand.Uint32())
				}
				pause.RLock()
				written, code := w.Write(nil, &fuse.WriteIn{InHeader: header, Fh: handle, Offset: uint64(i*region + at), Size: uint32(n)}, data)
				pause.RUnlock()
				if code != fuse.OK || int(written) != n {
					t.Errorf("write [%d,%d): %d %v", i*region+at, i*region+at+n, written, code)
					return
				}
				copy(mine[at:], data)
			}
		})
	}
	writes.Wait()
	_, mergedWhileWriting := vol.counts()
	close(stop)
	background.Wait()
	if t.Failed() {
		return
	}
	if err := flush(); err != nil {
		t.Fatalf("last flush: %v", err)
	}
	commits, merges := vol.counts()
	t.Logf("%d commits, %d merges while writing, %d in all", commits, mergedWhileWriting, merges)
	if mergedWhileWriting == 0 {
		t.Fatal("no flush merged while the writes ran")
	}
	if got, code, _ := read(0, size); code != fuse.OK || !bytes.Equal(got, want) {
		t.Fatalf("read back through the handle: %v, %d bytes, as written: %v", code, len(got), bytes.Equal(got, want))
	}
	if _, committed := vol.committed(t); !bytes.Equal(committed, want) {
		t.Fatal("the filer's entry does not hold what was written")
	}
}

// A merge of a sparse file must keep the data past each hole, and leave the
// holes unwritten.
func TestFlushMergeKeepsDataPastAHole(t *testing.T) {
	var chunks []*filer_pb.FileChunk
	vol := &proxyVolume{stored: map[string][]byte{}}
	// Overlapping 256-byte chunks over [0,300) and [512,856), a hole between.
	for i, offset := range []int64{0, 22, 44, 512, 534, 556, 578, 600} {
		c := &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: uint64(i + 1), Cookie: 1}, Offset: offset, Size: 256, ModifiedTsNs: int64(i + 1)}
		b := make([]byte, 256)
		x := uint32(i+1) * 2654435761
		for j := range b {
			x = x*1103515245 + 12345
			b[j] = byte(x >> 16)
		}
		vol.stored[c.GetFileIdString()] = b
		chunks = append(chunks, c)
	}
	const fileSize = 856
	want := make([]byte, fileSize)
	for _, c := range chunks {
		copy(want[c.Offset:], vol.stored[c.GetFileIdString()])
	}
	base := &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{
		Inode: 42, Uid: 2000, Gid: 20, FileMode: 0644, FileSize: fileSize, Mtime: 10,
	}, Chunks: chunks, CommitTsNs: 100}

	w, fh, _ := openProxyHandle(t, vol, base, vol)
	lock := w.fhLockTable.AcquireLock("test flush", fh.fh, util.ExclusiveLock)
	fh.dirtyMetadata = true
	w.fhLockTable.ReleaseLock(fh.fh, lock)
	if err := w.flushMetadataToFiler(context.Background(), fh, "/dir", "file", 0, 0, nil, &contentPublication{writes: fh.pageWrites.Load()}); err != nil {
		t.Fatalf("flush: %v", err)
	}

	entry, got := vol.committed(t)
	if _, merges := vol.counts(); merges != 1 {
		t.Fatalf("the flush did not merge the bloated file: %v", entry.Chunks)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the merge changed the file's content: [0,300) kept %v, hole zero %v, [512,856) kept %v",
			bytes.Equal(got[:300], want[:300]), bytes.Equal(got[300:512], want[300:512]), bytes.Equal(got[512:], want[512:]))
	}
	for _, c := range entry.Chunks {
		if c.Offset < 512 && c.Offset+int64(c.Size) > 300 {
			t.Errorf("merged chunk [%d,%d) writes into the hole [300,512)", c.Offset, c.Offset+int64(c.Size))
		}
	}
}
