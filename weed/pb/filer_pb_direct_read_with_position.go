package pb

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// MessagePosition is the per-shard cursor. LogEntry.Offset is per-filer, so
// a single global cursor can't resume safely from a merged stream.
type MessagePosition struct {
	TsNs   int64
	Offset int64
}

// MaxMessagePosition pauses a shard's emission. Function-not-var so an
// importer can't mutate the sentinel.
func MaxMessagePosition() MessagePosition {
	return MessagePosition{TsNs: 1<<63 - 1, Offset: 1<<63 - 1}
}

// LessOrEqual: strict `<=` so the last resolved event isn't replayed.
func (p MessagePosition) LessOrEqual(other MessagePosition) bool {
	if p.TsNs != other.TsNs {
		return p.TsNs < other.TsNs
	}
	return p.Offset <= other.Offset
}

// EventCallback halts the read on a non-nil error.
type EventCallback func(event *filer_pb.SubscribeMetadataResponse, filerId string, position MessagePosition) error

// ReadLogFileRefsWithPosition skips events with (ts, offset) <=
// startPositions[filer_id]. Cross-filer ordering is (ts, filerId, offset)
// so retries from the same cursor see the same event sequence.
// startPositions[filer_id] = MaxMessagePosition() pauses that shard.
func ReadLogFileRefsWithPosition(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startPositions map[string]MessagePosition,
	stopTsNs int64,
	filter PathFilter,
	cb EventCallback,
) (lastByFiler map[string]MessagePosition, err error) {

	lastByFiler = make(map[string]MessagePosition)
	if len(refs) == 0 {
		return
	}

	perFiler := make(map[string][]*filer_pb.LogFileChunkRef)
	var filerOrder []string
	for _, ref := range refs {
		if len(ref.Chunks) == 0 {
			continue
		}
		if _, seen := perFiler[ref.FilerId]; !seen {
			filerOrder = append(filerOrder, ref.FilerId)
		}
		perFiler[ref.FilerId] = append(perFiler[ref.FilerId], ref)
	}
	sort.Strings(filerOrder)

	if len(filerOrder) == 0 {
		return
	}

	streams := make([]*filerStream, 0, len(filerOrder))
	var wg sync.WaitGroup
	for _, fid := range filerOrder {
		startPos := startPositions[fid]
		paused := startPos == MaxMessagePosition()
		ch := make(chan *filer_pb.LogEntry, 256)
		errCh := make(chan error, 1)
		s := &filerStream{id: fid, ch: ch, errCh: errCh, startPos: startPos, paused: paused}
		streams = append(streams, s)
		if paused {
			close(ch)
			errCh <- nil
			close(errCh)
			continue
		}
		wg.Add(1)
		go func(refs []*filer_pb.LogFileChunkRef, sendCh chan<- *filer_pb.LogEntry, sendErr chan<- error, startPos MessagePosition) {
			defer wg.Done()
			defer close(sendCh)
			err := streamEntriesWithPosition(refs, newReader, startPos, stopTsNs, sendCh)
			sendErr <- err
			close(sendErr)
		}(perFiler[fid], ch, errCh, startPos)
	}

	h := &filerStreamHeap{}
	heap.Init(h)
	for _, s := range streams {
		entry, ok := <-s.ch
		if !ok {
			continue
		}
		heap.Push(h, &filerStreamHeapItem{stream: s, entry: entry})
	}

	for h.Len() > 0 {
		top := heap.Pop(h).(*filerStreamHeapItem)
		pos := MessagePosition{TsNs: top.entry.TsNs, Offset: top.entry.Offset}
		if cbErr := dispatchOneEntryWithPosition(top.entry, top.stream.id, pos, filter, cb); cbErr != nil {
			err = cbErr
			break
		}
		lastByFiler[top.stream.id] = pos
		if next, ok := <-top.stream.ch; ok {
			heap.Push(h, &filerStreamHeapItem{stream: top.stream, entry: next})
		}
	}
	// Drain so producer goroutines don't leak.
	for h.Len() > 0 {
		heap.Pop(h)
	}
	for _, s := range streams {
		if s.paused {
			continue
		}
		for range s.ch {
		}
	}
	wg.Wait()

	if err == nil {
		for _, s := range streams {
			if s.paused {
				continue
			}
			if pErr, ok := <-s.errCh; ok && pErr != nil {
				err = fmt.Errorf("read filer %s: %w", s.id, pErr)
				break
			}
		}
	}
	return
}

func streamEntriesWithPosition(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startPos MessagePosition,
	stopTsNs int64,
	out chan<- *filer_pb.LogEntry,
) error {
	// No file-level fast-skip: FileTsNs is flush-start; the file's last
	// entry can be later. Without FileMaxTsNs on the ref the per-entry
	// predicate is the only safe gate.
	for _, ref := range refs {
		entries, err := readLogFileEntries(newReader, ref.Chunks, 0, stopTsNs)
		if err != nil {
			if isChunkNotFound(err) {
				glog.V(0).Infof("skip log file filer=%s ts=%d: %v", ref.FilerId, ref.FileTsNs, err)
				continue
			}
			return fmt.Errorf("read log file filer=%s ts=%d: %w", ref.FilerId, ref.FileTsNs, err)
		}
		for _, entry := range entries {
			pos := MessagePosition{TsNs: entry.TsNs, Offset: entry.Offset}
			if pos.LessOrEqual(startPos) {
				continue
			}
			out <- entry
		}
	}
	return nil
}

func dispatchOneEntryWithPosition(
	logEntry *filer_pb.LogEntry,
	filerId string,
	pos MessagePosition,
	filter PathFilter,
	cb EventCallback,
) error {
	event := &filer_pb.SubscribeMetadataResponse{}
	if err := proto.Unmarshal(logEntry.Data, event); err != nil {
		glog.Errorf("unmarshal log entry: %v", err)
		return nil
	}
	if !matchesFilter(event, filter) {
		return nil
	}
	return cb(event, filerId, pos)
}

type filerStream struct {
	id       string
	ch       <-chan *filer_pb.LogEntry
	errCh    <-chan error
	startPos MessagePosition
	paused   bool
}

type filerStreamHeapItem struct {
	stream *filerStream
	entry  *filer_pb.LogEntry
}

type filerStreamHeap []*filerStreamHeapItem

func (h filerStreamHeap) Len() int { return len(h) }
func (h filerStreamHeap) Less(i, j int) bool {
	a, b := h[i].entry, h[j].entry
	if a.TsNs != b.TsNs {
		return a.TsNs < b.TsNs
	}
	if h[i].stream.id != h[j].stream.id {
		return h[i].stream.id < h[j].stream.id
	}
	return a.Offset < b.Offset
}
func (h filerStreamHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *filerStreamHeap) Push(x any)   { *h = append(*h, x.(*filerStreamHeapItem)) }
func (h *filerStreamHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}
