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

// MessagePosition is the (ts_ns, offset) tuple the lifecycle reader uses as
// a per-shard cursor. LogEntry.Offset is per-buffer per-filer (not globally
// unique), so a single global cursor can't safely resume from a multi-filer
// merged stream — each filer's cursor must be tracked independently.
//
// Defined locally to avoid pulling weed/util/log_buffer as a dep of pb.
type MessagePosition struct {
	TsNs   int64
	Offset int64
}

// MaxMessagePosition is the sentinel "skip every event for this shard"
// position. Use it in startPositions to pause a shard whose blocker is
// active without removing it from the map.
var MaxMessagePosition = MessagePosition{TsNs: 1<<63 - 1, Offset: 1<<63 - 1}

// LessOrEqual reports whether p is at or before other in lex (ts, offset)
// order. The reader's resume predicate is `entry-pos <= cursor`, strict
// `<=` so the last resolved event isn't replayed.
func (p MessagePosition) LessOrEqual(other MessagePosition) bool {
	if p.TsNs != other.TsNs {
		return p.TsNs < other.TsNs
	}
	return p.Offset <= other.Offset
}

// EventCallback is the per-event callback for ReadLogFileRefsWithPosition.
// filerId is the shard the event came from; position is the entry's
// (ts, offset). The callback returning a non-nil error halts the read.
type EventCallback func(event *filer_pb.SubscribeMetadataResponse, filerId string, position MessagePosition) error

// ReadLogFileRefsWithPosition is the per-shard variant of ReadLogFileRefs.
// Each filer's events are skipped while (event.ts, event.offset) <=
// startPositions[filer_id]. stopTsNs caps the upper bound (0 = no cap).
//
// Cross-filer ordering is by event TsNs primarily, then by filerId (stable
// tiebreak). The reader's per-event resolution-or-stop logic depends on a
// deterministic order so retries from the same starting cursor see the
// same event sequence.
//
// Returns lastByFiler: the highest position observed per filer. Callers
// merge this into reader_state.last_processed_*[delay_seconds][filer_id]
// at checkpoint time.
//
// startPositions[filer_id] = MaxMessagePosition pauses that shard's
// emission entirely (used when the shard has an active blocker; the cursor
// stays at the position recorded in the BlockerRecord).
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
	sort.Strings(filerOrder) // deterministic tiebreak across runs

	if len(filerOrder) == 0 {
		return
	}

	// Per-filer reader: streams entries through a channel; the merge loop
	// pops the smallest (ts, filerId, offset) tuple and dispatches.
	streams := make([]*filerStream, 0, len(filerOrder))
	var wg sync.WaitGroup
	for _, fid := range filerOrder {
		startPos := startPositions[fid]
		paused := startPos == MaxMessagePosition
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

	// Merge streams via min-heap on (ts, filerId, offset).
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
	// Drain remaining channels so producer goroutines don't leak.
	for h.Len() > 0 {
		heap.Pop(h)
	}
	for _, s := range streams {
		if s.paused {
			continue
		}
		for range s.ch { // drain
		}
	}
	wg.Wait()

	// Surface the first producer error if no callback error already set.
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

// streamEntriesWithPosition reads chunk-file entries for one filer in
// sequential file order, applies the per-shard skip predicate, and
// emits entries on out. Returns once all chunks are read or an
// unrecoverable error occurs.
func streamEntriesWithPosition(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startPos MessagePosition,
	stopTsNs int64,
	out chan<- *filer_pb.LogEntry,
) error {
	for _, ref := range refs {
		// No file-level fast-skip: the chunk's FileTsNs is the start time
		// of the flush, but its last entry can have a TsNs later than any
		// other file in the sequence. Without a FileMaxTsNs on the ref we
		// can't safely prune whole files; the per-entry predicate below
		// handles every case correctly.
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

// dispatchOneEntryWithPosition unmarshals one log entry, applies the path
// filter, and invokes the per-event callback with shard context.
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
		return nil // skip corrupt entries
	}
	if !matchesFilter(event, filter) {
		return nil
	}
	return cb(event, filerId, pos)
}

// --- merge heap on (ts, filerId, offset) ---

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

