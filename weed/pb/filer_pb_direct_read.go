package pb

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// LogFileReaderFn creates an io.ReadCloser for a set of file chunks.
type LogFileReaderFn func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error)

// logEntryChannelSize bounds decoded entries in flight per filer stream.
const logEntryChannelSize = 512

// errReaderStopped signals that the entry consumer asked to stop (the merge
// loop aborted or the caller hit a processing error). It is not a read failure.
var errReaderStopped = errors.New("log entry reader stopped")

// PathFilter holds subscription path filtering parameters, matching the
// server-side eachEventNotificationFn filtering logic.
type PathFilter struct {
	PathPrefix             string
	AdditionalPathPrefixes []string
	DirectoriesToWatch     []string
}

// ReadLogFileRefs reads log file data directly from volume servers using the
// chunk references, merges entries from multiple filers in timestamp order
// (same algorithm as the server's OrderedLogVisitor), applies path filtering,
// and invokes processEventFn for each matching event.
//
// Filers are read in parallel (one goroutine per filer), each streaming
// entries through a bounded channel.
func ReadLogFileRefs(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startTsNs, stopTsNs int64,
	filter PathFilter,
	processEventFn ProcessMetadataFunc,
) (lastTsNs int64, err error) {

	if len(refs) == 0 {
		return
	}

	// Group refs by filer ID, preserving order within each filer.
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

	if len(filerOrder) == 0 {
		return
	}

	return readFilersMerged(filerOrder, perFiler, newReader, startTsNs, stopTsNs, filter, processEventFn)
}

// readFilersMerged reads files from each filer in parallel (one producer
// goroutine per filer streaming decoded entries through a bounded channel),
// then merges entries in timestamp order via min-heap. A single filer is the
// degenerate one-stream merge.
func readFilersMerged(
	filerOrder []string,
	perFiler map[string][]*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startTsNs, stopTsNs int64,
	filter PathFilter,
	processEventFn ProcessMetadataFunc,
) (lastTsNs int64, err error) {

	type filerStream struct {
		filerId string
		entryCh chan *filer_pb.LogEntry
	}

	streams := make([]filerStream, len(filerOrder))

	// A genuine (non chunk-not-found) read error must fail the whole replay: the
	// caller advances its cursor only on success, so a swallowed error leaves a
	// permanent gap. stop aborts the other readers; fatalErr is read on exit.
	//
	// Closing stop is the only cleanup: producers observe it at every send and
	// every file boundary and exit on their own. An abort must not wait for
	// them — a producer can be wedged in an uncancellable chunk read, and
	// joining it would stall the caller's retry loop behind a dead connection.
	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() { stopOnce.Do(func() { close(stop) }) }
	defer closeStop()
	var fatalMu sync.Mutex
	var fatalErr error
	setFatal := func(e error) {
		fatalMu.Lock()
		if fatalErr == nil {
			fatalErr = e
		}
		fatalMu.Unlock()
		closeStop()
	}
	fatal := func() error {
		fatalMu.Lock()
		defer fatalMu.Unlock()
		return fatalErr
	}

	for i, filerId := range filerOrder {
		entryCh := make(chan *filer_pb.LogEntry, logEntryChannelSize)
		streams[i] = filerStream{filerId: filerId, entryCh: entryCh}

		go func(refs []*filer_pb.LogFileChunkRef, ch chan *filer_pb.LogEntry) {
			defer close(ch)
			readFilerFilesToChannel(refs, newReader, startTsNs, stopTsNs, ch, stop, setFatal)
		}(perFiler[filerId], entryCh)
	}

	// Seed the min-heap with the first entry from each filer
	pq := &logEntryHeap{}
	heap.Init(pq)
	for i := range streams {
		select {
		case entry, ok := <-streams[i].entryCh:
			if ok {
				heap.Push(pq, &logEntryHeapItem{entry: entry, filerIdx: i})
			}
		case <-stop:
			return lastTsNs, fatal()
		}
	}

	// Merge loop
	for pq.Len() > 0 {
		// stop is closed only by setFatal here, so a closed stop means a reader
		// aborted; lock-free bail on the hot path.
		select {
		case <-stop:
			return lastTsNs, fatal()
		default:
		}

		item := heap.Pop(pq).(*logEntryHeapItem)

		lastTsNs, err = processOneLogEntry(item.entry, filter, processEventFn)
		if err != nil {
			return
		}

		select {
		case entry, ok := <-streams[item.filerIdx].entryCh:
			if ok {
				heap.Push(pq, &logEntryHeapItem{entry: entry, filerIdx: item.filerIdx})
			}
		case <-stop:
			return lastTsNs, fatal()
		}
	}

	// All channels closed: every producer has finished (setFatal, if any,
	// happened before its close).
	if fe := fatal(); fe != nil {
		return lastTsNs, fe
	}
	return
}

func readFilerFilesToChannel(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startTsNs, stopTsNs int64,
	ch chan *filer_pb.LogEntry,
	stop <-chan struct{},
	setFatal func(error),
) {
	var sent int
	sendEntry := func(entry *filer_pb.LogEntry) error {
		// Prefer stop over a send the buffer could still absorb, so an aborted
		// producer quits at the next entry instead of filling the channel.
		select {
		case <-stop:
			return errReaderStopped
		default:
		}
		select {
		case ch <- entry:
			sent++
			return nil
		case <-stop:
			return errReaderStopped
		}
	}

	for _, ref := range refs {
		select {
		case <-stop:
			return
		default:
		}

		sentBefore := sent
		streamErr := streamLogFileEntries(newReader, ref.Chunks, startTsNs, stopTsNs, sendEntry)
		if streamErr == errReaderStopped {
			return
		}
		if streamErr != nil {
			if isChunkNotFound(streamErr) {
				// A mid-file not-found still delivered the readable prefix; say so
				// rather than pretending the whole file was skipped.
				glog.V(0).Infof("skip log file filer=%s ts=%d after %d entries: %v", ref.FilerId, ref.FileTsNs, sent-sentBefore, streamErr)
				continue
			}
			setFatal(fmt.Errorf("read log file filer=%s ts=%d: %w", ref.FilerId, ref.FileTsNs, streamErr))
			return
		}
	}
}

func processOneLogEntry(logEntry *filer_pb.LogEntry, filter PathFilter, processEventFn ProcessMetadataFunc) (int64, error) {
	event := &filer_pb.SubscribeMetadataResponse{}
	if err := proto.Unmarshal(logEntry.Data, event); err != nil {
		glog.Errorf("unmarshal log entry: %v", err)
		return 0, nil // skip corrupt entries
	}
	if !matchesFilter(event, filter) {
		return event.TsNs, nil
	}
	if err := processEventFn(event); err != nil {
		return event.TsNs, fmt.Errorf("process event: %w", err)
	}
	return event.TsNs, nil
}

// --- path filtering (mirrors server-side eachEventNotificationFn logic) ---

const systemLogDir = "/topics/.system/log"

func matchesFilter(resp *filer_pb.SubscribeMetadataResponse, filter PathFilter) bool {
	fullpath := filer_pb.MetadataEventSourceFullPath(resp)

	// Skip internal meta log entries
	if strings.HasPrefix(fullpath, systemLogDir) {
		return false
	}

	return filer_pb.MetadataEventMatchesSubscription(resp, filter.PathPrefix, filter.AdditionalPathPrefixes, filter.DirectoriesToWatch)
}

// isChunkNotFound checks if an error indicates a missing volume chunk.
// Matches the server-side isChunkNotFoundError logic.
func isChunkNotFound(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "not found") || strings.Contains(s, "status 404")
}

// --- min-heap for merging entries across filers ---

type logEntryHeapItem struct {
	entry    *filer_pb.LogEntry
	filerIdx int
}

type logEntryHeap []*logEntryHeapItem

func (h logEntryHeap) Len() int           { return len(h) }
func (h logEntryHeap) Less(i, j int) bool { return h[i].entry.TsNs < h[j].entry.TsNs }
func (h logEntryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *logEntryHeap) Push(x any)        { *h = append(*h, x.(*logEntryHeapItem)) }
func (h *logEntryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// --- log file parsing (uses io.ReadFull for correct partial-read handling) ---

// streamLogFileEntries reads a log file's chunks and invokes eachFn for every
// entry as it is decoded, without buffering the whole file: a multi-GB log
// file costs one entry plus the chunk reader's buffer at a time, not O(file
// size). An eachFn error stops the read and is propagated verbatim.
func streamLogFileEntries(newReader LogFileReaderFn, chunks []*filer_pb.FileChunk, startTsNs, stopTsNs int64, eachFn func(*filer_pb.LogEntry) error) error {
	reader, err := newReader(chunks)
	if err != nil {
		return fmt.Errorf("create reader: %w", err)
	}
	defer reader.Close()

	sizeBuf := make([]byte, 4)

	for {
		if _, err := io.ReadFull(reader, sizeBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		size := util.BytesToUint32(sizeBuf)
		entryData := make([]byte, size)
		if _, err := io.ReadFull(reader, entryData); err != nil {
			return err
		}

		logEntry := &filer_pb.LogEntry{}
		if err := proto.Unmarshal(entryData, logEntry); err != nil {
			return err
		}

		if logEntry.TsNs <= startTsNs {
			continue
		}
		if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
			return nil
		}

		if err := eachFn(logEntry); err != nil {
			return err
		}
	}
}
