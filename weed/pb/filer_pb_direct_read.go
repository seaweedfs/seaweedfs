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

// logEntryChannelSize bounds how many decoded entries are held in flight per
// filer stream. A whole log file is never buffered at once (see
// streamLogFileEntries), so peak memory is ~this many entries plus the chunk
// reader's buffer, not O(file size).
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
// Filers are read in parallel (one goroutine per filer). Within each filer,
// the next file is prefetched while the current file's entries are consumed.
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

	// Single filer fast path: no merge heap needed.
	if len(filerOrder) == 1 {
		return readFilerFilesWithPrefetch(perFiler[filerOrder[0]], newReader, startTsNs, stopTsNs, filter, processEventFn)
	}

	// Multiple filers: read each in parallel with prefetching, merge via min-heap.
	return readMultiFilersMerged(filerOrder, perFiler, newReader, startTsNs, stopTsNs, filter, processEventFn)
}

// readFilerFilesWithPrefetch reads files for a single filer, streaming entries
// through a bounded channel. A producer goroutine reads files in order and
// decodes one entry at a time (never a whole file), so the next file's network
// read overlaps processing while memory stays bounded by logEntryChannelSize.
func readFilerFilesWithPrefetch(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startTsNs, stopTsNs int64,
	filter PathFilter,
	processEventFn ProcessMetadataFunc,
) (lastTsNs int64, err error) {

	ch := make(chan *filer_pb.LogEntry, logEntryChannelSize)
	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() { stopOnce.Do(func() { close(stop) }) }
	defer closeStop()

	// readErr is written before the producer closes ch, so it is safe to read
	// after the range loop observes the close (happens-before via channel close).
	var readErr error
	go func() {
		defer close(ch)
		readFilerFilesToChannel(refs, newReader, startTsNs, stopTsNs, ch, stop, func(e error) { readErr = e })
	}()

	for entry := range ch {
		lastTsNs, err = processOneLogEntry(entry, filter, processEventFn)
		if err != nil {
			closeStop()
			for range ch { // unblock and drain the producer so it can exit
			}
			return
		}
	}
	if readErr != nil {
		return lastTsNs, readErr
	}
	return
}

// readMultiFilersMerged reads files from multiple filers in parallel (one goroutine
// per filer with prefetching), then merges entries in timestamp order via min-heap.
func readMultiFilersMerged(
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
	var wg sync.WaitGroup

	// A genuine (non chunk-not-found) read error must fail the whole replay: the
	// caller advances its cursor only on success, so a swallowed error leaves a
	// permanent gap. stop aborts the other readers; fatalErr is read on exit.
	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() { stopOnce.Do(func() { close(stop) }) }
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

	for i, filerId := range filerOrder {
		entryCh := make(chan *filer_pb.LogEntry, logEntryChannelSize)
		streams[i] = filerStream{filerId: filerId, entryCh: entryCh}

		wg.Add(1)
		go func(refs []*filer_pb.LogFileChunkRef, ch chan *filer_pb.LogEntry) {
			defer wg.Done()
			defer close(ch)
			readFilerFilesToChannel(refs, newReader, startTsNs, stopTsNs, ch, stop, setFatal)
		}(perFiler[filerId], entryCh)
	}

	// Stop readers, drain channels so none block on a send, then wait for exit.
	drainAndWait := func() {
		closeStop()
		for i := range streams {
			for range streams[i].entryCh {
			}
		}
		wg.Wait()
	}

	// Seed the min-heap with the first entry from each filer
	pq := &logEntryHeap{}
	heap.Init(pq)
	for i := range streams {
		if entry, ok := <-streams[i].entryCh; ok {
			heap.Push(pq, &logEntryHeapItem{entry: entry, filerIdx: i})
		}
	}

	// Merge loop
	for pq.Len() > 0 {
		// stop is closed only by setFatal here, so a closed stop means a reader
		// aborted; lock-free bail on the hot path.
		select {
		case <-stop:
			drainAndWait()
			fatalMu.Lock()
			fe := fatalErr
			fatalMu.Unlock()
			return lastTsNs, fe
		default:
		}

		item := heap.Pop(pq).(*logEntryHeapItem)

		lastTsNs, err = processOneLogEntry(item.entry, filter, processEventFn)
		if err != nil {
			drainAndWait()
			return
		}

		if entry, ok := <-streams[item.filerIdx].entryCh; ok {
			heap.Push(pq, &logEntryHeapItem{entry: entry, filerIdx: item.filerIdx})
		}
	}

	drainAndWait()
	fatalMu.Lock()
	fe := fatalErr
	fatalMu.Unlock()
	if fe != nil {
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
	sendEntry := func(entry *filer_pb.LogEntry) error {
		select {
		case ch <- entry:
			return nil
		case <-stop:
			return errReaderStopped
		}
	}

	for _, ref := range refs {
		streamErr := streamLogFileEntries(newReader, ref.Chunks, startTsNs, stopTsNs, sendEntry)
		if streamErr == errReaderStopped {
			return
		}
		if streamErr != nil {
			if isChunkNotFound(streamErr) {
				glog.V(0).Infof("skip log file filer=%s ts=%d: %v", ref.FilerId, ref.FileTsNs, streamErr)
				continue
			}
			glog.Errorf("read log file filer=%s ts=%d: %v", ref.FilerId, ref.FileTsNs, streamErr)
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
// entry as it is decoded, without buffering the whole file. A large (multi-GB)
// log file therefore costs one entry plus the chunk reader's buffer at a time,
// not O(file size). eachFn returns an error to stop early (e.g. the consumer
// went away); that error is propagated to the caller.
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
