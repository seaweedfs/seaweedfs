package pb

import (
	"container/heap"
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
// This is typically filer.NewChunkStreamReaderFromLookup or similar, passed in
// by the caller to avoid a circular dependency on the filer package.
type LogFileReaderFn func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error)

// ReadLogFileRefs reads log file data directly from volume servers using the
// chunk references, merges entries from multiple filers in timestamp order
// (same algorithm as the server's OrderedLogVisitor), applies path filtering,
// and invokes processEventFn for each matching event.
//
// Optimizations over the server-side path:
//   - Filers are read in parallel (one goroutine per filer)
//   - Within each filer, the next file is prefetched while the current file's
//     entries are being consumed by the merge heap
//   - No gRPC send/recv overhead per event
func ReadLogFileRefs(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startTsNs, stopTsNs int64,
	pathPrefix string,
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
		return readFilerFilesWithPrefetch(perFiler[filerOrder[0]], newReader, startTsNs, stopTsNs, pathPrefix, processEventFn)
	}

	// Multiple filers: read each in parallel with prefetching, merge via min-heap.
	return readMultiFilersMerged(filerOrder, perFiler, newReader, startTsNs, stopTsNs, pathPrefix, processEventFn)
}

// readFilerFilesWithPrefetch reads files for a single filer, prefetching the
// next file while processing entries from the current one.
func readFilerFilesWithPrefetch(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startTsNs, stopTsNs int64,
	pathPrefix string,
	processEventFn ProcessMetadataFunc,
) (lastTsNs int64, err error) {

	type prefetchResult struct {
		entries []*filer_pb.LogEntry
		err     error
	}

	var pending *prefetchResult
	var pendingCh chan prefetchResult

	// Start prefetch for the first file
	startPrefetch := func(ref *filer_pb.LogFileChunkRef) chan prefetchResult {
		ch := make(chan prefetchResult, 1)
		go func() {
			entries, readErr := readLogFileEntries(newReader, ref.Chunks, startTsNs, stopTsNs)
			ch <- prefetchResult{entries, readErr}
		}()
		return ch
	}

	if len(refs) > 0 {
		pendingCh = startPrefetch(refs[0])
	}

	for i, ref := range refs {
		// Wait for current file's data
		var result prefetchResult
		if pending != nil {
			result = *pending
			pending = nil
		} else if pendingCh != nil {
			result = <-pendingCh
			pendingCh = nil
		}

		// Start prefetching next file while we process current
		if i+1 < len(refs) {
			pendingCh = startPrefetch(refs[i+1])
		}

		if result.err != nil {
			glog.V(0).Infof("read log file filer=%s ts=%d: %v", ref.FilerId, ref.FileTsNs, result.err)
			continue
		}

		for _, logEntry := range result.entries {
			lastTsNs, err = processOneLogEntry(logEntry, pathPrefix, processEventFn)
			if err != nil {
				return
			}
		}
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
	pathPrefix string,
	processEventFn ProcessMetadataFunc,
) (lastTsNs int64, err error) {

	// Each filer gets a goroutine that reads its files (with prefetching)
	// and sends entries to a channel. The main goroutine merges via min-heap.
	type filerStream struct {
		filerId string
		entryCh chan *filer_pb.LogEntry
	}

	streams := make([]filerStream, len(filerOrder))
	var wg sync.WaitGroup

	for i, filerId := range filerOrder {
		entryCh := make(chan *filer_pb.LogEntry, 512)
		streams[i] = filerStream{filerId: filerId, entryCh: entryCh}

		wg.Add(1)
		go func(refs []*filer_pb.LogFileChunkRef, ch chan *filer_pb.LogEntry) {
			defer wg.Done()
			defer close(ch)
			readFilerFilesToChannel(refs, newReader, startTsNs, stopTsNs, ch)
		}(perFiler[filerId], entryCh)
	}

	// Seed the min-heap with the first entry from each filer
	pq := &logEntryHeap{}
	heap.Init(pq)
	for i := range streams {
		if entry, ok := <-streams[i].entryCh; ok {
			heap.Push(pq, &logEntryHeapItem{entry: entry, filerIdx: i})
		}
	}

	// Merge loop: pop minimum, process, advance that filer
	for pq.Len() > 0 {
		item := heap.Pop(pq).(*logEntryHeapItem)

		lastTsNs, err = processOneLogEntry(item.entry, pathPrefix, processEventFn)
		if err != nil {
			// Drain remaining channels so goroutines exit
			for i := range streams {
				for range streams[i].entryCh {
				}
			}
			return
		}

		// Read next entry from the same filer
		if entry, ok := <-streams[item.filerIdx].entryCh; ok {
			heap.Push(pq, &logEntryHeapItem{entry: entry, filerIdx: item.filerIdx})
		}
	}

	wg.Wait()
	return
}

// readFilerFilesToChannel reads files for one filer with prefetching and sends
// entries to a channel. Used by the parallel multi-filer merge.
func readFilerFilesToChannel(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startTsNs, stopTsNs int64,
	ch chan *filer_pb.LogEntry,
) {
	type prefetchResult struct {
		entries []*filer_pb.LogEntry
		err     error
	}

	startPrefetch := func(ref *filer_pb.LogFileChunkRef) chan prefetchResult {
		resultCh := make(chan prefetchResult, 1)
		go func() {
			entries, err := readLogFileEntries(newReader, ref.Chunks, startTsNs, stopTsNs)
			resultCh <- prefetchResult{entries, err}
		}()
		return resultCh
	}

	var pendingCh chan prefetchResult
	if len(refs) > 0 {
		pendingCh = startPrefetch(refs[0])
	}

	for i, ref := range refs {
		result := <-pendingCh

		// Prefetch next file while we send current entries
		if i+1 < len(refs) {
			pendingCh = startPrefetch(refs[i+1])
		}

		if result.err != nil {
			glog.V(0).Infof("read log file filer=%s ts=%d: %v", ref.FilerId, ref.FileTsNs, result.err)
			continue
		}

		for _, entry := range result.entries {
			ch <- entry
		}
	}
}

func processOneLogEntry(logEntry *filer_pb.LogEntry, pathPrefix string, processEventFn ProcessMetadataFunc) (int64, error) {
	event := &filer_pb.SubscribeMetadataResponse{}
	if err := proto.Unmarshal(logEntry.Data, event); err != nil {
		glog.Errorf("unmarshal log entry: %v", err)
		return 0, nil // skip corrupt entries
	}
	if !matchesPathPrefix(event, pathPrefix) {
		return event.TsNs, nil
	}
	if err := processEventFn(event); err != nil {
		return event.TsNs, fmt.Errorf("process event: %w", err)
	}
	return event.TsNs, nil
}

// --- min-heap for merging entries across filers (same as server's LogEntryItemPriorityQueue) ---

type logEntryHeapItem struct {
	entry    *filer_pb.LogEntry
	filerIdx int
}

type logEntryHeap []*logEntryHeapItem

func (h logEntryHeap) Len() int            { return len(h) }
func (h logEntryHeap) Less(i, j int) bool  { return h[i].entry.TsNs < h[j].entry.TsNs }
func (h logEntryHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *logEntryHeap) Push(x any)         { *h = append(*h, x.(*logEntryHeapItem)) }
func (h *logEntryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// --- log file parsing (shared format with server's LogFileIterator.getNext) ---

func readLogFileEntries(newReader LogFileReaderFn, chunks []*filer_pb.FileChunk, startTsNs, stopTsNs int64) ([]*filer_pb.LogEntry, error) {
	reader, err := newReader(chunks)
	if err != nil {
		return nil, fmt.Errorf("create reader: %w", err)
	}
	defer reader.Close()

	sizeBuf := make([]byte, 4)
	var entries []*filer_pb.LogEntry

	for {
		n, err := reader.Read(sizeBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return entries, err
		}
		if n != 4 {
			return entries, fmt.Errorf("size %d bytes, expected 4", n)
		}

		size := util.BytesToUint32(sizeBuf)
		entryData := make([]byte, size)
		n, err = reader.Read(entryData)
		if err != nil {
			return entries, err
		}
		if n != int(size) {
			return entries, fmt.Errorf("entry data %d bytes, expected %d", n, size)
		}

		logEntry := &filer_pb.LogEntry{}
		if err = proto.Unmarshal(entryData, logEntry); err != nil {
			return entries, err
		}

		if logEntry.TsNs <= startTsNs {
			continue
		}
		if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
			break
		}

		entries = append(entries, logEntry)
	}
	return entries, nil
}

func matchesPathPrefix(resp *filer_pb.SubscribeMetadataResponse, pathPrefix string) bool {
	if pathPrefix == "" || pathPrefix == "/" {
		return true
	}

	var entryName string
	if resp.EventNotification != nil {
		if resp.EventNotification.OldEntry != nil {
			entryName = resp.EventNotification.OldEntry.Name
		} else if resp.EventNotification.NewEntry != nil {
			entryName = resp.EventNotification.NewEntry.Name
		}
	}

	fullpath := resp.Directory + "/" + entryName
	if strings.HasPrefix(fullpath, pathPrefix) {
		return true
	}

	if resp.EventNotification != nil && resp.EventNotification.NewParentPath != "" {
		newFullPath := resp.EventNotification.NewParentPath + "/" + entryName
		if strings.HasPrefix(newFullPath, pathPrefix) {
			return true
		}
	}

	return false
}
