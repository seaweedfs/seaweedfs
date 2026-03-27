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
type LogFileReaderFn func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error)

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

// readFilerFilesWithPrefetch reads files for a single filer, prefetching the
// next file while processing entries from the current one.
func readFilerFilesWithPrefetch(
	refs []*filer_pb.LogFileChunkRef,
	newReader LogFileReaderFn,
	startTsNs, stopTsNs int64,
	filter PathFilter,
	processEventFn ProcessMetadataFunc,
) (lastTsNs int64, err error) {

	type prefetchResult struct {
		entries []*filer_pb.LogEntry
		err     error
	}

	startPrefetch := func(ref *filer_pb.LogFileChunkRef) chan prefetchResult {
		ch := make(chan prefetchResult, 1)
		go func() {
			entries, readErr := readLogFileEntries(newReader, ref.Chunks, startTsNs, stopTsNs)
			ch <- prefetchResult{entries, readErr}
		}()
		return ch
	}

	var pendingCh chan prefetchResult
	if len(refs) > 0 {
		pendingCh = startPrefetch(refs[0])
	}

	for i, ref := range refs {
		result := <-pendingCh

		// Start prefetching next file while we process current
		if i+1 < len(refs) {
			pendingCh = startPrefetch(refs[i+1])
		}

		if result.err != nil {
			if isChunkNotFound(result.err) {
				glog.V(0).Infof("skip log file filer=%s ts=%d: %v", ref.FilerId, ref.FileTsNs, result.err)
				continue
			}
			return lastTsNs, fmt.Errorf("read log file filer=%s ts=%d: %w", ref.FilerId, ref.FileTsNs, result.err)
		}

		for _, logEntry := range result.entries {
			lastTsNs, err = processOneLogEntry(logEntry, filter, processEventFn)
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
	filter PathFilter,
	processEventFn ProcessMetadataFunc,
) (lastTsNs int64, err error) {

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

	// Merge loop
	for pq.Len() > 0 {
		item := heap.Pop(pq).(*logEntryHeapItem)

		lastTsNs, err = processOneLogEntry(item.entry, filter, processEventFn)
		if err != nil {
			for i := range streams {
				for range streams[i].entryCh {
				}
			}
			wg.Wait()
			return
		}

		if entry, ok := <-streams[item.filerIdx].entryCh; ok {
			heap.Push(pq, &logEntryHeapItem{entry: entry, filerIdx: item.filerIdx})
		}
	}

	wg.Wait()
	return
}

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

		if i+1 < len(refs) {
			pendingCh = startPrefetch(refs[i+1])
		}

		if result.err != nil {
			if isChunkNotFound(result.err) {
				glog.V(0).Infof("skip log file filer=%s ts=%d: %v", ref.FilerId, ref.FileTsNs, result.err)
			} else {
				glog.Errorf("read log file filer=%s ts=%d: %v", ref.FilerId, ref.FileTsNs, result.err)
			}
			continue
		}

		for _, entry := range result.entries {
			ch <- entry
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
	var entryName string
	if resp.EventNotification != nil {
		if resp.EventNotification.OldEntry != nil {
			entryName = resp.EventNotification.OldEntry.Name
		} else if resp.EventNotification.NewEntry != nil {
			entryName = resp.EventNotification.NewEntry.Name
		}
	}

	fullpath := util.Join(resp.Directory, entryName)

	// Skip internal meta log entries
	if strings.HasPrefix(fullpath, systemLogDir) {
		return false
	}

	// Check AdditionalPathPrefixes
	for _, p := range filter.AdditionalPathPrefixes {
		if strings.HasPrefix(fullpath, p) {
			return true
		}
	}

	// Check DirectoriesToWatch (exact directory match)
	for _, dir := range filter.DirectoriesToWatch {
		if resp.Directory == dir {
			return true
		}
	}

	// Check primary PathPrefix
	if filter.PathPrefix == "" || filter.PathPrefix == "/" {
		return true
	}
	if strings.HasPrefix(fullpath, filter.PathPrefix) {
		return true
	}

	// Check rename target
	if resp.EventNotification != nil && resp.EventNotification.NewParentPath != "" {
		newFullPath := util.Join(resp.EventNotification.NewParentPath, entryName)
		if strings.HasPrefix(newFullPath, filter.PathPrefix) {
			return true
		}
	}

	return false
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

// --- log file parsing (uses io.ReadFull for correct partial-read handling) ---

func readLogFileEntries(newReader LogFileReaderFn, chunks []*filer_pb.FileChunk, startTsNs, stopTsNs int64) ([]*filer_pb.LogEntry, error) {
	reader, err := newReader(chunks)
	if err != nil {
		return nil, fmt.Errorf("create reader: %w", err)
	}
	defer reader.Close()

	sizeBuf := make([]byte, 4)
	var entries []*filer_pb.LogEntry

	for {
		_, err := io.ReadFull(reader, sizeBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return entries, err
		}

		size := util.BytesToUint32(sizeBuf)
		entryData := make([]byte, size)
		_, err = io.ReadFull(reader, entryData)
		if err != nil {
			return entries, err
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
