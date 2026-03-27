package pb

import (
	"container/heap"
	"fmt"
	"io"
	"strings"

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
	// This mirrors the server's perFilerIteratorMap in OrderedLogVisitor.
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

	// Read all files per filer, collecting entries.
	// The server does the same in LogFileQueueIterator.readFileEntries().
	type filerEntries struct {
		filerId string
		entries []*filer_pb.LogEntry
		idx     int
	}
	filerData := make([]*filerEntries, 0, len(filerOrder))
	for _, filerId := range filerOrder {
		var allEntries []*filer_pb.LogEntry
		for _, ref := range perFiler[filerId] {
			entries, readErr := readLogFileEntries(newReader, ref.Chunks, startTsNs, stopTsNs)
			if readErr != nil {
				glog.V(0).Infof("read log file filer=%s ts=%d: %v", ref.FilerId, ref.FileTsNs, readErr)
				continue // skip unreadable files
			}
			allEntries = append(allEntries, entries...)
		}
		if len(allEntries) > 0 {
			filerData = append(filerData, &filerEntries{filerId: filerId, entries: allEntries})
		}
	}

	if len(filerData) == 0 {
		return
	}

	// If only one filer, no merge needed — process sequentially.
	if len(filerData) == 1 {
		return processEntriesForFiler(filerData[0].entries, pathPrefix, processEventFn)
	}

	// Merge entries from multiple filers in timestamp order using a min-heap.
	// This is the same algorithm as the server's OrderedLogVisitor + LogEntryItemPriorityQueue.
	pq := &logEntryHeap{}
	heap.Init(pq)
	for i, fd := range filerData {
		if len(fd.entries) > 0 {
			heap.Push(pq, &logEntryHeapItem{
				entry:    fd.entries[0],
				filerIdx: i,
			})
			filerData[i].idx = 1
		}
	}

	for pq.Len() > 0 {
		item := heap.Pop(pq).(*logEntryHeapItem)

		// Process this entry
		lastTsNs, err = processOneLogEntry(item.entry, pathPrefix, processEventFn)
		if err != nil {
			return
		}

		// Advance the filer iterator and push next entry
		fd := filerData[item.filerIdx]
		if fd.idx < len(fd.entries) {
			heap.Push(pq, &logEntryHeapItem{
				entry:    fd.entries[fd.idx],
				filerIdx: item.filerIdx,
			})
			fd.idx++
		}
	}
	return
}

func processEntriesForFiler(entries []*filer_pb.LogEntry, pathPrefix string, processEventFn ProcessMetadataFunc) (lastTsNs int64, err error) {
	for _, logEntry := range entries {
		lastTsNs, err = processOneLogEntry(logEntry, pathPrefix, processEventFn)
		if err != nil {
			return
		}
	}
	return
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
	filerIdx int // index into filerData slice
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

// readLogFileEntries reads a single log file and parses the
// [4-byte size | protobuf LogEntry] records within. Filters by timestamp range.
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

// matchesPathPrefix checks if a subscription event matches the given path prefix.
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
