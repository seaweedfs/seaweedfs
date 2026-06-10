package filer

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type LogFileEntry struct {
	TsNs      int64
	FileEntry *Entry
}

func (f *Filer) collectPersistedLogBuffer(startPosition log_buffer.MessagePosition, stopTsNs int64) (v *OrderedLogVisitor, err error) {

	if stopTsNs != 0 && startPosition.Time.UnixNano() > stopTsNs {
		return nil, io.EOF
	}

	startDate := fmt.Sprintf("%04d-%02d-%02d", startPosition.Time.Year(), startPosition.Time.Month(), startPosition.Time.Day())

	dayEntries, _, listDayErr := f.ListDirectoryEntries(context.Background(), SystemLogDir, startDate, true, math.MaxInt32, "", "", "")
	if listDayErr != nil {
		return nil, fmt.Errorf("fail to list log by day: %w", listDayErr)
	}

	return NewOrderedLogVisitor(f, startPosition, stopTsNs, dayEntries)

}

// CollectLogFileRefs lists persisted log files and returns their chunk references
// without reading any data from volume servers. The client can use the returned
// fids to read log file data directly from volume servers in parallel.
func (f *Filer) CollectLogFileRefs(ctx context.Context, startPosition log_buffer.MessagePosition, stopTsNs int64) (refs []*filer_pb.LogFileChunkRef, lastTsNs int64, err error) {
	if stopTsNs != 0 && startPosition.Time.UnixNano() > stopTsNs {
		return nil, 0, nil
	}

	startDate := fmt.Sprintf("%04d-%02d-%02d", startPosition.Time.Year(), startPosition.Time.Month(), startPosition.Time.Day())
	startHourMinute := fmt.Sprintf("%02d-%02d", startPosition.Time.Hour(), startPosition.Time.Minute())
	var stopDate, stopHourMinute string
	if stopTsNs != 0 {
		stopTime := time.Unix(0, stopTsNs).UTC()
		stopDate = fmt.Sprintf("%04d-%02d-%02d", stopTime.Year(), stopTime.Month(), stopTime.Day())
		stopHourMinute = fmt.Sprintf("%02d-%02d", stopTime.Hour(), stopTime.Minute())
	}

	dayEntries, _, listDayErr := f.ListDirectoryEntries(ctx, SystemLogDir, startDate, true, math.MaxInt32, "", "", "")
	if listDayErr != nil {
		return nil, 0, fmt.Errorf("fail to list log by day: %w", listDayErr)
	}

	for _, dayEntry := range dayEntries {
		if stopDate != "" && strings.Compare(dayEntry.Name(), stopDate) > 0 {
			break
		}

		hourMinuteEntries, _, listErr := f.ListDirectoryEntries(ctx, util.NewFullPath(SystemLogDir, dayEntry.Name()), "", false, math.MaxInt32, "", "", "")
		if listErr != nil {
			return nil, 0, fmt.Errorf("fail to list log %s: %w", dayEntry.Name(), listErr)
		}

		for _, hmEntry := range hourMinuteEntries {
			hourMinute := util.FileNameBase(hmEntry.Name())
			if dayEntry.Name() == startDate && strings.Compare(hourMinute, startHourMinute) < 0 {
				continue
			}
			if dayEntry.Name() == stopDate && stopHourMinute != "" && strings.Compare(hourMinute, stopHourMinute) > 0 {
				break
			}

			tsMinute := fmt.Sprintf("%s-%s", dayEntry.Name(), hourMinute)
			t, parseErr := time.Parse("2006-01-02-15-04", tsMinute)
			if parseErr != nil {
				glog.Errorf("failed to parse %s: %v", tsMinute, parseErr)
				continue
			}
			filerId := getFilerId(hmEntry.Name())
			if filerId == "" {
				continue
			}

			chunks := hmEntry.GetChunks()
			if len(chunks) == 0 {
				continue
			}

			refs = append(refs, &filer_pb.LogFileChunkRef{
				Chunks:   chunks,
				FileTsNs: t.UnixNano(),
				FilerId:  filerId,
			})
			lastTsNs = t.UnixNano()
		}
	}
	return
}

func (f *Filer) HasPersistedLogFiles(startPosition log_buffer.MessagePosition) (bool, error) {
	startDate := fmt.Sprintf("%04d-%02d-%02d", startPosition.Time.Year(), startPosition.Time.Month(), startPosition.Time.Day())
	dayEntries, _, listDayErr := f.ListDirectoryEntries(context.Background(), SystemLogDir, startDate, true, 1, "", "", "")

	if listDayErr != nil {
		return false, fmt.Errorf("fail to list log by day: %w", listDayErr)
	}
	if len(dayEntries) == 0 {
		return false, nil
	}
	return true, nil
}

// ----------
type LogEntryItem struct {
	Entry *filer_pb.LogEntry
	filer string
}

// LogEntryItemPriorityQueue a priority queue for LogEntry
type LogEntryItemPriorityQueue []*LogEntryItem

func (pq LogEntryItemPriorityQueue) Len() int { return len(pq) }
func (pq LogEntryItemPriorityQueue) Less(i, j int) bool {
	return pq[i].Entry.TsNs < pq[j].Entry.TsNs
}
func (pq LogEntryItemPriorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }
func (pq *LogEntryItemPriorityQueue) Push(x any) {
	item := x.(*LogEntryItem)
	*pq = append(*pq, item)
}
func (pq *LogEntryItemPriorityQueue) Pop() any {
	n := len(*pq)
	item := (*pq)[n-1]
	*pq = (*pq)[:n-1]
	return item
}

// ----------

type OrderedLogVisitor struct {
	perFilerIteratorMap   map[string]*LogFileQueueIterator
	pq                    *LogEntryItemPriorityQueue
	logFileEntryCollector *LogFileEntryCollector
}

func NewOrderedLogVisitor(f *Filer, startPosition log_buffer.MessagePosition, stopTsNs int64, dayEntries []*Entry) (*OrderedLogVisitor, error) {

	perFilerQueueMap := make(map[string]*LogFileQueueIterator)
	// initialize the priority queue
	pq := &LogEntryItemPriorityQueue{}
	heap.Init(pq)

	t := &OrderedLogVisitor{
		perFilerIteratorMap:   perFilerQueueMap,
		pq:                    pq,
		logFileEntryCollector: NewLogFileEntryCollector(f, startPosition, stopTsNs, dayEntries),
	}
	if err := t.logFileEntryCollector.collectMore(t); err != nil && err != io.EOF {
		return nil, err
	}
	return t, nil
}

func (o *OrderedLogVisitor) GetNext() (logEntry *filer_pb.LogEntry, err error) {
	if o.pq.Len() == 0 {
		return nil, io.EOF
	}
	item := heap.Pop(o.pq).(*LogEntryItem)
	filerId := item.filer

	// fill the pq with the next log entry from the same filer
	it := o.perFilerIteratorMap[filerId]
	next, nextErr := it.getNext(o)
	if nextErr != nil {
		if nextErr == io.EOF {
			// do nothing since the filer has no more log entries
		} else {
			return nil, fmt.Errorf("failed to get next log entry: %w", nextErr)
		}
	} else {
		heap.Push(o.pq, &LogEntryItem{
			Entry: next,
			filer: filerId,
		})
	}
	return item.Entry, nil
}

// Close releases any log file readers still open across the per-filer
// iterators, e.g. when a subscription stops before reaching the end. Safe to
// call more than once.
func (o *OrderedLogVisitor) Close() {
	for _, it := range o.perFilerIteratorMap {
		it.Close()
	}
}

func getFilerId(name string) string {
	idx := strings.LastIndex(name, ".")
	if idx < 0 {
		return ""
	}
	return name[idx+1:]
}

// ----------

type LogFileEntryCollector struct {
	f               *Filer
	startTsNs       int64
	stopTsNs        int64
	dayEntryQueue   *util.Queue[*Entry]
	startDate       string
	startHourMinute string
	stopDate        string
	stopHourMinute  string
}

func NewLogFileEntryCollector(f *Filer, startPosition log_buffer.MessagePosition, stopTsNs int64, dayEntries []*Entry) *LogFileEntryCollector {
	dayEntryQueue := util.NewQueue[*Entry]()
	for _, dayEntry := range dayEntries {
		dayEntryQueue.Enqueue(dayEntry)
		// println("enqueue day entry", dayEntry.Name())
	}

	startDate := fmt.Sprintf("%04d-%02d-%02d", startPosition.Time.Year(), startPosition.Time.Month(), startPosition.Time.Day())
	startHourMinute := fmt.Sprintf("%02d-%02d", startPosition.Time.Hour(), startPosition.Time.Minute())
	var stopDate, stopHourMinute string
	if stopTsNs != 0 {
		stopTime := time.Unix(0, stopTsNs+24*60*60*int64(time.Second)).UTC()
		stopDate = fmt.Sprintf("%04d-%02d-%02d", stopTime.Year(), stopTime.Month(), stopTime.Day())
		stopHourMinute = fmt.Sprintf("%02d-%02d", stopTime.Hour(), stopTime.Minute())
	}

	return &LogFileEntryCollector{
		f:               f,
		startTsNs:       startPosition.Time.UnixNano(),
		stopTsNs:        stopTsNs,
		dayEntryQueue:   dayEntryQueue,
		startDate:       startDate,
		startHourMinute: startHourMinute,
		stopDate:        stopDate,
		stopHourMinute:  stopHourMinute,
	}
}

func (c *LogFileEntryCollector) collectMore(v *OrderedLogVisitor) (err error) {
	dayEntry := c.dayEntryQueue.Dequeue()
	if dayEntry == nil {
		return io.EOF
	}
	// println("dequeue day entry", dayEntry.Name())
	if c.stopDate != "" {
		if strings.Compare(dayEntry.Name(), c.stopDate) > 0 {
			return io.EOF
		}
	}

	hourMinuteEntries, _, listHourMinuteErr := c.f.ListDirectoryEntries(context.Background(), util.NewFullPath(SystemLogDir, dayEntry.Name()), "", false, math.MaxInt32, "", "", "")
	if listHourMinuteErr != nil {
		return fmt.Errorf("fail to list log %s by day: %v", dayEntry.Name(), listHourMinuteErr)
	}
	freshFilerIds := make(map[string]string)
	for _, hourMinuteEntry := range hourMinuteEntries {
		// println("checking hh-mm", hourMinuteEntry.FullPath)
		hourMinute := util.FileNameBase(hourMinuteEntry.Name())
		if dayEntry.Name() == c.startDate {
			if strings.Compare(hourMinute, c.startHourMinute) < 0 {
				continue
			}
		}
		if dayEntry.Name() == c.stopDate {
			if strings.Compare(hourMinute, c.stopHourMinute) > 0 {
				break
			}
		}

		tsMinute := fmt.Sprintf("%s-%s", dayEntry.Name(), hourMinute)
		// println("  enqueue", tsMinute)
		t, parseErr := time.Parse("2006-01-02-15-04", tsMinute)
		if parseErr != nil {
			glog.Errorf("failed to parse %s: %v", tsMinute, parseErr)
			continue
		}
		filerId := getFilerId(hourMinuteEntry.Name())
		if filerId == "" {
			glog.Warningf("Invalid log file name format: %s", hourMinuteEntry.Name())
			continue // Skip files with invalid format
		}
		iter, found := v.perFilerIteratorMap[filerId]
		if !found {
			iter = newLogFileQueueIterator(c.f.MasterClient, c.f.persistedLogCache, util.NewQueue[*LogFileEntry](), c.startTsNs, c.stopTsNs)
			v.perFilerIteratorMap[filerId] = iter
			freshFilerIds[filerId] = hourMinuteEntry.Name()
		}
		iter.q.Enqueue(&LogFileEntry{
			TsNs:      t.UnixNano(),
			FileEntry: hourMinuteEntry,
		})
	}

	// fill the pq with the next log entry if it is a new filer
	for filerId, entryName := range freshFilerIds {
		iter, found := v.perFilerIteratorMap[filerId]
		if !found {
			glog.Errorf("Unexpected! failed to find iterator for filer %s", filerId)
			continue
		}
		next, nextErr := iter.getNext(v)
		if nextErr != nil {
			if nextErr == io.EOF {
				// do nothing since the filer has no more log entries
			} else {
				return fmt.Errorf("failed to get next log entry for %v: %w", entryName, nextErr)
			}
		} else {
			heap.Push(v.pq, &LogEntryItem{
				Entry: next,
				filer: filerId,
			})
		}
	}

	return nil
}

// ----------

type LogFileQueueIterator struct {
	q                   *util.Queue[*LogFileEntry]
	masterClient        *wdclient.MasterClient
	cache               *persistedLogCache
	startTsNs           int64
	stopTsNs            int64
	currentFileIterator *LogFileIterator
}

func newLogFileQueueIterator(masterClient *wdclient.MasterClient, cache *persistedLogCache, q *util.Queue[*LogFileEntry], startTsNs, stopTsNs int64) *LogFileQueueIterator {
	return &LogFileQueueIterator{
		q:            q,
		masterClient: masterClient,
		cache:        cache,
		startTsNs:    startTsNs,
		stopTsNs:     stopTsNs,
	}
}

// Close releases the current log file reader, if any. Safe to call more than once.
func (iter *LogFileQueueIterator) Close() {
	if iter.currentFileIterator != nil {
		if err := iter.currentFileIterator.Close(); err != nil {
			glog.Warningf("close log file %s: %v", iter.currentFileIterator.filePath, err)
		}
		iter.currentFileIterator = nil
	}
}

// getNext streams one log entry at a time from the current file, advancing to
// the next file as each is exhausted. It returns io.EOF when done. Entries are
// not buffered per file, so memory stays O(1) regardless of log file size.
func (iter *LogFileQueueIterator) getNext(v *OrderedLogVisitor) (logEntry *filer_pb.LogEntry, err error) {
	for {
		if iter.currentFileIterator != nil {
			logEntry, err = iter.currentFileIterator.getNext()
			if err == nil {
				return logEntry, nil
			}
			// The current file is done (io.EOF), its volume was deleted, or it is
			// unreadable. Close it on every path so the reader is not left alive
			// until GC; only a genuine read error is propagated.
			readErr := err
			switch {
			case readErr == io.EOF:
				readErr = nil
			case isChunkNotFoundError(readErr):
				// Volume or chunk was deleted, skip the rest of this log file
				glog.Warningf("skipping rest of %s: %v", iter.currentFileIterator.filePath, readErr)
				readErr = nil
			}
			if closeErr := iter.currentFileIterator.Close(); closeErr != nil {
				glog.Warningf("close log file %s: %v", iter.currentFileIterator.filePath, closeErr)
			}
			iter.currentFileIterator = nil
			if readErr != nil {
				return nil, readErr
			}
		}

		// advance to the next file
		if iter.q.Len() == 0 {
			return nil, io.EOF
		}
		t := iter.q.Dequeue()
		if t == nil {
			continue
		}
		// skip the file if it is after the stopTsNs
		if iter.stopTsNs != 0 && t.TsNs > iter.stopTsNs {
			return nil, io.EOF
		}
		next := iter.q.Peek()
		if next == nil {
			if collectErr := v.logFileEntryCollector.collectMore(v); collectErr != nil && collectErr != io.EOF {
				return nil, collectErr
			}
			next = iter.q.Peek() // Re-peek after collectMore
		}
		// skip the file if the next entry is before the startTsNs
		if next != nil && next.TsNs <= iter.startTsNs {
			continue
		}
		iter.currentFileIterator = newLogFileIterator(iter.masterClient, iter.cache, t.FileEntry, t.TsNs, iter.startTsNs, iter.stopTsNs)
	}
}

// ----------

type LogFileIterator struct {
	// streaming mode (current/incomplete file): one entry read at a time.
	r       io.Reader
	sizeBuf []byte
	// cached mode (completed file): iterate the shared decoded slice. Entries are
	// read-only and shared across subscribers, so they are never mutated here.
	cached    []*filer_pb.LogEntry
	cachedPos int
	loadErr   error
	startTsNs int64
	stopTsNs  int64
	filePath  string
}

func newLogFileIterator(masterClient *wdclient.MasterClient, cache *persistedLogCache, fileEntry *Entry, fileTsNs, startTsNs, stopTsNs int64) *LogFileIterator {
	filePath := string(fileEntry.FullPath)
	if cache != nil && logFileIsCacheable(fileTsNs) {
		chunks := fileEntry.GetChunks()
		fingerprint := chunksFingerprint(chunks)
		entries, err := cache.getOrLoad(filePath, fingerprint, func() ([]*filer_pb.LogEntry, bool, error) {
			return loadLogFileEntries(masterClient, filePath, chunks)
		})
		return &LogFileIterator{
			cached:    entries,
			loadErr:   err,
			startTsNs: startTsNs,
			stopTsNs:  stopTsNs,
			filePath:  filePath,
		}
	}
	return &LogFileIterator{
		r:         NewChunkStreamReaderFromFiler(context.Background(), masterClient, fileEntry.Chunks),
		sizeBuf:   make([]byte, 4),
		startTsNs: startTsNs,
		stopTsNs:  stopTsNs,
		filePath:  filePath,
	}
}

func (iter *LogFileIterator) Close() error {
	if r, ok := iter.r.(io.Closer); ok {
		return r.Close()
	}
	return nil
}

// getNext will return io.EOF when done
func (iter *LogFileIterator) getNext() (logEntry *filer_pb.LogEntry, err error) {
	if iter.r == nil {
		return iter.getNextCached()
	}
	var n int
	for {
		n, err = iter.r.Read(iter.sizeBuf)
		if err != nil {
			return
		}
		if n != 4 {
			return nil, fmt.Errorf("size %d bytes, expected 4 bytes", n)
		}
		size := util.BytesToUint32(iter.sizeBuf)
		if size > maxLogEntrySize {
			return nil, fmt.Errorf("%s entry size %d exceeds %d", iter.filePath, size, maxLogEntrySize)
		}
		entryData := make([]byte, size)
		n, err = iter.r.Read(entryData)
		if err != nil {
			return
		}
		if n != int(size) {
			return nil, fmt.Errorf("entry data %d bytes, expected %d bytes", n, size)
		}
		logEntry = &filer_pb.LogEntry{}
		if err = proto.Unmarshal(entryData, logEntry); err != nil {
			return
		}
		if logEntry.TsNs <= iter.startTsNs {
			continue
		}
		if iter.stopTsNs != 0 && logEntry.TsNs > iter.stopTsNs {
			return nil, io.EOF
		}
		return
	}
}

// getNextCached yields entries from the shared cached slice, applying the same
// per-subscriber timestamp filtering as the streaming path. Any load error is
// surfaced only after the read entries are yielded, so a partial read delivers
// its prefix before failing, just like the streaming path.
func (iter *LogFileIterator) getNextCached() (logEntry *filer_pb.LogEntry, err error) {
	for iter.cachedPos < len(iter.cached) {
		logEntry = iter.cached[iter.cachedPos]
		iter.cachedPos++
		if logEntry.TsNs <= iter.startTsNs {
			continue
		}
		if iter.stopTsNs != 0 && logEntry.TsNs > iter.stopTsNs {
			return nil, io.EOF
		}
		return logEntry, nil
	}
	if iter.loadErr != nil {
		err = iter.loadErr
		iter.loadErr = nil
		return nil, err
	}
	return nil, io.EOF
}
