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

	startDate := fmt.Sprintf("%04d-%02d-%02d", startPosition.Year(), startPosition.Month(), startPosition.Day())

	dayEntries, _, listDayErr := f.ListDirectoryEntries(context.Background(), SystemLogDir, startDate, true, math.MaxInt32, "", "", "")
	if listDayErr != nil {
		return nil, fmt.Errorf("fail to list log by day: %v", listDayErr)
	}

	return NewOrderedLogVisitor(f, startPosition, stopTsNs, dayEntries)

}

func (f *Filer) HasPersistedLogFiles(startPosition log_buffer.MessagePosition) (bool, error) {
	startDate := fmt.Sprintf("%04d-%02d-%02d", startPosition.Year(), startPosition.Month(), startPosition.Day())
	dayEntries, _, listDayErr := f.ListDirectoryEntries(context.Background(), SystemLogDir, startDate, true, 1, "", "", "")

	if listDayErr != nil {
		return false, fmt.Errorf("fail to list log by day: %v", listDayErr)
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
			return nil, fmt.Errorf("failed to get next log entry: %v", nextErr)
		}
	} else {
		heap.Push(o.pq, &LogEntryItem{
			Entry: next,
			filer: filerId,
		})
	}
	return item.Entry, nil
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

	startDate := fmt.Sprintf("%04d-%02d-%02d", startPosition.Year(), startPosition.Month(), startPosition.Day())
	startHourMinute := fmt.Sprintf("%02d-%02d", startPosition.Hour(), startPosition.Minute())
	var stopDate, stopHourMinute string
	if stopTsNs != 0 {
		stopTime := time.Unix(0, stopTsNs+24*60*60*int64(time.Nanosecond)).UTC()
		stopDate = fmt.Sprintf("%04d-%02d-%02d", stopTime.Year(), stopTime.Month(), stopTime.Day())
		stopHourMinute = fmt.Sprintf("%02d-%02d", stopTime.Hour(), stopTime.Minute())
	}

	return &LogFileEntryCollector{
		f:               f,
		startTsNs:       startPosition.UnixNano(),
		stopTsNs:        stopTsNs,
		dayEntryQueue:   dayEntryQueue,
		startDate:       startDate,
		startHourMinute: startHourMinute,
		stopDate:        stopDate,
		stopHourMinute:  stopHourMinute,
	}
}

func (c *LogFileEntryCollector) hasMore() bool {
	return c.dayEntryQueue.Len() > 0
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
		iter, found := v.perFilerIteratorMap[filerId]
		if !found {
			iter = newLogFileQueueIterator(c.f.MasterClient, util.NewQueue[*LogFileEntry](), c.startTsNs, c.stopTsNs)
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
				return fmt.Errorf("failed to get next log entry for %v: %v", entryName, err)
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
	startTsNs           int64
	stopTsNs            int64
	currentFileIterator *LogFileIterator
}

func newLogFileQueueIterator(masterClient *wdclient.MasterClient, q *util.Queue[*LogFileEntry], startTsNs, stopTsNs int64) *LogFileQueueIterator {
	return &LogFileQueueIterator{
		q:            q,
		masterClient: masterClient,
		startTsNs:    startTsNs,
		stopTsNs:     stopTsNs,
	}
}

// getNext will return io.EOF when done
func (iter *LogFileQueueIterator) getNext(v *OrderedLogVisitor) (logEntry *filer_pb.LogEntry, err error) {
	for {
		if iter.currentFileIterator != nil {
			logEntry, err = iter.currentFileIterator.getNext()
			if err != io.EOF {
				return
			}
		}
		// now either iter.currentFileIterator is nil or err is io.EOF
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
		}
		// skip the file if the next entry is before the startTsNs
		if next != nil && next.TsNs <= iter.startTsNs {
			continue
		}
		iter.currentFileIterator = newLogFileIterator(iter.masterClient, t.FileEntry, iter.startTsNs, iter.stopTsNs)
	}
}

// ----------

type LogFileIterator struct {
	r         io.Reader
	sizeBuf   []byte
	startTsNs int64
	stopTsNs  int64
}

func newLogFileIterator(masterClient *wdclient.MasterClient, fileEntry *Entry, startTsNs, stopTsNs int64) *LogFileIterator {
	return &LogFileIterator{
		r:         NewChunkStreamReaderFromFiler(context.Background(), masterClient, fileEntry.Chunks),
		sizeBuf:   make([]byte, 4),
		startTsNs: startTsNs,
		stopTsNs:  stopTsNs,
	}
}

// getNext will return io.EOF when done
func (iter *LogFileIterator) getNext() (logEntry *filer_pb.LogEntry, err error) {
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
		// println("entry size", size)
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
