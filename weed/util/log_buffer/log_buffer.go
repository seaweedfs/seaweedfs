package log_buffer

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const BufferSize = 8 * 1024 * 1024
const PreviousBufferCount = 32

type dataToFlush struct {
	startTime time.Time
	stopTime  time.Time
	data      *bytes.Buffer
}

type EachLogEntryFuncType func(logEntry *filer_pb.LogEntry) (isDone bool, err error)
type LogFlushFuncType func(logBuffer *LogBuffer, startTime, stopTime time.Time, buf []byte)
type LogReadFromDiskFuncType func(startPosition MessagePosition, stopTsNs int64, eachLogEntryFn EachLogEntryFuncType) (lastReadPosition MessagePosition, isDone bool, err error)

type LogBuffer struct {
	LastFlushTsNs     int64
	name              string
	prevBuffers       *SealedBuffers
	buf               []byte
	batchIndex        int64
	idx               []int
	pos               int
	startTime         time.Time
	stopTime          time.Time
	lastFlushDataTime time.Time
	sizeBuf           []byte
	flushInterval     time.Duration
	flushFn           LogFlushFuncType
	ReadFromDiskFn    LogReadFromDiskFuncType
	notifyFn          func()
	isStopping        *atomic.Bool
	isAllFlushed      bool
	flushChan         chan *dataToFlush
	LastTsNs          atomic.Int64
	sync.RWMutex
}

func NewLogBuffer(name string, flushInterval time.Duration, flushFn LogFlushFuncType,
	readFromDiskFn LogReadFromDiskFuncType, notifyFn func()) *LogBuffer {
	lb := &LogBuffer{
		name:           name,
		prevBuffers:    newSealedBuffers(PreviousBufferCount),
		buf:            make([]byte, BufferSize),
		sizeBuf:        make([]byte, 4),
		flushInterval:  flushInterval,
		flushFn:        flushFn,
		ReadFromDiskFn: readFromDiskFn,
		notifyFn:       notifyFn,
		flushChan:      make(chan *dataToFlush, 256),
		isStopping:     new(atomic.Bool),
	}
	go lb.loopFlush()
	go lb.loopInterval()
	return lb
}

func (logBuffer *LogBuffer) AddToBuffer(message *mq_pb.DataMessage) {
	logBuffer.AddDataToBuffer(message.Key, message.Value, message.TsNs)
}

func (logBuffer *LogBuffer) AddDataToBuffer(partitionKey, data []byte, processingTsNs int64) {

	var toFlush *dataToFlush
	logBuffer.Lock()
	defer func() {
		logBuffer.Unlock()
		if toFlush != nil {
			logBuffer.flushChan <- toFlush
		}
		if logBuffer.notifyFn != nil {
			logBuffer.notifyFn()
		}
	}()

	// need to put the timestamp inside the lock
	var ts time.Time
	if processingTsNs == 0 {
		ts = time.Now()
		processingTsNs = ts.UnixNano()
	} else {
		ts = time.Unix(0, processingTsNs)
	}
	if logBuffer.LastTsNs.Load() >= processingTsNs {
		// this is unlikely to happen, but just in case
		processingTsNs = logBuffer.LastTsNs.Add(1)
		ts = time.Unix(0, processingTsNs)
	}
	logBuffer.LastTsNs.Store(processingTsNs)
	logEntry := &filer_pb.LogEntry{
		TsNs:             processingTsNs,
		PartitionKeyHash: util.HashToInt32(partitionKey),
		Data:             data,
		Key:              partitionKey,
	}

	logEntryData, _ := proto.Marshal(logEntry)

	size := len(logEntryData)

	if logBuffer.pos == 0 {
		logBuffer.startTime = ts
	}

	if logBuffer.startTime.Add(logBuffer.flushInterval).Before(ts) || len(logBuffer.buf)-logBuffer.pos < size+4 {
		// glog.V(0).Infof("%s copyToFlush1 batch:%d count:%d start time %v, ts %v, remaining %d bytes", logBuffer.name, logBuffer.batchIndex, len(logBuffer.idx), logBuffer.startTime, ts, len(logBuffer.buf)-logBuffer.pos)
		toFlush = logBuffer.copyToFlush()
		logBuffer.startTime = ts
		if len(logBuffer.buf) < size+4 {
			logBuffer.buf = make([]byte, 2*size+4)
		}
	}
	logBuffer.stopTime = ts

	logBuffer.idx = append(logBuffer.idx, logBuffer.pos)
	util.Uint32toBytes(logBuffer.sizeBuf, uint32(size))
	copy(logBuffer.buf[logBuffer.pos:logBuffer.pos+4], logBuffer.sizeBuf)
	copy(logBuffer.buf[logBuffer.pos+4:logBuffer.pos+4+size], logEntryData)
	logBuffer.pos += size + 4

	// fmt.Printf("partitionKey %v entry size %d total %d count %d\n", string(partitionKey), size, m.pos, len(m.idx))

}

func (logBuffer *LogBuffer) IsStopping() bool {
	return logBuffer.isStopping.Load()
}

// ShutdownLogBuffer flushes the buffer and stops the log buffer
func (logBuffer *LogBuffer) ShutdownLogBuffer() {
	isAlreadyStopped := logBuffer.isStopping.Swap(true)
	if isAlreadyStopped {
		return
	}
	toFlush := logBuffer.copyToFlush()
	logBuffer.flushChan <- toFlush
	close(logBuffer.flushChan)
}

// IsAllFlushed returns true if all data in the buffer has been flushed, after calling ShutdownLogBuffer().
func (logBuffer *LogBuffer) IsAllFlushed() bool {
	return logBuffer.isAllFlushed
}

func (logBuffer *LogBuffer) loopFlush() {
	for d := range logBuffer.flushChan {
		if d != nil {
			// glog.V(4).Infof("%s flush [%v, %v] size %d", m.name, d.startTime, d.stopTime, len(d.data.Bytes()))
			logBuffer.flushFn(logBuffer, d.startTime, d.stopTime, d.data.Bytes())
			d.releaseMemory()
			// local logbuffer is different from aggregate logbuffer here
			logBuffer.lastFlushDataTime = d.stopTime
		}
	}
	logBuffer.isAllFlushed = true
}

func (logBuffer *LogBuffer) loopInterval() {
	for !logBuffer.IsStopping() {
		time.Sleep(logBuffer.flushInterval)
		if logBuffer.IsStopping() {
			return
		}
		logBuffer.Lock()
		toFlush := logBuffer.copyToFlush()
		logBuffer.Unlock()
		if toFlush != nil {
			glog.V(4).Infof("%s flush [%v, %v] size %d", logBuffer.name, toFlush.startTime, toFlush.stopTime, len(toFlush.data.Bytes()))
			logBuffer.flushChan <- toFlush
		} else {
			// glog.V(0).Infof("%s no flush", m.name)
		}
	}
}

func (logBuffer *LogBuffer) copyToFlush() *dataToFlush {

	if logBuffer.pos > 0 {
		// fmt.Printf("flush buffer %d pos %d empty space %d\n", len(m.buf), m.pos, len(m.buf)-m.pos)
		var d *dataToFlush
		if logBuffer.flushFn != nil {
			d = &dataToFlush{
				startTime: logBuffer.startTime,
				stopTime:  logBuffer.stopTime,
				data:      copiedBytes(logBuffer.buf[:logBuffer.pos]),
			}
			// glog.V(4).Infof("%s flushing [0,%d) with %d entries [%v, %v]", m.name, m.pos, len(m.idx), m.startTime, m.stopTime)
		} else {
			// glog.V(4).Infof("%s removed from memory [0,%d) with %d entries [%v, %v]", m.name, m.pos, len(m.idx), m.startTime, m.stopTime)
			logBuffer.lastFlushDataTime = logBuffer.stopTime
		}
		logBuffer.buf = logBuffer.prevBuffers.SealBuffer(logBuffer.startTime, logBuffer.stopTime, logBuffer.buf, logBuffer.pos, logBuffer.batchIndex)
		logBuffer.startTime = time.Unix(0, 0)
		logBuffer.stopTime = time.Unix(0, 0)
		logBuffer.pos = 0
		logBuffer.idx = logBuffer.idx[:0]
		logBuffer.batchIndex++
		return d
	}
	return nil
}

func (logBuffer *LogBuffer) GetEarliestTime() time.Time {
	return logBuffer.startTime
}
func (logBuffer *LogBuffer) GetEarliestPosition() MessagePosition {
	return MessagePosition{
		Time:       logBuffer.startTime,
		BatchIndex: logBuffer.batchIndex,
	}
}

func (d *dataToFlush) releaseMemory() {
	d.data.Reset()
	bufferPool.Put(d.data)
}

func (logBuffer *LogBuffer) ReadFromBuffer(lastReadPosition MessagePosition) (bufferCopy *bytes.Buffer, batchIndex int64, err error) {
	logBuffer.RLock()
	defer logBuffer.RUnlock()

	// Read from disk and memory
	//	1. read from disk, last time is = td
	//	2. in memory, the earliest time = tm
	//	if tm <= td, case 2.1
	//		read from memory
	//	if tm is empty, case 2.2
	//		read from memory
	//	if td < tm, case 2.3
	//		read from disk again
	var tsMemory time.Time
	var tsBatchIndex int64
	if !logBuffer.startTime.IsZero() {
		tsMemory = logBuffer.startTime
		tsBatchIndex = logBuffer.batchIndex
	}
	for _, prevBuf := range logBuffer.prevBuffers.buffers {
		if !prevBuf.startTime.IsZero() && prevBuf.startTime.Before(tsMemory) {
			tsMemory = prevBuf.startTime
			tsBatchIndex = prevBuf.batchIndex
		}
	}
	if tsMemory.IsZero() { // case 2.2
		// println("2.2 no data")
		return nil, -2, nil
	} else if lastReadPosition.Before(tsMemory) && lastReadPosition.BatchIndex+1 < tsBatchIndex { // case 2.3
		if !logBuffer.lastFlushDataTime.IsZero() {
			glog.V(0).Infof("resume with last flush time: %v", logBuffer.lastFlushDataTime)
			return nil, -2, ResumeFromDiskError
		}
	}

	// the following is case 2.1

	if lastReadPosition.Equal(logBuffer.stopTime) {
		return nil, logBuffer.batchIndex, nil
	}
	if lastReadPosition.After(logBuffer.stopTime) {
		// glog.Fatalf("unexpected last read time %v, older than latest %v", lastReadPosition, m.stopTime)
		return nil, logBuffer.batchIndex, nil
	}
	if lastReadPosition.Before(logBuffer.startTime) {
		// println("checking ", lastReadPosition.UnixNano())
		for _, buf := range logBuffer.prevBuffers.buffers {
			if buf.startTime.After(lastReadPosition.Time) {
				// glog.V(4).Infof("%s return the %d sealed buffer %v", m.name, i, buf.startTime)
				// println("return the", i, "th in memory", buf.startTime.UnixNano())
				return copiedBytes(buf.buf[:buf.size]), buf.batchIndex, nil
			}
			if !buf.startTime.After(lastReadPosition.Time) && buf.stopTime.After(lastReadPosition.Time) {
				pos := buf.locateByTs(lastReadPosition.Time)
				// fmt.Printf("locate buffer[%d] pos %d\n", i, pos)
				return copiedBytes(buf.buf[pos:buf.size]), buf.batchIndex, nil
			}
		}
		// glog.V(4).Infof("%s return the current buf %v", m.name, lastReadPosition)
		return copiedBytes(logBuffer.buf[:logBuffer.pos]), logBuffer.batchIndex, nil
	}

	lastTs := lastReadPosition.UnixNano()
	l, h := 0, len(logBuffer.idx)-1

	/*
		for i, pos := range m.idx {
			logEntry, ts := readTs(m.buf, pos)
			event := &filer_pb.SubscribeMetadataResponse{}
			proto.Unmarshal(logEntry.Data, event)
			entry := event.EventNotification.OldEntry
			if entry == nil {
				entry = event.EventNotification.NewEntry
			}
			fmt.Printf("entry %d ts: %v offset:%d dir:%s name:%s\n", i, time.Unix(0, ts), pos, event.Directory, entry.Name)
		}
		fmt.Printf("l=%d, h=%d\n", l, h)
	*/

	for l <= h {
		mid := (l + h) / 2
		pos := logBuffer.idx[mid]
		_, t := readTs(logBuffer.buf, pos)
		if t <= lastTs {
			l = mid + 1
		} else if lastTs < t {
			var prevT int64
			if mid > 0 {
				_, prevT = readTs(logBuffer.buf, logBuffer.idx[mid-1])
			}
			if prevT <= lastTs {
				// fmt.Printf("found l=%d, m-1=%d(ts=%d), m=%d(ts=%d), h=%d [%d, %d) \n", l, mid-1, prevT, mid, t, h, pos, m.pos)
				return copiedBytes(logBuffer.buf[pos:logBuffer.pos]), logBuffer.batchIndex, nil
			}
			h = mid
		}
		// fmt.Printf("l=%d, h=%d\n", l, h)
	}

	// FIXME: this could be that the buffer has been flushed already
	println("Not sure why no data", lastReadPosition.BatchIndex, tsBatchIndex)
	return nil, -2, nil

}
func (logBuffer *LogBuffer) ReleaseMemory(b *bytes.Buffer) {
	bufferPool.Put(b)
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func copiedBytes(buf []byte) (copied *bytes.Buffer) {
	copied = bufferPool.Get().(*bytes.Buffer)
	copied.Reset()
	copied.Write(buf)
	return
}

func readTs(buf []byte, pos int) (size int, ts int64) {

	size = int(util.BytesToUint32(buf[pos : pos+4]))
	entryData := buf[pos+4 : pos+4+size]
	logEntry := &filer_pb.LogEntry{}

	err := proto.Unmarshal(entryData, logEntry)
	if err != nil {
		glog.Fatalf("unexpected unmarshal filer_pb.LogEntry: %v", err)
	}
	return size, logEntry.TsNs

}
