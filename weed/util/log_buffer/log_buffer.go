package log_buffer

import (
	"bytes"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

const BufferSize = 4 * 1024 * 1024
const PreviousBufferCount = 3

type dataToFlush struct {
	startTime time.Time
	stopTime  time.Time
	data      *bytes.Buffer
}

type LogBuffer struct {
	name          string
	prevBuffers   *SealedBuffers
	buf           []byte
	idx           []int
	pos           int
	startTime     time.Time
	stopTime      time.Time
	lastFlushTime time.Time
	sizeBuf       []byte
	flushInterval time.Duration
	flushFn       func(startTime, stopTime time.Time, buf []byte)
	notifyFn      func()
	isStopping    bool
	flushChan     chan *dataToFlush
	lastTsNs      int64
	sync.RWMutex
}

func NewLogBuffer(name string, flushInterval time.Duration, flushFn func(startTime, stopTime time.Time, buf []byte), notifyFn func()) *LogBuffer {
	lb := &LogBuffer{
		name:          name,
		prevBuffers:   newSealedBuffers(PreviousBufferCount),
		buf:           make([]byte, BufferSize),
		sizeBuf:       make([]byte, 4),
		flushInterval: flushInterval,
		flushFn:       flushFn,
		notifyFn:      notifyFn,
		flushChan:     make(chan *dataToFlush, 256),
	}
	go lb.loopFlush()
	go lb.loopInterval()
	return lb
}

func (m *LogBuffer) AddToBuffer(partitionKey, data []byte, processingTsNs int64) {

	var toFlush *dataToFlush
	m.Lock()
	defer func() {
		m.Unlock()
		if toFlush != nil {
			m.flushChan <- toFlush
		}
		if m.notifyFn != nil {
			m.notifyFn()
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
	if m.lastTsNs >= processingTsNs {
		// this is unlikely to happen, but just in case
		processingTsNs = m.lastTsNs + 1
		ts = time.Unix(0, processingTsNs)
	}
	m.lastTsNs = processingTsNs
	logEntry := &filer_pb.LogEntry{
		TsNs:             processingTsNs,
		PartitionKeyHash: util.HashToInt32(partitionKey),
		Data:             data,
	}

	logEntryData, _ := proto.Marshal(logEntry)

	size := len(logEntryData)

	if m.pos == 0 {
		m.startTime = ts
	}

	if m.startTime.Add(m.flushInterval).Before(ts) || len(m.buf)-m.pos < size+4 {
		// glog.V(4).Infof("%s copyToFlush1 start time %v, ts %v, remaining %d bytes", m.name, m.startTime, ts, len(m.buf)-m.pos)
		toFlush = m.copyToFlush()
		m.startTime = ts
		if len(m.buf) < size+4 {
			m.buf = make([]byte, 2*size+4)
		}
	}
	m.stopTime = ts

	m.idx = append(m.idx, m.pos)
	util.Uint32toBytes(m.sizeBuf, uint32(size))
	copy(m.buf[m.pos:m.pos+4], m.sizeBuf)
	copy(m.buf[m.pos+4:m.pos+4+size], logEntryData)
	m.pos += size + 4

	// fmt.Printf("entry size %d total %d count %d, buffer:%p\n", size, m.pos, len(m.idx), m)

}

func (m *LogBuffer) Shutdown() {
	m.Lock()
	defer m.Unlock()

	if m.isStopping {
		return
	}
	m.isStopping = true
	toFlush := m.copyToFlush()
	m.flushChan <- toFlush
	close(m.flushChan)
}

func (m *LogBuffer) loopFlush() {
	for d := range m.flushChan {
		if d != nil {
			// glog.V(4).Infof("%s flush [%v, %v] size %d", m.name, d.startTime, d.stopTime, len(d.data.Bytes()))
			m.flushFn(d.startTime, d.stopTime, d.data.Bytes())
			d.releaseMemory()
			// local logbuffer is different from aggregate logbuffer here
			m.lastFlushTime = d.stopTime
		}
	}
}

func (m *LogBuffer) loopInterval() {
	for !m.isStopping {
		time.Sleep(m.flushInterval)
		m.Lock()
		if m.isStopping {
			m.Unlock()
			return
		}
		toFlush := m.copyToFlush()
		m.Unlock()
		if toFlush != nil {
			m.flushChan <- toFlush
		}
	}
}

func (m *LogBuffer) copyToFlush() *dataToFlush {

	if m.pos > 0 {
		// fmt.Printf("flush buffer %d pos %d empty space %d\n", len(m.buf), m.pos, len(m.buf)-m.pos)
		var d *dataToFlush
		if m.flushFn != nil {
			d = &dataToFlush{
				startTime: m.startTime,
				stopTime:  m.stopTime,
				data:      copiedBytes(m.buf[:m.pos]),
			}
			// glog.V(4).Infof("%s flushing [0,%d) with %d entries [%v, %v]", m.name, m.pos, len(m.idx), m.startTime, m.stopTime)
		} else {
			// glog.V(4).Infof("%s removed from memory [0,%d) with %d entries [%v, %v]", m.name, m.pos, len(m.idx), m.startTime, m.stopTime)
			m.lastFlushTime = m.stopTime
		}
		m.buf = m.prevBuffers.SealBuffer(m.startTime, m.stopTime, m.buf, m.pos)
		m.startTime = time.Unix(0, 0)
		m.stopTime = time.Unix(0, 0)
		m.pos = 0
		m.idx = m.idx[:0]
		return d
	}
	return nil
}

func (d *dataToFlush) releaseMemory() {
	d.data.Reset()
	bufferPool.Put(d.data)
}

func (m *LogBuffer) ReadFromBuffer(lastReadTime time.Time) (bufferCopy *bytes.Buffer, err error) {
	m.RLock()
	defer m.RUnlock()

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
	if !m.startTime.IsZero() {
		tsMemory = m.startTime
	}
	for _, prevBuf := range m.prevBuffers.buffers {
		if !prevBuf.startTime.IsZero() && prevBuf.startTime.Before(tsMemory) {
			tsMemory = prevBuf.startTime
		}
	}
	if tsMemory.IsZero() { // case 2.2
		return nil, nil
	} else if lastReadTime.Before(tsMemory) { // case 2.3
		if !m.lastFlushTime.IsZero() {
			glog.V(0).Infof("resume with last flush time: %v", m.lastFlushTime)
			return nil, ResumeFromDiskError
		}
	}

	// the following is case 2.1

	if lastReadTime.Equal(m.stopTime) {
		return nil, nil
	}
	if lastReadTime.After(m.stopTime) {
		// glog.Fatalf("unexpected last read time %v, older than latest %v", lastReadTime, m.stopTime)
		return nil, nil
	}
	if lastReadTime.Before(m.startTime) {
		// println("checking ", lastReadTime.UnixNano())
		for _, buf := range m.prevBuffers.buffers {
			if buf.startTime.After(lastReadTime) {
				// glog.V(4).Infof("%s return the %d sealed buffer %v", m.name, i, buf.startTime)
				// println("return the", i, "th in memory", buf.startTime.UnixNano())
				return copiedBytes(buf.buf[:buf.size]), nil
			}
			if !buf.startTime.After(lastReadTime) && buf.stopTime.After(lastReadTime) {
				pos := buf.locateByTs(lastReadTime)
				// fmt.Printf("locate buffer[%d] pos %d\n", i, pos)
				return copiedBytes(buf.buf[pos:buf.size]), nil
			}
		}
		// glog.V(4).Infof("%s return the current buf %v", m.name, lastReadTime)
		return copiedBytes(m.buf[:m.pos]), nil
	}

	lastTs := lastReadTime.UnixNano()
	l, h := 0, len(m.idx)-1

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
		pos := m.idx[mid]
		_, t := readTs(m.buf, pos)
		if t <= lastTs {
			l = mid + 1
		} else if lastTs < t {
			var prevT int64
			if mid > 0 {
				_, prevT = readTs(m.buf, m.idx[mid-1])
			}
			if prevT <= lastTs {
				// fmt.Printf("found l=%d, m-1=%d(ts=%d), m=%d(ts=%d), h=%d [%d, %d) \n", l, mid-1, prevT, mid, t, h, pos, m.pos)
				return copiedBytes(m.buf[pos:m.pos]), nil
			}
			h = mid
		}
		// fmt.Printf("l=%d, h=%d\n", l, h)
	}

	// FIXME: this could be that the buffer has been flushed already
	return nil, nil

}
func (m *LogBuffer) ReleaseMemory(b *bytes.Buffer) {
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
