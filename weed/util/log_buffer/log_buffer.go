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

func NewLogBuffer(flushInterval time.Duration, flushFn func(startTime, stopTime time.Time, buf []byte), notifyFn func()) *LogBuffer {
	lb := &LogBuffer{
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

func (m *LogBuffer) AddToBuffer(partitionKey, data []byte, eventTsNs int64) {

	m.Lock()
	defer func() {
		m.Unlock()
		if m.notifyFn != nil {
			m.notifyFn()
		}
	}()

	// need to put the timestamp inside the lock
	var ts time.Time
	if eventTsNs == 0 {
		ts = time.Now()
		eventTsNs = ts.UnixNano()
	} else {
		ts = time.Unix(0, eventTsNs)
	}
	if m.lastTsNs >= eventTsNs {
		// this is unlikely to happen, but just in case
		eventTsNs = m.lastTsNs + 1
		ts = time.Unix(0, eventTsNs)
	}
	m.lastTsNs = eventTsNs
	logEntry := &filer_pb.LogEntry{
		TsNs:             eventTsNs,
		PartitionKeyHash: util.HashToInt32(partitionKey),
		Data:             data,
	}

	logEntryData, _ := proto.Marshal(logEntry)

	size := len(logEntryData)

	if m.pos == 0 {
		m.startTime = ts
	}

	if m.startTime.Add(m.flushInterval).Before(ts) || len(m.buf)-m.pos < size+4 {
		m.flushChan <- m.copyToFlush()
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
			// fmt.Printf("flush [%v, %v] size %d\n", d.startTime, d.stopTime, len(d.data.Bytes()))
			m.flushFn(d.startTime, d.stopTime, d.data.Bytes())
			d.releaseMemory()
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
		// println("loop interval")
		toFlush := m.copyToFlush()
		m.flushChan <- toFlush
		m.Unlock()
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
		}
		// fmt.Printf("flusing [0,%d) with %d entries\n", m.pos, len(m.idx))
		m.buf = m.prevBuffers.SealBuffer(m.startTime, m.stopTime, m.buf, m.pos)
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

	if !m.lastFlushTime.IsZero() && m.lastFlushTime.After(lastReadTime) {
		return nil, ResumeFromDiskError
	}

	/*
		fmt.Printf("read buffer %p: %v last stop time: [%v,%v], pos %d, entries:%d, prevBufs:%d\n", m, lastReadTime, m.startTime, m.stopTime, m.pos, len(m.idx), len(m.prevBuffers.buffers))
		for i, prevBuf := range m.prevBuffers.buffers {
			fmt.Printf("  prev %d : %s\n", i, prevBuf.String())
		}
	*/

	if lastReadTime.Equal(m.stopTime) {
		return nil, nil
	}
	if lastReadTime.After(m.stopTime) {
		// glog.Fatalf("unexpected last read time %v, older than latest %v", lastReadTime, m.stopTime)
		return nil, nil
	}
	if lastReadTime.Before(m.startTime) {
		// println("checking ", lastReadTime.UnixNano())
		for i, buf := range m.prevBuffers.buffers {
			if buf.startTime.After(lastReadTime) {
				if i == 0 {
					// println("return the earliest in memory", buf.startTime.UnixNano())
					return copiedBytes(buf.buf[:buf.size]), nil
				}
				// println("return the", i, "th in memory", buf.startTime.UnixNano())
				return copiedBytes(buf.buf[:buf.size]), nil
			}
			if !buf.startTime.After(lastReadTime) && buf.stopTime.After(lastReadTime) {
				pos := buf.locateByTs(lastReadTime)
				// fmt.Printf("locate buffer[%d] pos %d\n", i, pos)
				return copiedBytes(buf.buf[pos:buf.size]), nil
			}
		}
		// println("return the current buf", lastReadTime.UnixNano())
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
