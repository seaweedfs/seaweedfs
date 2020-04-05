package queue

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type LogBuffer struct {
	buf           []byte
	idx           []int
	pos           int
	startTime     time.Time
	stopTime      time.Time
	sizeBuf       []byte
	flushInterval time.Duration
	flushFn       func(startTime, stopTime time.Time, buf []byte)
	notifyFn      func()
	isStopping    bool
	sync.RWMutex
}

func NewLogBuffer(flushInterval time.Duration, flushFn func(startTime, stopTime time.Time, buf []byte), notifyFn func()) *LogBuffer {
	lb := &LogBuffer{
		buf:           make([]byte, 4*1024*1024),
		sizeBuf:       make([]byte, 4),
		flushInterval: flushInterval,
		flushFn:       flushFn,
		notifyFn:      notifyFn,
	}
	go lb.loopFlush()
	return lb
}

func (m *LogBuffer) AddToBuffer(ts time.Time, key, data []byte) {

	logEntry := &filer_pb.LogEntry{
		TsNs:             ts.UnixNano(),
		PartitionKeyHash: util.HashToInt32(key),
		Data:             data,
	}

	logEntryData, _ := proto.Marshal(logEntry)

	size := len(logEntryData)

	m.Lock()
	defer func() {
		m.Unlock()
		if m.notifyFn != nil {
			m.notifyFn()
		}
	}()

	if m.pos == 0 {
		m.startTime = ts
	}

	if m.startTime.Add(m.flushInterval).Before(ts) || len(m.buf)-m.pos < size+4 {
		m.flush()
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
}

func (m *LogBuffer) Shutdown() {
	if m.isStopping {
		return
	}
	m.isStopping = true
	m.Lock()
	m.flush()
	m.Unlock()
}

func (m *LogBuffer) loopFlush() {
	for !m.isStopping {
		m.Lock()
		m.flush()
		m.Unlock()
		time.Sleep(m.flushInterval)
	}
}

func (m *LogBuffer) flush() {

	if m.flushFn != nil && m.pos > 0 {
		// fmt.Printf("flush buffer %d pos %d empty space %d\n", len(m.buf), m.pos, len(m.buf)-m.pos)
		m.flushFn(m.startTime, m.stopTime, m.buf[:m.pos])
		m.pos = 0
		m.idx = m.idx[:0]
	}
}

func (m *LogBuffer) ReadFromBuffer(lastReadTime time.Time) (ts time.Time, bufferCopy []byte) {
	m.RLock()
	defer m.RUnlock()

	// fmt.Printf("read from buffer: %v\n", lastReadTime)

	if lastReadTime.Equal(m.stopTime) {
		return lastReadTime, nil
	}
	if lastReadTime.After(m.stopTime) {
		// glog.Fatalf("unexpected last read time %v, older than latest %v", lastReadTime, m.stopTime)
		return lastReadTime, nil
	}
	if lastReadTime.Before(m.startTime) {
		return m.stopTime, copiedBytes(m.buf[:m.pos])
	}

	lastTs := lastReadTime.UnixNano()
	l, h := 0, len(m.idx)-1

	// fmt.Printf("l=%d, h=%d\n", l, h)
	for {
		mid := (l + h) / 2
		pos := m.idx[mid]
		t := readTs(m.buf, m.idx[mid])
		if t <= lastTs {
			l = mid + 1
		} else if lastTs < t {
			var prevT int64
			if mid > 0 {
				prevT = readTs(m.buf, m.idx[mid-1])
			}
			if prevT <= lastTs {
				return time.Unix(0, t), copiedBytes(m.buf[pos:m.pos])
			}
			h = mid - 1
		}
		// fmt.Printf("l=%d, h=%d\n", l, h)
	}

}
func copiedBytes(buf []byte) (copied []byte) {
	copied = make([]byte, len(buf))
	copy(copied, buf)
	return
}

func readTs(buf []byte, pos int) int64 {

	size := util.BytesToUint32(buf[pos : pos+4])
	entryData := buf[pos+4 : pos+4+int(size)]
	logEntry := &filer_pb.LogEntry{}

	err := proto.Unmarshal(entryData, logEntry)
	if err != nil {
		glog.Fatalf("unexpected unmarshal filer_pb.LogEntry: %v", err)
	}
	return logEntry.TsNs

}
