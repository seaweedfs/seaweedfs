package queue

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type LogBuffer struct {
	buf           []byte
	pos           int
	startTime     time.Time
	stopTime      time.Time
	sizeBuf       []byte
	flushInterval time.Duration
	flushFn       func(startTime, stopTime time.Time, buf []byte)
	isStopping    bool
	sync.Mutex
}

func NewLogBuffer(flushInterval time.Duration, flushFn func(startTime, stopTime time.Time, buf []byte)) *LogBuffer {
	lb := &LogBuffer{
		buf:           make([]byte, 4*0124*1024),
		sizeBuf:       make([]byte, 4),
		flushInterval: flushInterval,
		flushFn:       flushFn,
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
	defer m.Unlock()

	if m.pos == 0 {
		m.startTime = ts
	}

	if m.startTime.Add(m.flushInterval).Before(ts) || len(m.buf)-m.pos < size+4 {
		m.flush()
		m.startTime = ts
	}
	m.stopTime = ts

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
		m.flushFn(m.startTime, m.stopTime, m.buf[:m.pos])
		m.pos = 0
	}
}
