package buffered_writer

import (
	"io"
	"sync"
	"time"
)

type TimedWriteBuffer struct {
	maxLatencyWriterAt *maxLatencyWriterAt
	bufWriterAt        *bufferedWriterAt
}

func (t *TimedWriteBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	bufStart := t.bufWriterAt.nextOffset - int64(t.bufWriterAt.dataSize)
	start := max(bufStart, off)
	stop := min(t.bufWriterAt.nextOffset, off+int64(len(p)))
	if start <= stop {
		n = copy(p, t.bufWriterAt.data[start-bufStart:stop-bufStart])
	}
	return
}
func (t *TimedWriteBuffer) WriteAt(p []byte, offset int64) (n int, err error) {
	return t.maxLatencyWriterAt.WriteAt(p, offset)
}
func (t *TimedWriteBuffer) Flush() {
	t.maxLatencyWriterAt.Flush()
}
func (t *TimedWriteBuffer) Close() {
	t.maxLatencyWriterAt.Close()
}

func NewTimedWriteBuffer(writerAt io.WriterAt, size int, latency time.Duration, currentOffset int64) *TimedWriteBuffer {
	bufWriterAt := newBufferedWriterAt(writerAt, size, currentOffset)
	maxLatencyWriterAt := newMaxLatencyWriterAt(bufWriterAt, latency)
	return &TimedWriteBuffer{
		bufWriterAt:        bufWriterAt,
		maxLatencyWriterAt: maxLatencyWriterAt,
	}
}

type bufferedWriterAt struct {
	data       []byte
	dataSize   int
	nextOffset int64
	writerAt   io.WriterAt
	counter    int
}

func newBufferedWriterAt(writerAt io.WriterAt, bufferSize int, currentOffset int64) *bufferedWriterAt {
	return &bufferedWriterAt{
		data:       make([]byte, bufferSize),
		nextOffset: currentOffset,
		dataSize:   0,
		writerAt:   writerAt,
	}
}

func (b *bufferedWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	if b.nextOffset != offset {
		println("nextOffset", b.nextOffset, "bufSize", b.dataSize, "offset", offset, "data", len(p))
	}
	if b.nextOffset != offset || len(p)+b.dataSize > len(b.data) {
		if err = b.Flush(); err != nil {
			return 0, err
		}
	}
	if len(p)+b.dataSize > len(b.data) {
		n, err = b.writerAt.WriteAt(p, offset)
		if err == nil {
			b.nextOffset = offset + int64(n)
		}
	} else {
		n = copy(b.data[b.dataSize:len(p)+b.dataSize], p)
		b.dataSize += n
		b.nextOffset += int64(n)
		b.counter++
	}
	return
}

func (b *bufferedWriterAt) Flush() (err error) {
	if b.dataSize == 0 {
		return nil
	}
	// println("flush", b.counter)
	b.counter = 0
	_, err = b.writerAt.WriteAt(b.data[0:b.dataSize], b.nextOffset-int64(b.dataSize))
	if err == nil {
		b.dataSize = 0
	}
	return
}

// adapted from https://golang.org/src/net/http/httputil/reverseproxy.go

type writeFlusher interface {
	io.WriterAt
	Flush() error
}

type maxLatencyWriterAt struct {
	dst          writeFlusher
	latency      time.Duration // non-zero; negative means to flush immediately
	mu           sync.Mutex    // protects t, flushPending, and dst.Flush
	t            *time.Timer
	flushPending bool
}

func newMaxLatencyWriterAt(dst writeFlusher, latency time.Duration) *maxLatencyWriterAt {
	return &maxLatencyWriterAt{
		dst:     dst,
		latency: latency,
	}
}

func (m *maxLatencyWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, err = m.dst.WriteAt(p, offset)
	if m.latency < 0 {
		m.dst.Flush()
		return
	}
	if m.flushPending {
		return
	}
	if m.t == nil {
		m.t = time.AfterFunc(m.latency, m.Flush)
	} else {
		m.t.Reset(m.latency)
	}
	m.flushPending = true
	return
}

func (m *maxLatencyWriterAt) Flush() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.flushPending { // if stop was called but AfterFunc already started this goroutine
		return
	}
	m.dst.Flush()
	m.flushPending = false
}

func (m *maxLatencyWriterAt) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushPending = false
	if m.t != nil {
		m.t.Stop()
	}
}

func min(x, y int64) int64 {
	if x <= y {
		return x
	}
	return y
}
func max(x, y int64) int64 {
	if x <= y {
		return y
	}
	return x
}
