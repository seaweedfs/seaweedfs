package log_buffer

import (
	"fmt"
	"sync"
	"time"
)

type MemBuffer struct {
	buf         []byte
	size        int
	startTime   time.Time
	stopTime    time.Time
	startOffset int64 // First offset in this buffer
	offset      int64 // Last offset in this buffer (endOffset)

	// snapshot is a GC-owned copy of buf[:size] shared by all readers of this
	// sealed window, so N subscribers reading the same window cost one copy
	// instead of N pooled copies. Created lazily by the first reader; travels
	// with the window when SealBuffer shifts slots. Immutable once created.
	snapMu   sync.Mutex
	snapshot []byte
}

// sharedSnapshot returns the shared read-only copy of this sealed window,
// creating it on first use. Callers must hold the LogBuffer read lock, which
// keeps buf stable (SealBuffer mutates slots only under the write lock).
func (mb *MemBuffer) sharedSnapshot() []byte {
	mb.snapMu.Lock()
	defer mb.snapMu.Unlock()
	if mb.snapshot == nil {
		mb.snapshot = append([]byte(nil), mb.buf[:mb.size]...)
	}
	return mb.snapshot
}

type SealedBuffers struct {
	buffers []*MemBuffer
}

func newSealedBuffers(size int) *SealedBuffers {
	sbs := &SealedBuffers{}

	sbs.buffers = make([]*MemBuffer, size)
	for i := 0; i < size; i++ {
		sbs.buffers[i] = &MemBuffer{
			buf: make([]byte, BufferSize),
		}
	}

	return sbs
}

func (sbs *SealedBuffers) SealBuffer(startTime, stopTime time.Time, buf []byte, pos int, startOffset int64, endOffset int64) (newBuf []byte) {
	oldBuf := sbs.buffers[0].buf
	size := len(sbs.buffers)
	for i := 0; i < size-1; i++ {
		sbs.buffers[i].buf = sbs.buffers[i+1].buf
		sbs.buffers[i].size = sbs.buffers[i+1].size
		sbs.buffers[i].startTime = sbs.buffers[i+1].startTime
		sbs.buffers[i].stopTime = sbs.buffers[i+1].stopTime
		sbs.buffers[i].startOffset = sbs.buffers[i+1].startOffset
		sbs.buffers[i].offset = sbs.buffers[i+1].offset
		sbs.buffers[i].snapshot = sbs.buffers[i+1].snapshot // snapshot follows its window
	}
	sbs.buffers[size-1].buf = buf
	sbs.buffers[size-1].size = pos
	sbs.buffers[size-1].startTime = startTime
	sbs.buffers[size-1].stopTime = stopTime
	sbs.buffers[size-1].startOffset = startOffset
	sbs.buffers[size-1].offset = endOffset
	sbs.buffers[size-1].snapshot = nil
	return oldBuf
}

func (mb *MemBuffer) locateByTs(lastReadTime time.Time) (pos int, err error) {
	lastReadTs := lastReadTime.UnixNano()
	for pos < mb.size {
		size, t, readErr := readTs(mb.buf, pos)
		if readErr != nil {
			// Return error if buffer is corrupted
			return 0, fmt.Errorf("locateByTs: buffer corruption at pos %d: %w", pos, readErr)
		}
		if t > lastReadTs {
			return pos, nil
		}
		pos += size + 4
	}
	return mb.size, nil
}

func (mb *MemBuffer) String() string {
	return fmt.Sprintf("[%v,%v] bytes:%d", mb.startTime, mb.stopTime, mb.size)
}
