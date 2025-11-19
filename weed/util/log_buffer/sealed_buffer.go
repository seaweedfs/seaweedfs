package log_buffer

import (
	"fmt"
	"time"
)

type MemBuffer struct {
	buf         []byte
	size        int
	startTime   time.Time
	stopTime    time.Time
	startOffset int64 // First offset in this buffer
	offset      int64 // Last offset in this buffer (endOffset)
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
	oldMemBuffer := sbs.buffers[0]
	size := len(sbs.buffers)
	for i := 0; i < size-1; i++ {
		sbs.buffers[i].buf = sbs.buffers[i+1].buf
		sbs.buffers[i].size = sbs.buffers[i+1].size
		sbs.buffers[i].startTime = sbs.buffers[i+1].startTime
		sbs.buffers[i].stopTime = sbs.buffers[i+1].stopTime
		sbs.buffers[i].startOffset = sbs.buffers[i+1].startOffset
		sbs.buffers[i].offset = sbs.buffers[i+1].offset
	}
	sbs.buffers[size-1].buf = buf
	sbs.buffers[size-1].size = pos
	sbs.buffers[size-1].startTime = startTime
	sbs.buffers[size-1].stopTime = stopTime
	sbs.buffers[size-1].startOffset = startOffset
	sbs.buffers[size-1].offset = endOffset
	return oldMemBuffer.buf
}

func (mb *MemBuffer) locateByTs(lastReadTime time.Time) (pos int, err error) {
	lastReadTs := lastReadTime.UnixNano()
	for pos < len(mb.buf) {
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
	return len(mb.buf), nil
}

func (mb *MemBuffer) String() string {
	return fmt.Sprintf("[%v,%v] bytes:%d", mb.startTime, mb.stopTime, mb.size)
}
