package log_buffer

import "time"

type MemBuffer struct {
	buf       []byte
	startTime time.Time
	stopTime  time.Time
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

func (sbs *SealedBuffers) SealBuffer(startTime, stopTime time.Time, buf []byte) (newBuf []byte) {
	oldMemBuffer := sbs.buffers[0]
	size := len(sbs.buffers)
	for i := 0; i < size-1; i++ {
		sbs.buffers[i].buf = sbs.buffers[i+1].buf
		sbs.buffers[i].startTime = sbs.buffers[i+1].startTime
		sbs.buffers[i].stopTime = sbs.buffers[i+1].stopTime
	}
	sbs.buffers[size-1].buf = buf
	sbs.buffers[size-1].startTime = startTime
	sbs.buffers[size-1].stopTime = stopTime
	return oldMemBuffer.buf
}
