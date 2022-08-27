package mount

import "sync/atomic"

type WriterPattern struct {
	isSequentialCounter int64
	lastWriteStopOffset int64
	chunkSize           int64
}

const ModeChangeLimit = 3

// For streaming write: only cache the first chunk
// For random write: fall back to temp file approach

func NewWriterPattern(chunkSize int64) *WriterPattern {
	return &WriterPattern{
		isSequentialCounter: 0,
		lastWriteStopOffset: 0,
		chunkSize:           chunkSize,
	}
}

func (rp *WriterPattern) MonitorWriteAt(offset int64, size int) {
	lastOffset := atomic.SwapInt64(&rp.lastWriteStopOffset, offset+int64(size))
	counter := atomic.LoadInt64(&rp.isSequentialCounter)
	if lastOffset == offset {
		if counter < ModeChangeLimit {
			atomic.AddInt64(&rp.isSequentialCounter, 1)
		}
	} else {
		if counter > -ModeChangeLimit {
			atomic.AddInt64(&rp.isSequentialCounter, -1)
		}
	}
}

func (rp *WriterPattern) IsSequentialMode() bool {
	return atomic.LoadInt64(&rp.isSequentialCounter) >= 0
}
