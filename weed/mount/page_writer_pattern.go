package mount

type WriterPattern struct {
	isSequentialCounter int64
	lastWriteStopOffset int64
	chunkSize           int64
}

// For streaming write: only cache the first chunk
// For random write: fall back to temp file approach
// writes can only change from streaming mode to non-streaming mode

func NewWriterPattern(chunkSize int64) *WriterPattern {
	return &WriterPattern{
		isSequentialCounter: 0,
		lastWriteStopOffset: 0,
		chunkSize:           chunkSize,
	}
}

func (rp *WriterPattern) MonitorWriteAt(offset int64, size int) {
	if rp.lastWriteStopOffset == offset {
		rp.isSequentialCounter++
	} else {
		rp.isSequentialCounter--
	}
	rp.lastWriteStopOffset = offset + int64(size)
}

func (rp *WriterPattern) IsSequentialMode() bool {
	return rp.isSequentialCounter >= 0
}
