package mount

type WriterPattern struct {
	isStreaming     bool
	lastWriteOffset int64
	chunkSize       int64
}

// For streaming write: only cache the first chunk
// For random write: fall back to temp file approach
// writes can only change from streaming mode to non-streaming mode

func NewWriterPattern(chunkSize int64) *WriterPattern {
	return &WriterPattern{
		isStreaming:     true,
		lastWriteOffset: -1,
		chunkSize:       chunkSize,
	}
}

func (rp *WriterPattern) MonitorWriteAt(offset int64, size int) {
	if rp.lastWriteOffset > offset {
		rp.isStreaming = false
	}
	if rp.lastWriteOffset == -1 {
		if offset != 0 {
			rp.isStreaming = false
		}
	}
	rp.lastWriteOffset = offset
}

func (rp *WriterPattern) IsStreamingMode() bool {
	return rp.isStreaming
}

func (rp *WriterPattern) IsRandomMode() bool {
	return !rp.isStreaming
}

func (rp *WriterPattern) Reset() {
	rp.isStreaming = true
	rp.lastWriteOffset = -1
}
