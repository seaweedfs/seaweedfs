package page_writer

type WriterPattern struct {
	isStreaming     bool
	lastWriteOffset int64
}

// For streaming write: only cache the first chunk
// For random write: fall back to temp file approach

func NewWriterPattern() *WriterPattern {
	return &WriterPattern{
		isStreaming:     true,
		lastWriteOffset: 0,
	}
}

func (rp *WriterPattern) MonitorWriteAt(offset int64, size int) {
	if rp.lastWriteOffset > offset {
		rp.isStreaming = false
	}
	rp.lastWriteOffset = offset
}

func (rp *WriterPattern) IsStreamingMode() bool {
	return rp.isStreaming
}

func (rp *WriterPattern) IsRandomMode() bool {
	return !rp.isStreaming
}
