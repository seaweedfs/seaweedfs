package mount

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
	if rp.lastWriteStopOffset == offset {
		if rp.isSequentialCounter < ModeChangeLimit {
			rp.isSequentialCounter++
		}
	} else {
		if rp.isSequentialCounter > -ModeChangeLimit {
			rp.isSequentialCounter--
		}
	}
	rp.lastWriteStopOffset = offset + int64(size)
}

func (rp *WriterPattern) IsSequentialMode() bool {
	return rp.isSequentialCounter >= 0
}
