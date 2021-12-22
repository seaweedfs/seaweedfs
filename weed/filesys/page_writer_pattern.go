package filesys

import "fmt"

type WriterPattern struct {
	isStreaming     bool
	lastWriteOffset int64
	chunkSize       int64
	fileName        string
}

// For streaming write: only cache the first chunk
// For random write: fall back to temp file approach
// writes can only change from streaming mode to non-streaming mode

func NewWriterPattern(fileName string, chunkSize int64) *WriterPattern {
	return &WriterPattern{
		isStreaming:     true,
		lastWriteOffset: 0,
		chunkSize:       chunkSize,
		fileName:        fileName,
	}
}

func (rp *WriterPattern) MonitorWriteAt(offset int64, size int) {
	if rp.lastWriteOffset == 0 {
	}
	if rp.lastWriteOffset > offset {
		if rp.isStreaming {
			fmt.Printf("file %s ==> non streaming at [%d,%d)\n", rp.fileName, offset, offset+int64(size))
		}
		fmt.Printf("write %s [%d,%d)\n", rp.fileName, offset, offset+int64(size))
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
