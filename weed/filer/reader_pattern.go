package filer

type ReaderPattern struct {
	isStreaming    bool
	lastReadOffset int64
}

// For streaming read: only cache the first chunk
// For random read: only fetch the requested range, instead of the whole chunk

func NewReaderPattern() *ReaderPattern {
	return &ReaderPattern{
		isStreaming:    true,
		lastReadOffset: 0,
	}
}

func (rp *ReaderPattern) MonitorReadAt(offset int64, size int) {
	if rp.lastReadOffset > offset {
		rp.isStreaming = false
	}
	rp.lastReadOffset = offset
}

func (rp *ReaderPattern) IsStreamingMode() bool {
	return rp.isStreaming
}

func (rp *ReaderPattern) IsRandomMode() bool {
	return !rp.isStreaming
}
