package filer

type ReaderPattern struct {
	isSequentialCounter int64
	lastReadStopOffset  int64
}

const ModeChangeLimit = 3

// For streaming read: only cache the first chunk
// For random read: only fetch the requested range, instead of the whole chunk

func NewReaderPattern() *ReaderPattern {
	return &ReaderPattern{
		isSequentialCounter: 0,
		lastReadStopOffset:  0,
	}
}

func (rp *ReaderPattern) MonitorReadAt(offset int64, size int) {
	if rp.lastReadStopOffset == offset {
		if rp.isSequentialCounter < ModeChangeLimit {
			rp.isSequentialCounter++
		}
	} else {
		if rp.isSequentialCounter > -ModeChangeLimit {
			rp.isSequentialCounter--
		}
	}
	rp.lastReadStopOffset = offset + int64(size)
}

func (rp *ReaderPattern) IsRandomMode() bool {
	return rp.isSequentialCounter < 0
}
