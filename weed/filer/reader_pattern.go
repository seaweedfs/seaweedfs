package filer

import (
	"sync/atomic"
)

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
	lastOffset := atomic.SwapInt64(&rp.lastReadStopOffset, offset+int64(size))
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

func (rp *ReaderPattern) IsRandomMode() bool {
	return atomic.LoadInt64(&rp.isSequentialCounter) < 0
}
