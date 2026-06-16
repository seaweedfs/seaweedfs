package filer

import (
	"sync/atomic"
)

type ReaderPattern struct {
	isSequentialCounter int64
	readFrontier        int64 // highest (offset+size) observed across reads
}

const ModeChangeLimit = 3

// SeqTolerance: a read whose start is within this many bytes of the current read
// frontier still counts as sequential. Using a tolerance window rather than
// strict contiguity absorbs reordered/concurrent readahead (multiple ReadAt can
// be in flight at once) while still rejecting far random jumps.
const SeqTolerance = 8 << 20 // 8 MiB

// For streaming read: only cache the first chunk
// For random read: only fetch the requested range, instead of the whole chunk

func NewReaderPattern() *ReaderPattern {
	return &ReaderPattern{
		isSequentialCounter: 0,
		readFrontier:        0,
	}
}

func (rp *ReaderPattern) MonitorReadAt(offset int64, size int) {
	// Snapshot the frontier this read is judged against, then advance it to
	// max(frontier, offset+size). The CAS loop keeps this lock-free under
	// concurrent reads, consistent with the rest of this type.
	frontier := atomic.LoadInt64(&rp.readFrontier)
	end := offset + int64(size)
	for {
		cur := atomic.LoadInt64(&rp.readFrontier)
		if end <= cur || atomic.CompareAndSwapInt64(&rp.readFrontier, cur, end) {
			break
		}
	}

	// near = this read starts within SeqTolerance of where reads had reached.
	// Hysteresis (the ±ModeChangeLimit counter) keeps a single outlier read from
	// flipping the mode.
	diff := offset - frontier
	if diff < 0 {
		diff = -diff
	}
	counter := atomic.LoadInt64(&rp.isSequentialCounter)
	if diff <= SeqTolerance {
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
