package mount

import "sync/atomic"

type WriterPattern struct {
	isSequentialCounter int64
	writeFrontier       int64 // highest (offset+size) observed across writes
	chunkSize           int64
}

const ModeChangeLimit = 3

// SeqTolerance: a write whose start is within this many bytes of the current
// write frontier still counts as sequential. Using a tolerance window rather
// than strict contiguity absorbs reordered/concurrent FUSE writeback — multiple
// Write upcalls for one handle interleave (MonitorWriteAt runs before the
// per-handle write lock) and writeback flushes dirty pages slightly out of
// offset order — while still rejecting far random seeks.
const SeqTolerance = 8 << 20 // 8 MiB

// For streaming write: keep dirty chunks in memory (NewMemChunk).
// For random write: fall back to the on-disk swap file (NewSwapFileChunk).

func NewWriterPattern(chunkSize int64) *WriterPattern {
	return &WriterPattern{
		isSequentialCounter: 0,
		writeFrontier:       0,
		chunkSize:           chunkSize,
	}
}

func (rp *WriterPattern) MonitorWriteAt(offset int64, size int) {
	// Advance the frontier to max(frontier, offset+size) and capture, in the same
	// CAS loop, the pre-image this write is judged against. Reading the frontier
	// inside the loop (rather than once up front) keeps `diff` below comparing
	// against the freshest value even if a concurrent writeback upcall advances
	// the frontier while we loop. Lock-free, consistent with the atomic counter.
	end := offset + int64(size)
	var frontier int64
	for {
		frontier = atomic.LoadInt64(&rp.writeFrontier)
		if end <= frontier || atomic.CompareAndSwapInt64(&rp.writeFrontier, frontier, end) {
			break
		}
	}

	// near = this write starts within SeqTolerance of where writes had reached.
	// Hysteresis (the ±ModeChangeLimit counter) keeps a single outlier write
	// from flipping the mode and spilling a sequential stream to the swap file.
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

func (rp *WriterPattern) IsSequentialMode() bool {
	return atomic.LoadInt64(&rp.isSequentialCounter) >= 0
}
