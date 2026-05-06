package page_writer

import (
	"math"
)

// ChunkWrittenInterval mark one written interval within one page chunk
type ChunkWrittenInterval struct {
	StartOffset int64
	stopOffset  int64
	TsNs        int64
	prev        *ChunkWrittenInterval
	next        *ChunkWrittenInterval
}

func (interval *ChunkWrittenInterval) Size() int64 {
	return interval.stopOffset - interval.StartOffset
}

func (interval *ChunkWrittenInterval) isComplete(chunkSize int64) bool {
	return interval.stopOffset-interval.StartOffset == chunkSize
}

// ChunkWrittenIntervalList mark written intervals within one page chunk
type ChunkWrittenIntervalList struct {
	head *ChunkWrittenInterval
	tail *ChunkWrittenInterval
}

func newChunkWrittenIntervalList() *ChunkWrittenIntervalList {
	list := &ChunkWrittenIntervalList{
		head: &ChunkWrittenInterval{
			StartOffset: -1,
			stopOffset:  -1,
		},
		tail: &ChunkWrittenInterval{
			StartOffset: math.MaxInt64,
			stopOffset:  math.MaxInt64,
		},
	}
	list.head.next = list.tail
	list.tail.prev = list.head
	return list
}

func (list *ChunkWrittenIntervalList) MarkWritten(startOffset, stopOffset, tsNs int64) {
	if startOffset >= stopOffset {
		return
	}
	interval := &ChunkWrittenInterval{
		StartOffset: startOffset,
		stopOffset:  stopOffset,
		TsNs:        tsNs,
	}
	list.addInterval(interval)
}

// IsComplete reports whether every byte of [0, chunkSize) has been
// written, possibly via multiple adjacent or overlapping intervals.
// addInterval does not merge adjacent intervals — a chunk filled by
// two 1 MiB writes ends up as {[0,1M], [1M,2M]}, not one [0,2M] —
// so checking list.size()==1 misses the "filled by adjacent writes"
// case, leaving the chunk pinned in writableChunks even though all
// its bytes are present. That latent bug became a hard deadlock once
// -writeBufferSizeMB started reserving a global slot per writable
// chunk: the chunks never got sealed, no uploader ran, no slot was
// ever released, and the FUSE writer blocked in Reserve forever
// (seaweedfs issue #8777 / PR #9066). Walking the list and tracking
// the furthest covered offset detects adjacency correctly.
func (list *ChunkWrittenIntervalList) IsComplete(chunkSize int64) bool {
	var covered int64
	for t := list.head.next; t != list.tail; t = t.next {
		if t.StartOffset > covered {
			return false // gap before this interval
		}
		if t.stopOffset > covered {
			covered = t.stopOffset
		}
	}
	return covered >= chunkSize
}
func (list *ChunkWrittenIntervalList) WrittenSize() (writtenByteCount int64) {
	for t := list.head; t != nil; t = t.next {
		writtenByteCount += t.Size()
	}
	return
}

// IsContiguouslyWritten reports whether the chunk's written bytes form
// one unbroken run starting at offset 0. Returns false for an empty list
// (nothing to seal yet) and for any list whose first interval does not
// start at 0 or whose intervals have an internal gap.
//
// SaveContent emits one volume chunk per maximal adjacent run, so a list
// with a gap (leading or internal) produces multiple volume chunks with
// no chunk covering the gap; reads then silently zero-fill the gap range
// (filer/stream.go:177-186). This is correct for genuinely sparse writes
// finalized at flush, but incorrect when an in-progress chunk is sealed
// by buffer-pressure eviction while FUSE writeback still has writes in
// flight for the gap range. The eviction policy checks this so that
// only gap-free chunks are sealed under pressure (issue #9330).
//
// The "starts at 0" check applies the same logic to a leading hole that
// the adjacency check applies to an internal hole — pressure-driven
// sealing should not race FUSE writeback on either, and at flush both
// are sealed unconditionally via FlushAll for sparse-file semantics.
func (list *ChunkWrittenIntervalList) IsContiguouslyWritten() bool {
	first := list.head.next
	if first == list.tail {
		return false // empty: nothing to seal
	}
	if first.StartOffset != 0 {
		return false // leading gap before offset 0
	}
	for t := first; t.next != list.tail; t = t.next {
		if t.stopOffset != t.next.StartOffset {
			return false // internal gap
		}
	}
	return true
}

func (list *ChunkWrittenIntervalList) addInterval(interval *ChunkWrittenInterval) {

	//t := list.head
	//for ; t.next != nil; t = t.next {
	//	if t.TsNs > interval.TsNs {
	//		println("writes is out of order", t.TsNs-interval.TsNs, "ns")
	//	}
	//}

	p := list.head
	for ; p.next != nil && p.next.stopOffset <= interval.StartOffset; p = p.next {
	}
	q := list.tail
	for ; q.prev != nil && q.prev.StartOffset >= interval.stopOffset; q = q.prev {
	}

	// left side
	// interval after p.next start
	if p.next.StartOffset < interval.StartOffset {
		t := &ChunkWrittenInterval{
			StartOffset: p.next.StartOffset,
			stopOffset:  interval.StartOffset,
			TsNs:        p.next.TsNs,
		}
		p.next = t
		t.prev = p
		t.next = interval
		interval.prev = t
	} else {
		p.next = interval
		interval.prev = p
	}

	// right side
	// interval ends before p.prev
	if interval.stopOffset < q.prev.stopOffset {
		t := &ChunkWrittenInterval{
			StartOffset: interval.stopOffset,
			stopOffset:  q.prev.stopOffset,
			TsNs:        q.prev.TsNs,
		}
		q.prev = t
		t.next = q
		interval.next = t
		t.prev = interval
	} else {
		q.prev = interval
		interval.next = q
	}

}

func (list *ChunkWrittenIntervalList) size() int {
	var count int
	for t := list.head; t != nil; t = t.next {
		count++
	}
	return count - 2
}
