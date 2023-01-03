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

func (list *ChunkWrittenIntervalList) IsComplete(chunkSize int64) bool {
	return list.size() == 1 && list.head.next.isComplete(chunkSize)
}
func (list *ChunkWrittenIntervalList) WrittenSize() (writtenByteCount int64) {
	for t := list.head; t != nil; t = t.next {
		writtenByteCount += t.Size()
	}
	return
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
