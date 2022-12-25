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

	p := list.head
	for ; p.next != nil && p.next.stopOffset <= interval.StartOffset; p = p.next {
	}
	q := list.tail
	for ; q.prev != nil && q.prev.StartOffset >= interval.stopOffset; q = q.prev {
	}

	// left side
	// interval after p.next start
	if p.next.StartOffset < interval.StartOffset {
		p.next.stopOffset = interval.StartOffset
		p.next.next = interval
		interval.prev = p.next
	} else {
		p.next = interval
		interval.prev = p
	}

	// right side
	// interval ends before p.prev
	if q.prev.stopOffset > interval.stopOffset {
		q.prev.StartOffset = interval.stopOffset
		q.prev.prev = interval
		interval.next = q.prev
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
