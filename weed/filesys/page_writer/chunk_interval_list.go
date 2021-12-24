package page_writer

import "math"

// ChunkWrittenInterval mark one written interval within one page chunk
type ChunkWrittenInterval struct {
	startOffset int64
	stopOffset  int64
	prev        *ChunkWrittenInterval
	next        *ChunkWrittenInterval
}

func (interval *ChunkWrittenInterval) Size() int64 {
	return interval.stopOffset - interval.startOffset
}

// ChunkWrittenIntervalList mark written intervals within one page chunk
type ChunkWrittenIntervalList struct {
	head *ChunkWrittenInterval
	tail *ChunkWrittenInterval
}

func newChunkWrittenIntervalList() *ChunkWrittenIntervalList {
	list := &ChunkWrittenIntervalList{
		head: &ChunkWrittenInterval{
			startOffset: -1,
			stopOffset:  -1,
		},
		tail: &ChunkWrittenInterval{
			startOffset: math.MaxInt64,
			stopOffset:  math.MaxInt64,
		},
	}
	list.head.next = list.tail
	list.tail.prev = list.head
	return list
}

func (list *ChunkWrittenIntervalList) MarkWritten(startOffset, stopOffset int64) {
	interval := &ChunkWrittenInterval{
		startOffset: startOffset,
		stopOffset:  stopOffset,
	}
	list.addInterval(interval)
}
func (list *ChunkWrittenIntervalList) addInterval(interval *ChunkWrittenInterval) {

	p := list.head
	for ; p.next != nil && p.next.startOffset <= interval.startOffset; p = p.next {
	}
	q := list.tail
	for ; q.prev != nil && q.prev.stopOffset >= interval.stopOffset; q = q.prev {
	}

	if interval.startOffset <= p.stopOffset && q.startOffset <= interval.stopOffset {
		// merge p and q together
		p.stopOffset = q.stopOffset
		unlinkNodesBetween(p, q.next)
		return
	}
	if interval.startOffset <= p.stopOffset {
		// merge new interval into p
		p.stopOffset = interval.stopOffset
		unlinkNodesBetween(p, q)
		return
	}
	if q.startOffset <= interval.stopOffset {
		// merge new interval into q
		q.startOffset = interval.startOffset
		unlinkNodesBetween(p, q)
		return
	}

	// add the new interval between p and q
	unlinkNodesBetween(p, q)
	p.next = interval
	interval.prev = p
	q.prev = interval
	interval.next = q

}

// unlinkNodesBetween remove all nodes after start and before stop, exclusive
func unlinkNodesBetween(start *ChunkWrittenInterval, stop *ChunkWrittenInterval) {
	if start.next == stop {
		return
	}
	start.next.prev = nil
	start.next = stop
	stop.prev.next = nil
	stop.prev = start
}

func (list *ChunkWrittenIntervalList) size() int {
	var count int
	for t := list.head; t != nil; t = t.next {
		count++
	}
	return count - 2
}
