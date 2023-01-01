package filer

import "math"

type Interval[T any] struct {
	StartOffset int64
	StopOffset  int64
	TsNs        int64
	Value       T
	Prev        *Interval[T]
	Next        *Interval[T]
}

func (interval *Interval[T]) Size() int64 {
	return interval.StopOffset - interval.StartOffset
}

// IntervalList mark written intervals within one page chunk
type IntervalList[T any] struct {
	head *Interval[T]
	tail *Interval[T]
}

func NewIntervalList[T any]() *IntervalList[T] {
	list := &IntervalList[T]{
		head: &Interval[T]{
			StartOffset: -1,
			StopOffset:  -1,
		},
		tail: &Interval[T]{
			StartOffset: math.MaxInt64,
			StopOffset:  math.MaxInt64,
		},
	}
	list.head.Next = list.tail
	list.tail.Prev = list.head
	return list
}

func (list *IntervalList[T]) Front() (interval *Interval[T]) {
	return list.head.Next
}

func (list *IntervalList[T]) AppendInterval(interval *Interval[T]) {
	interval.Prev = list.tail.Prev
	list.tail.Prev.Next = interval
	list.tail.Prev = interval
}

func (list *IntervalList[T]) Overlay(startOffset, stopOffset, tsNs int64, value T) {
	if startOffset >= stopOffset {
		return
	}
	interval := &Interval[T]{
		StartOffset: startOffset,
		StopOffset:  stopOffset,
		TsNs:        tsNs,
		Value:       value,
	}
	list.addInterval(interval)
}

func (list *IntervalList[T]) addInterval(interval *Interval[T]) {

	//t := list.head
	//for ; t.Next != nil; t = t.Next {
	//	if t.TsNs > interval.TsNs {
	//		println("writes is out of order", t.TsNs-interval.TsNs, "ns")
	//	}
	//}

	p := list.head
	for ; p.Next != nil && p.Next.StopOffset <= interval.StartOffset; p = p.Next {
	}
	q := list.tail
	for ; q.Prev != nil && q.Prev.StartOffset >= interval.StopOffset; q = q.Prev {
	}

	// left side
	// interval after p.Next start
	if p.Next != nil && p.Next.StartOffset < interval.StartOffset {
		t := &Interval[T]{
			StartOffset: p.Next.StartOffset,
			StopOffset:  interval.StartOffset,
			TsNs:        p.Next.TsNs,
			Value:       p.Next.Value,
		}
		p.Next = t
		t.Prev = p
		t.Next = interval
		interval.Prev = t
	} else {
		p.Next = interval
		if p != list.head {
			interval.Prev = p
		}
	}

	// right side
	// interval ends before p.Prev
	if q.Prev != nil && interval.StopOffset < q.Prev.StopOffset {
		t := &Interval[T]{
			StartOffset: interval.StopOffset,
			StopOffset:  q.Prev.StopOffset,
			TsNs:        q.Prev.TsNs,
			Value:       q.Prev.Value,
		}
		q.Prev = t
		t.Next = q
		interval.Next = t
		t.Prev = interval
	} else {
		q.Prev = interval
		if q != list.tail {
			interval.Next = q
		}
	}

}

func (list *IntervalList[T]) Len() int {
	var count int
	for t := list.head; t != nil; t = t.Next {
		count++
	}
	return count - 1
}
