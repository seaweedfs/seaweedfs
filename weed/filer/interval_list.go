package filer

import (
	"math"
	"sync"
)

type IntervalValue interface {
	SetStartStop(start, stop int64)
	Clone() IntervalValue
}

type Interval[T IntervalValue] struct {
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
type IntervalList[T IntervalValue] struct {
	head *Interval[T]
	tail *Interval[T]
	Lock sync.RWMutex
}

func NewIntervalList[T IntervalValue]() *IntervalList[T] {
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
	return list
}

func (list *IntervalList[T]) Front() (interval *Interval[T]) {
	return list.head.Next
}

func (list *IntervalList[T]) AppendInterval(interval *Interval[T]) {
	list.Lock.Lock()
	defer list.Lock.Unlock()

	if list.head.Next == nil {
		list.head.Next = interval
	}
	interval.Prev = list.tail.Prev
	if list.tail.Prev != nil {
		list.tail.Prev.Next = interval
	}
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

	list.Lock.Lock()
	defer list.Lock.Unlock()

	list.overlayInterval(interval)
}

func (list *IntervalList[T]) InsertInterval(startOffset, stopOffset, tsNs int64, value T) {
	interval := &Interval[T]{
		StartOffset: startOffset,
		StopOffset:  stopOffset,
		TsNs:        tsNs,
		Value:       value,
	}

	list.Lock.Lock()
	defer list.Lock.Unlock()

	value.SetStartStop(startOffset, stopOffset)
	list.insertInterval(interval)
}

func (list *IntervalList[T]) insertInterval(interval *Interval[T]) {
	prev := list.head
	next := prev.Next

	for interval.StartOffset < interval.StopOffset {
		if next == nil {
			// add to the end
			list.insertBetween(prev, interval, list.tail)
			break
		}

		// interval is ahead of the next
		if interval.StopOffset <= next.StartOffset {
			list.insertBetween(prev, interval, next)
			break
		}

		// interval is after the next
		if next.StopOffset <= interval.StartOffset {
			prev = next
			next = next.Next
			continue
		}

		// intersecting next and interval
		if interval.TsNs >= next.TsNs {
			// interval is newer
			if next.StartOffset < interval.StartOffset {
				// left side of next is ahead of interval
				t := &Interval[T]{
					StartOffset: next.StartOffset,
					StopOffset:  interval.StartOffset,
					TsNs:        next.TsNs,
					Value:       next.Value.Clone().(T),
				}
				t.Value.SetStartStop(t.StartOffset, t.StopOffset)
				list.insertBetween(prev, t, interval)
				next.StartOffset = interval.StartOffset
				next.Value.SetStartStop(next.StartOffset, next.StopOffset)
				prev = t
			}
			if interval.StopOffset < next.StopOffset {
				// right side of next is after interval
				next.StartOffset = interval.StopOffset
				next.Value.SetStartStop(next.StartOffset, next.StopOffset)
				list.insertBetween(prev, interval, next)
				break
			} else {
				// next is covered
				prev.Next = interval
				next = next.Next
			}
		} else {
			// next is newer
			if interval.StartOffset < next.StartOffset {
				// left side of interval is ahead of next
				t := &Interval[T]{
					StartOffset: interval.StartOffset,
					StopOffset:  next.StartOffset,
					TsNs:        interval.TsNs,
					Value:       interval.Value.Clone().(T),
				}
				t.Value.SetStartStop(t.StartOffset, t.StopOffset)
				list.insertBetween(prev, t, next)
				interval.StartOffset = next.StartOffset
				interval.Value.SetStartStop(interval.StartOffset, interval.StopOffset)
			}
			if next.StopOffset < interval.StopOffset {
				// right side of interval is after next
				interval.StartOffset = next.StopOffset
				interval.Value.SetStartStop(interval.StartOffset, interval.StopOffset)
			} else {
				// interval is covered
				break
			}
		}

	}
}

func (list *IntervalList[T]) insertBetween(a, interval, b *Interval[T]) {
	a.Next = interval
	b.Prev = interval
	if a != list.head {
		interval.Prev = a
	}
	if b != list.tail {
		interval.Next = b
	}
}

func (list *IntervalList[T]) overlayInterval(interval *Interval[T]) {

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
		if p != list.head {
			t.Prev = p
		}
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
		if q != list.tail {
			t.Next = q
		}
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
	list.Lock.RLock()
	defer list.Lock.RUnlock()

	var count int
	for t := list.head; t != nil; t = t.Next {
		count++
	}
	return count - 1
}
