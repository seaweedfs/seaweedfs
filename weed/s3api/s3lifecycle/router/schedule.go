package router

import (
	"container/heap"
	"sync"
	"time"
)

// Schedule is the worker's in-memory pending list, ordered by DueTime.
// The dispatcher polls Drain on each tick to retrieve due Matches.
//
// Duplicates are allowed: a re-schedule for the same (ActionKey, ObjectKey)
// before the prior dispatch ran results in two heap entries. The
// LifecycleDelete RPC's identity-CAS makes the second dispatch a no-op
// (NOOP_RESOLVED with STALE_IDENTITY), so dedup at insert time would only
// be a micro-optimization at the cost of an extra map.
type Schedule struct {
	mu sync.Mutex
	h  scheduleHeap
}

func NewSchedule() *Schedule {
	return &Schedule{}
}

// Add enqueues a Match.
func (s *Schedule) Add(m Match) {
	s.mu.Lock()
	defer s.mu.Unlock()
	heap.Push(&s.h, m)
}

// Len returns the number of pending Matches.
func (s *Schedule) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.h.Len()
}

// NextDue returns the DueTime of the earliest pending Match. ok=false if
// the schedule is empty.
func (s *Schedule) NextDue() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.h.Len() == 0 {
		return time.Time{}, false
	}
	return s.h[0].DueTime, true
}

// Drain pops and returns all Matches whose DueTime <= now, in DueTime order.
// Subsequent calls return only newly-due Matches.
func (s *Schedule) Drain(now time.Time) []Match {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Match
	for s.h.Len() > 0 && !s.h[0].DueTime.After(now) {
		out = append(out, heap.Pop(&s.h).(Match))
	}
	return out
}

// scheduleHeap implements heap.Interface ordered by Match.DueTime ascending.
type scheduleHeap []Match

func (h scheduleHeap) Len() int            { return len(h) }
func (h scheduleHeap) Less(i, j int) bool  { return h[i].DueTime.Before(h[j].DueTime) }
func (h scheduleHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *scheduleHeap) Push(x interface{}) { *h = append(*h, x.(Match)) }
func (h *scheduleHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
