package router

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

func mkMatch(due time.Time, key string) Match {
	return Match{
		Key:       s3lifecycle.ActionKey{Bucket: key},
		DueTime:   due,
		ObjectKey: key,
	}
}

func TestScheduleEmpty(t *testing.T) {
	s := NewSchedule()
	if s.Len() != 0 {
		t.Fatal("Len != 0")
	}
	if _, ok := s.NextDue(); ok {
		t.Fatal("NextDue ok=true on empty")
	}
	if got := s.Drain(time.Now()); got != nil {
		t.Fatalf("Drain on empty returned %v", got)
	}
}

func TestScheduleOrderedByDueTime(t *testing.T) {
	s := NewSchedule()
	t0 := time.Now()
	s.Add(mkMatch(t0.Add(3*time.Second), "c"))
	s.Add(mkMatch(t0.Add(1*time.Second), "a"))
	s.Add(mkMatch(t0.Add(2*time.Second), "b"))

	if s.Len() != 3 {
		t.Fatalf("Len=%d, want 3", s.Len())
	}
	due, ok := s.NextDue()
	if !ok || !due.Equal(t0.Add(1*time.Second)) {
		t.Fatalf("NextDue=%v ok=%v", due, ok)
	}

	got := s.Drain(t0.Add(2 * time.Second))
	if len(got) != 2 || got[0].ObjectKey != "a" || got[1].ObjectKey != "b" {
		t.Fatalf("Drain order: %+v", got)
	}
	if s.Len() != 1 {
		t.Fatalf("Len after drain=%d, want 1", s.Len())
	}

	got = s.Drain(t0.Add(5 * time.Second))
	if len(got) != 1 || got[0].ObjectKey != "c" {
		t.Fatalf("Drain rest: %+v", got)
	}
	if s.Len() != 0 {
		t.Fatal("Len != 0 after final drain")
	}
}

func TestScheduleDrainBoundaryInclusive(t *testing.T) {
	// DueTime exactly equal to now is drainable (<=).
	s := NewSchedule()
	t0 := time.Now()
	s.Add(mkMatch(t0, "a"))
	got := s.Drain(t0)
	if len(got) != 1 {
		t.Fatalf("expected boundary-inclusive drain, got %d", len(got))
	}
}

func TestScheduleAllowsDuplicates(t *testing.T) {
	s := NewSchedule()
	t0 := time.Now()
	s.Add(mkMatch(t0, "a"))
	s.Add(mkMatch(t0, "a"))
	if s.Len() != 2 {
		t.Fatalf("dup count=%d, want 2", s.Len())
	}
	got := s.Drain(t0)
	if len(got) != 2 {
		t.Fatalf("Drain dup count=%d, want 2", len(got))
	}
}

func TestScheduleDrainBeforeAnyDueReturnsNothing(t *testing.T) {
	// Drain at a time before the earliest DueTime must return an empty
	// slice and leave the heap intact. Otherwise the dispatcher would
	// consume future-due matches early.
	s := NewSchedule()
	t0 := time.Now()
	s.Add(mkMatch(t0.Add(5*time.Second), "a"))
	s.Add(mkMatch(t0.Add(10*time.Second), "b"))

	got := s.Drain(t0)
	if got != nil {
		t.Fatalf("Drain before any due should be nil, got %+v", got)
	}
	if s.Len() != 2 {
		t.Fatalf("Len after no-op Drain=%d, want 2", s.Len())
	}
}

func TestScheduleNextDueAfterPartialDrain(t *testing.T) {
	// After draining a prefix, NextDue must point at the earliest
	// remaining Match — regression catch for a Drain implementation
	// that forgets to maintain the heap invariant.
	s := NewSchedule()
	t0 := time.Now()
	s.Add(mkMatch(t0.Add(1*time.Second), "a"))
	s.Add(mkMatch(t0.Add(2*time.Second), "b"))
	s.Add(mkMatch(t0.Add(3*time.Second), "c"))

	got := s.Drain(t0.Add(1500 * time.Millisecond))
	if len(got) != 1 || got[0].ObjectKey != "a" {
		t.Fatalf("Drain prefix=%+v, want [a]", got)
	}
	due, ok := s.NextDue()
	if !ok || !due.Equal(t0.Add(2*time.Second)) {
		t.Fatalf("NextDue after partial Drain=%v ok=%v, want t+2s", due, ok)
	}
}

func TestScheduleAddAfterDrainKeepsOrder(t *testing.T) {
	// Adding to a non-empty schedule mid-stream — a fresh Match with an
	// earlier DueTime than the existing minimum must become the next
	// drainable. Pins that heap.Push correctly bubbles up.
	s := NewSchedule()
	t0 := time.Now()
	s.Add(mkMatch(t0.Add(10*time.Second), "old"))
	s.Add(mkMatch(t0.Add(1*time.Second), "new")) // pushed after but earlier

	due, ok := s.NextDue()
	if !ok || !due.Equal(t0.Add(1*time.Second)) {
		t.Fatalf("NextDue=%v ok=%v, want t+1s", due, ok)
	}

	got := s.Drain(t0.Add(2 * time.Second))
	if len(got) != 1 || got[0].ObjectKey != "new" {
		t.Fatalf("Drain=%+v, want [new]", got)
	}
}

func TestScheduleDrainOrderIsAscendingDueTime(t *testing.T) {
	// Drain must return Matches in DueTime order regardless of insert
	// order — explicit pinning of the contract documented on Drain.
	s := NewSchedule()
	t0 := time.Now()
	for _, off := range []time.Duration{
		5 * time.Second,
		1 * time.Second,
		3 * time.Second,
		2 * time.Second,
		4 * time.Second,
	} {
		s.Add(mkMatch(t0.Add(off), "k"))
	}

	got := s.Drain(t0.Add(10 * time.Second))
	if len(got) != 5 {
		t.Fatalf("Drain count=%d, want 5", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].DueTime.Before(got[i-1].DueTime) {
			t.Fatalf("Drain[%d]=%v is before Drain[%d]=%v", i, got[i].DueTime, i-1, got[i-1].DueTime)
		}
	}
}

func TestScheduleConcurrentAddDrainNoRace(t *testing.T) {
	// The dispatcher's Add and Drain run on separate goroutines; the
	// schedule's mutex must serialize them without deadlock. -race
	// catches a regression that drops the lock on either path.
	s := NewSchedule()
	t0 := time.Now()
	const N = 64
	var wg sync.WaitGroup
	wg.Add(N * 2)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			s.Add(mkMatch(t0.Add(time.Duration(i)*time.Millisecond), "k"))
		}()
		go func() {
			defer wg.Done()
			_ = s.Drain(t0.Add(10 * time.Second))
		}()
	}
	wg.Wait()
}
