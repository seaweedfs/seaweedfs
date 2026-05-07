package router

import (
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
