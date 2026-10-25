package posixlock

import (
	"math"
	"testing"
)

// acquire is a test helper asserting the lock is granted.
func mustAcquire(t *testing.T, s *Set, lk Range) {
	t.Helper()
	if _, granted := s.Acquire(lk); !granted {
		t.Fatalf("expected lock granted: %+v", lk)
	}
}

func TestNonOverlappingLocksFromDifferentOwners(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 49, Type: Write, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 50, End: 99, Type: Write, Owner: 2, Pid: 20})
}

func TestOverlappingReadLocksFromDifferentOwners(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Read, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 50, End: 149, Type: Read, Owner: 2, Pid: 20})
}

func TestOverlappingWriteReadConflict(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10})
	if _, granted := s.Acquire(Range{Start: 50, End: 149, Type: Read, Owner: 2, Pid: 20}); granted {
		t.Fatal("expected conflict")
	}
}

func TestOverlappingWriteWriteConflict(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10})
	if _, granted := s.Acquire(Range{Start: 50, End: 149, Type: Write, Owner: 2, Pid: 20}); granted {
		t.Fatal("expected conflict")
	}
}

func TestSameOwnerUpgradeReadToWrite(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Read, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10})

	c, found := s.Conflict(Range{Start: 0, End: 99, Type: Write, Owner: 2, Pid: 20})
	if !found || c.Type != Write {
		t.Fatalf("expected conflicting write lock after upgrade, got %+v found=%v", c, found)
	}
}

func TestSameOwnerDowngradeWriteToRead(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Read, Owner: 1, Pid: 10})
	// Another owner can now take a shared read lock.
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Read, Owner: 2, Pid: 20})
}

func TestLockCoalescing(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 9, Type: Write, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 10, End: 19, Type: Write, Owner: 1, Pid: 10})

	if len(s.locks) != 1 {
		t.Fatalf("expected 1 coalesced lock, got %d: %+v", len(s.locks), s.locks)
	}
	if s.locks[0].Start != 0 || s.locks[0].End != 19 {
		t.Errorf("expected coalesced [0,19], got [%d,%d]", s.locks[0].Start, s.locks[0].End)
	}
}

func TestLockSplitting(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10})
	s.Release(Range{Start: 40, End: 59, Type: Unlock, Owner: 1, Pid: 10})

	if len(s.locks) != 2 {
		t.Fatalf("expected 2 locks after split, got %d: %+v", len(s.locks), s.locks)
	}
	if s.locks[0].Start != 0 || s.locks[0].End != 39 {
		t.Errorf("expected left [0,39], got [%d,%d]", s.locks[0].Start, s.locks[0].End)
	}
	if s.locks[1].Start != 60 || s.locks[1].End != 99 {
		t.Errorf("expected right [60,99], got [%d,%d]", s.locks[1].Start, s.locks[1].End)
	}
}

func TestConflictReportsHolder(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 10, End: 50, Type: Write, Owner: 1, Pid: 10})

	c, found := s.Conflict(Range{Start: 30, End: 70, Type: Read, Owner: 2, Pid: 20})
	if !found {
		t.Fatal("expected a conflict")
	}
	if c.Type != Write || c.Pid != 10 || c.Start != 10 || c.End != 50 {
		t.Fatalf("unexpected conflict report: %+v", c)
	}
}

func TestConflictNoneForSharedReads(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 10, End: 50, Type: Read, Owner: 1, Pid: 10})
	if _, found := s.Conflict(Range{Start: 30, End: 70, Type: Read, Owner: 2, Pid: 20}); found {
		t.Fatal("two read locks should not conflict")
	}
}

func TestConflictSameOwnerNone(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10})
	if _, found := s.Conflict(Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10}); found {
		t.Fatal("an owner should not conflict with itself")
	}
}

func TestReleaseOwner(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 49, Type: Write, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 50, End: 99, Type: Write, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 200, End: 299, Type: Read, Owner: 2, Pid: 20})

	s.ReleaseOwner(0, 1)

	if _, found := s.Conflict(Range{Start: 0, End: 99, Type: Write, Owner: 3, Pid: 30}); found {
		t.Fatal("owner 1's locks should be gone")
	}
	c, found := s.Conflict(Range{Start: 200, End: 299, Type: Write, Owner: 3, Pid: 30})
	if !found || c.Type != Read {
		t.Fatalf("owner 2's read lock should remain, got %+v found=%v", c, found)
	}
}

func TestFlockAndFcntlDoNotConflict(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 0, End: math.MaxUint64, Type: Write, Owner: 2, Pid: 20, IsFlock: true})
}

func TestReleasePosixOwnerKeepsFlock(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: math.MaxUint64, Type: Write, Owner: 1, Pid: 10, IsFlock: true})
	s.ReleasePosixOwner(0, 1)
	if _, found := s.Conflict(Range{Start: 0, End: math.MaxUint64, Type: Write, Owner: 2, Pid: 20, IsFlock: true}); !found {
		t.Fatal("flock lock should remain after ReleasePosixOwner")
	}
}

func TestReleaseFlockOwnerKeepsPosix(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 0, End: math.MaxUint64, Type: Write, Owner: 2, Pid: 10, IsFlock: true})

	s.ReleaseFlockOwner(0, 2)

	if _, found := s.Conflict(Range{Start: 0, End: 99, Type: Write, Owner: 3, Pid: 30}); !found {
		t.Fatal("fcntl lock should remain after ReleaseFlockOwner")
	}
	if _, found := s.Conflict(Range{Start: 0, End: math.MaxUint64, Type: Write, Owner: 4, Pid: 40, IsFlock: true}); found {
		t.Fatal("flock lock should be gone after ReleaseFlockOwner")
	}
}

func TestHasPosixIgnoresMissingOwnerAndFlock(t *testing.T) {
	s := &Set{}
	if s.HasPosix(0, 1) {
		t.Fatal("empty set should report no posix owner")
	}
	mustAcquire(t, s, Range{Start: 0, End: math.MaxUint64, Type: Write, Owner: 1, Pid: 10, IsFlock: true})
	if s.HasPosix(0, 1) {
		t.Fatal("a flock owner is not a posix owner")
	}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 2, Pid: 20})
	if !s.HasPosix(0, 2) {
		t.Fatal("posix owner should be reported")
	}
}

func TestWholeFileLock(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: math.MaxUint64, Type: Write, Owner: 1, Pid: 10})
	if _, granted := s.Acquire(Range{Start: 0, End: math.MaxUint64, Type: Write, Owner: 2, Pid: 20}); granted {
		t.Fatal("whole-file lock should block another owner")
	}
	if _, granted := s.Acquire(Range{Start: 100, End: 200, Type: Read, Owner: 2, Pid: 20}); granted {
		t.Fatal("partial overlap with whole-file lock should conflict")
	}
}

func TestReleaseNoExistingLocks(t *testing.T) {
	s := &Set{}
	s.Release(Range{Start: 0, End: 99, Type: Unlock, Owner: 1, Pid: 10})
	if !s.Empty() {
		t.Fatal("releasing on an empty set should be a no-op")
	}
}

func TestSameOwnerReplaceDifferentType(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 30, End: 60, Type: Read, Owner: 1, Pid: 10})

	if len(s.locks) != 3 {
		t.Fatalf("expected 3 locks after partial type change, got %d: %+v", len(s.locks), s.locks)
	}
	if s.locks[0].Type != Write || s.locks[0].Start != 0 || s.locks[0].End != 29 {
		t.Errorf("expected write [0,29], got %+v", s.locks[0])
	}
	if s.locks[1].Type != Read || s.locks[1].Start != 30 || s.locks[1].End != 60 {
		t.Errorf("expected read [30,60], got %+v", s.locks[1])
	}
	if s.locks[2].Type != Write || s.locks[2].Start != 61 || s.locks[2].End != 99 {
		t.Errorf("expected write [61,99], got %+v", s.locks[2])
	}
}

func TestNonAdjacentRangesNotCoalesced(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 5, End: math.MaxUint64, Type: Write, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 0, End: 2, Type: Write, Owner: 1, Pid: 10})

	if len(s.locks) != 2 {
		t.Fatalf("gap [3,4] should prevent coalescing, got %d: %+v", len(s.locks), s.locks)
	}
	if s.locks[0].Start != 0 || s.locks[0].End != 2 {
		t.Errorf("expected [0,2], got [%d,%d]", s.locks[0].Start, s.locks[0].End)
	}
	if s.locks[1].Start != 5 || s.locks[1].End != math.MaxUint64 {
		t.Errorf("expected [5,MaxUint64], got [%d,%d]", s.locks[1].Start, s.locks[1].End)
	}
}

func TestAdjacencyNoOverflowAtMaxUint64(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 100, End: math.MaxUint64, Type: Write, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 0, End: 0, Type: Write, Owner: 1, Pid: 10})

	if len(s.locks) != 2 {
		t.Fatalf("MaxUint64+1 must not wrap and falsely merge, got %d: %+v", len(s.locks), s.locks)
	}
}

// Sessions are part of owner identity: the same FUSE Owner number on two
// different mounts (Sid) is two distinct owners and must contend.
func TestTwoSessionsSameOwnerDoNotAlias(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 5, Pid: 10})

	if _, granted := s.Acquire(Range{Start: 50, End: 149, Type: Write, Sid: 2, Owner: 5, Pid: 20}); granted {
		t.Fatal("same Owner number on a different session must conflict, not alias")
	}
	// A read on session 2 against a session-1 read is fine (shared).
	s2 := &Set{}
	mustAcquire(t, s2, Range{Start: 0, End: 99, Type: Read, Sid: 1, Owner: 5})
	mustAcquire(t, s2, Range{Start: 0, End: 99, Type: Read, Sid: 2, Owner: 5})
}

// Reaping a dead mount drops only that session's locks.
func TestReleaseSessionReapsOnlyThatSession(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 49, Type: Write, Sid: 1, Owner: 1, Pid: 10})
	mustAcquire(t, s, Range{Start: 0, End: math.MaxUint64, Type: Write, Sid: 1, Owner: 2, Pid: 11, IsFlock: true})
	mustAcquire(t, s, Range{Start: 50, End: 99, Type: Write, Sid: 2, Owner: 1, Pid: 20})

	s.ReleaseSession(1)

	for _, h := range s.locks {
		if h.Sid == 1 {
			t.Fatalf("session 1 lock survived reaping: %+v", h)
		}
	}
	c, found := s.Conflict(Range{Start: 50, End: 99, Type: Write, Sid: 3, Owner: 9})
	if !found || c.Sid != 2 {
		t.Fatalf("session 2's lock should remain, got %+v found=%v", c, found)
	}
}

func TestEmptyAfterReleasingAll(t *testing.T) {
	s := &Set{}
	mustAcquire(t, s, Range{Start: 0, End: 99, Type: Write, Owner: 1, Pid: 10})
	if s.Empty() {
		t.Fatal("set should not be empty with a held lock")
	}
	s.Release(Range{Start: 0, End: 99, Type: Unlock, Owner: 1, Pid: 10})
	if !s.Empty() {
		t.Fatal("set should be empty after releasing the only lock")
	}
}
