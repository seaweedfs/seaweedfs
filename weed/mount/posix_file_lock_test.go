package mount

import (
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

func TestNonOverlappingLocksFromDifferentOwners(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	s1 := plt.SetLk(inode, lockRange{Start: 0, End: 49, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	if s1 != fuse.OK {
		t.Fatalf("expected OK, got %v", s1)
	}
	s2 := plt.SetLk(inode, lockRange{Start: 50, End: 99, Typ: syscall.F_WRLCK, Owner: 2, Pid: 20})
	if s2 != fuse.OK {
		t.Fatalf("expected OK, got %v", s2)
	}
}

func TestOverlappingReadLocksFromDifferentOwners(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	s1 := plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_RDLCK, Owner: 1, Pid: 10})
	if s1 != fuse.OK {
		t.Fatalf("expected OK, got %v", s1)
	}
	s2 := plt.SetLk(inode, lockRange{Start: 50, End: 149, Typ: syscall.F_RDLCK, Owner: 2, Pid: 20})
	if s2 != fuse.OK {
		t.Fatalf("expected OK, got %v", s2)
	}
}

func TestOverlappingWriteReadConflict(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	s := plt.SetLk(inode, lockRange{Start: 50, End: 149, Typ: syscall.F_RDLCK, Owner: 2, Pid: 20})
	if s != fuse.EAGAIN {
		t.Fatalf("expected EAGAIN, got %v", s)
	}
}

func TestOverlappingWriteWriteConflict(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	s := plt.SetLk(inode, lockRange{Start: 50, End: 149, Typ: syscall.F_WRLCK, Owner: 2, Pid: 20})
	if s != fuse.EAGAIN {
		t.Fatalf("expected EAGAIN, got %v", s)
	}
}

func TestSameOwnerUpgradeReadToWrite(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_RDLCK, Owner: 1, Pid: 10})
	s := plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	if s != fuse.OK {
		t.Fatalf("expected OK for same-owner upgrade, got %v", s)
	}

	// Verify the lock is now a write lock.
	var out fuse.LkOut
	plt.GetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 2, Pid: 20}, &out)
	if out.Lk.Typ != syscall.F_WRLCK {
		t.Fatalf("expected conflicting write lock, got type %d", out.Lk.Typ)
	}
}

func TestSameOwnerDowngradeWriteToRead(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	s := plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_RDLCK, Owner: 1, Pid: 10})
	if s != fuse.OK {
		t.Fatalf("expected OK for same-owner downgrade, got %v", s)
	}

	// Another owner should now be able to get a read lock.
	s2 := plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_RDLCK, Owner: 2, Pid: 20})
	if s2 != fuse.OK {
		t.Fatalf("expected OK for shared read lock, got %v", s2)
	}
}

func TestLockCoalescing(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 9, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	plt.SetLk(inode, lockRange{Start: 10, End: 19, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})

	il := plt.getInodeLocks(inode)
	il.mu.Lock()
	ownerLocks := 0
	for _, lk := range il.locks {
		if lk.Owner == 1 {
			ownerLocks++
			if lk.Start != 0 || lk.End != 19 {
				t.Errorf("expected coalesced lock [0,19], got [%d,%d]", lk.Start, lk.End)
			}
		}
	}
	il.mu.Unlock()
	if ownerLocks != 1 {
		t.Fatalf("expected 1 coalesced lock, got %d", ownerLocks)
	}
}

func TestLockSplitting(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	// Unlock the middle portion.
	plt.SetLk(inode, lockRange{Start: 40, End: 59, Typ: syscall.F_UNLCK, Owner: 1, Pid: 10})

	il := plt.getInodeLocks(inode)
	il.mu.Lock()
	ownerLocks := 0
	for _, lk := range il.locks {
		if lk.Owner == 1 {
			ownerLocks++
		}
	}
	if ownerLocks != 2 {
		il.mu.Unlock()
		t.Fatalf("expected 2 locks after split, got %d", ownerLocks)
	}
	// Check the ranges.
	if il.locks[0].Start != 0 || il.locks[0].End != 39 {
		t.Errorf("expected left lock [0,39], got [%d,%d]", il.locks[0].Start, il.locks[0].End)
	}
	if il.locks[1].Start != 60 || il.locks[1].End != 99 {
		t.Errorf("expected right lock [60,99], got [%d,%d]", il.locks[1].Start, il.locks[1].End)
	}
	il.mu.Unlock()
}

func TestGetLkConflict(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 10, End: 50, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})

	var out fuse.LkOut
	plt.GetLk(inode, lockRange{Start: 30, End: 70, Typ: syscall.F_RDLCK, Owner: 2, Pid: 20}, &out)
	if out.Lk.Typ != syscall.F_WRLCK {
		t.Fatalf("expected conflicting write lock, got type %d", out.Lk.Typ)
	}
	if out.Lk.Pid != 10 {
		t.Fatalf("expected holder PID 10, got %d", out.Lk.Pid)
	}
	if out.Lk.Start != 10 || out.Lk.End != 50 {
		t.Fatalf("expected conflict [10,50], got [%d,%d]", out.Lk.Start, out.Lk.End)
	}
}

func TestGetLkNoConflict(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 10, End: 50, Typ: syscall.F_RDLCK, Owner: 1, Pid: 10})

	var out fuse.LkOut
	plt.GetLk(inode, lockRange{Start: 30, End: 70, Typ: syscall.F_RDLCK, Owner: 2, Pid: 20}, &out)
	if out.Lk.Typ != syscall.F_UNLCK {
		t.Fatalf("expected F_UNLCK (no conflict), got type %d", out.Lk.Typ)
	}
}

func TestGetLkSameOwnerNoConflict(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})

	var out fuse.LkOut
	plt.GetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10}, &out)
	if out.Lk.Typ != syscall.F_UNLCK {
		t.Fatalf("same owner should not conflict with itself, got type %d", out.Lk.Typ)
	}
}

func TestReleaseOwner(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 49, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	plt.SetLk(inode, lockRange{Start: 50, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	plt.SetLk(inode, lockRange{Start: 200, End: 299, Typ: syscall.F_RDLCK, Owner: 2, Pid: 20})

	plt.ReleaseOwner(inode, 1)

	// Owner 1's locks should be gone.
	var out fuse.LkOut
	plt.GetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 3, Pid: 30}, &out)
	if out.Lk.Typ != syscall.F_UNLCK {
		t.Fatalf("expected no conflict after ReleaseOwner, got type %d", out.Lk.Typ)
	}

	// Owner 2's lock should still exist.
	plt.GetLk(inode, lockRange{Start: 200, End: 299, Typ: syscall.F_WRLCK, Owner: 3, Pid: 30}, &out)
	if out.Lk.Typ != syscall.F_RDLCK {
		t.Fatalf("expected owner 2's read lock to remain, got type %d", out.Lk.Typ)
	}
}

func TestReleaseOwnerWakesWaiters(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})

	done := make(chan fuse.Status, 1)
	go func() {
		cancel := make(chan struct{})
		s := plt.SetLkw(inode, lockRange{Start: 50, End: 60, Typ: syscall.F_WRLCK, Owner: 2, Pid: 20}, cancel)
		done <- s
	}()

	// Give the goroutine time to block.
	time.Sleep(50 * time.Millisecond)

	plt.ReleaseOwner(inode, 1)

	select {
	case s := <-done:
		if s != fuse.OK {
			t.Fatalf("expected OK after ReleaseOwner woke waiter, got %v", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SetLkw did not unblock after ReleaseOwner")
	}
}

func TestSetLkwBlocksAndSucceeds(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})

	done := make(chan fuse.Status, 1)
	go func() {
		cancel := make(chan struct{})
		s := plt.SetLkw(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 2, Pid: 20}, cancel)
		done <- s
	}()

	// Give the goroutine time to block.
	time.Sleep(50 * time.Millisecond)

	// Release the conflicting lock.
	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_UNLCK, Owner: 1, Pid: 10})

	select {
	case s := <-done:
		if s != fuse.OK {
			t.Fatalf("expected OK, got %v", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SetLkw did not unblock after conflicting lock was released")
	}
}

func TestSetLkwCancellation(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})

	cancel := make(chan struct{})
	done := make(chan fuse.Status, 1)
	go func() {
		s := plt.SetLkw(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 2, Pid: 20}, cancel)
		done <- s
	}()

	// Give the goroutine time to block.
	time.Sleep(50 * time.Millisecond)

	close(cancel)

	select {
	case s := <-done:
		if s != fuse.EINTR {
			t.Fatalf("expected EINTR on cancel, got %v", s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SetLkw did not unblock after cancel")
	}
}

func TestWholeFileLock(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	// Simulate flock() — whole-file exclusive lock.
	s1 := plt.SetLk(inode, lockRange{Start: 0, End: math.MaxUint64, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	if s1 != fuse.OK {
		t.Fatalf("expected OK, got %v", s1)
	}

	// Second owner should be blocked.
	s2 := plt.SetLk(inode, lockRange{Start: 0, End: math.MaxUint64, Typ: syscall.F_WRLCK, Owner: 2, Pid: 20})
	if s2 != fuse.EAGAIN {
		t.Fatalf("expected EAGAIN, got %v", s2)
	}

	// Even a partial overlap should fail.
	s3 := plt.SetLk(inode, lockRange{Start: 100, End: 200, Typ: syscall.F_RDLCK, Owner: 2, Pid: 20})
	if s3 != fuse.EAGAIN {
		t.Fatalf("expected EAGAIN for partial overlap with whole-file lock, got %v", s3)
	}
}

func TestUnlockNoExistingLocks(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	// Unlock on an inode with no locks should succeed silently.
	s := plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_UNLCK, Owner: 1, Pid: 10})
	if s != fuse.OK {
		t.Fatalf("expected OK for unlock with no existing locks, got %v", s)
	}
}

func TestMultipleInodesIndependent(t *testing.T) {
	plt := NewPosixLockTable()

	// Write lock on inode 1 should not affect inode 2.
	plt.SetLk(1, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	s := plt.SetLk(2, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 2, Pid: 20})
	if s != fuse.OK {
		t.Fatalf("locks on different inodes should be independent, got %v", s)
	}
}

func TestMemoryCleanup(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	plt.ReleaseOwner(inode, 1)

	plt.mu.Lock()
	_, exists := plt.inodes[inode]
	plt.mu.Unlock()
	if exists {
		t.Fatal("expected inode entry to be cleaned up after all locks released")
	}
}

func TestSameOwnerReplaceDifferentType(t *testing.T) {
	plt := NewPosixLockTable()
	inode := uint64(1)

	// Lock [0, 99] as write.
	plt.SetLk(inode, lockRange{Start: 0, End: 99, Typ: syscall.F_WRLCK, Owner: 1, Pid: 10})
	// Replace middle portion [30, 60] with read lock.
	plt.SetLk(inode, lockRange{Start: 30, End: 60, Typ: syscall.F_RDLCK, Owner: 1, Pid: 10})

	il := plt.getInodeLocks(inode)
	il.mu.Lock()
	defer il.mu.Unlock()

	// Should have 3 locks: write [0,29], read [30,60], write [61,99].
	if len(il.locks) != 3 {
		t.Fatalf("expected 3 locks after partial type change, got %d", len(il.locks))
	}
	if il.locks[0].Typ != syscall.F_WRLCK || il.locks[0].Start != 0 || il.locks[0].End != 29 {
		t.Errorf("expected write [0,29], got type=%d [%d,%d]", il.locks[0].Typ, il.locks[0].Start, il.locks[0].End)
	}
	if il.locks[1].Typ != syscall.F_RDLCK || il.locks[1].Start != 30 || il.locks[1].End != 60 {
		t.Errorf("expected read [30,60], got type=%d [%d,%d]", il.locks[1].Typ, il.locks[1].Start, il.locks[1].End)
	}
	if il.locks[2].Typ != syscall.F_WRLCK || il.locks[2].Start != 61 || il.locks[2].End != 99 {
		t.Errorf("expected write [61,99], got type=%d [%d,%d]", il.locks[2].Typ, il.locks[2].Start, il.locks[2].End)
	}
}
