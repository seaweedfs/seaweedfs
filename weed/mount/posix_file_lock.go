package mount

import (
	"math"
	"sort"
	"sync"
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// lockRange represents a single held POSIX byte-range lock.
type lockRange struct {
	Start uint64 // inclusive byte offset
	End   uint64 // inclusive; math.MaxUint64 means "to EOF"
	Typ   uint32 // syscall.F_RDLCK or syscall.F_WRLCK
	Owner uint64 // FUSE lock owner (from LkIn.Owner)
	Pid   uint32 // PID of lock holder (for GetLk reporting)
}

// inodeLocks holds all locks for one inode plus a waiter queue for SetLkw.
type inodeLocks struct {
	mu      sync.Mutex
	locks   []lockRange   // currently held locks, sorted by Start
	waiters []*lockWaiter // blocked SetLkw callers
}

// lockWaiter represents a blocked SetLkw caller.
type lockWaiter struct {
	ch chan struct{} // closed when the waiter should re-check
}

// PosixLockTable is the per-mount POSIX lock manager.
type PosixLockTable struct {
	mu     sync.Mutex
	inodes map[uint64]*inodeLocks
}

func NewPosixLockTable() *PosixLockTable {
	return &PosixLockTable{
		inodes: make(map[uint64]*inodeLocks),
	}
}

// getOrCreateInodeLocks returns the lock state for an inode, creating it if needed.
func (plt *PosixLockTable) getOrCreateInodeLocks(inode uint64) *inodeLocks {
	plt.mu.Lock()
	defer plt.mu.Unlock()
	il, ok := plt.inodes[inode]
	if !ok {
		il = &inodeLocks{}
		plt.inodes[inode] = il
	}
	return il
}

// getInodeLocks returns the lock state for an inode, or nil if none exists.
func (plt *PosixLockTable) getInodeLocks(inode uint64) *inodeLocks {
	plt.mu.Lock()
	defer plt.mu.Unlock()
	return plt.inodes[inode]
}

// maybeCleanupInode removes the inodeLocks entry if it has no locks and no waiters.
func (plt *PosixLockTable) maybeCleanupInode(inode uint64, il *inodeLocks) {
	// Caller must NOT hold il.mu. We acquire both locks in the correct order.
	plt.mu.Lock()
	defer plt.mu.Unlock()
	il.mu.Lock()
	defer il.mu.Unlock()
	if len(il.locks) == 0 && len(il.waiters) == 0 {
		delete(plt.inodes, inode)
	}
}

// rangesOverlap returns true if two inclusive ranges overlap.
func rangesOverlap(aStart, aEnd, bStart, bEnd uint64) bool {
	return aStart <= bEnd && bStart <= aEnd
}

// findConflict returns the first lock that conflicts with the proposed lock.
// A conflict exists when ranges overlap, at least one is a write lock, and the owners differ.
func findConflict(locks []lockRange, proposed lockRange) (lockRange, bool) {
	for _, h := range locks {
		if h.Owner == proposed.Owner {
			continue
		}
		if !rangesOverlap(h.Start, h.End, proposed.Start, proposed.End) {
			continue
		}
		if h.Typ == syscall.F_RDLCK && proposed.Typ == syscall.F_RDLCK {
			continue
		}
		return h, true
	}
	return lockRange{}, false
}

// insertAndCoalesce inserts a lock for the given owner, replacing/splitting any
// existing same-owner locks that overlap. Adjacent same-type locks are merged.
// Caller must hold il.mu.
func insertAndCoalesce(il *inodeLocks, lk lockRange) {
	owner := lk.Owner

	var kept []lockRange
	for _, h := range il.locks {
		if h.Owner != owner {
			kept = append(kept, h)
			continue
		}
		if !rangesOverlap(h.Start, h.End, lk.Start, lk.End) {
			// Check for adjacency with same type for merging.
			if h.Typ == lk.Typ && (h.End+1 == lk.Start || lk.End+1 == h.Start) {
				// Merge adjacent same-type lock into lk.
				if h.Start < lk.Start {
					lk.Start = h.Start
				}
				if h.End > lk.End {
					lk.End = h.End
				}
				continue
			}
			kept = append(kept, h)
			continue
		}
		// Overlapping same-owner lock.
		if h.Typ == lk.Typ {
			// Same type: absorb into lk (expand range).
			if h.Start < lk.Start {
				lk.Start = h.Start
			}
			if h.End > lk.End {
				lk.End = h.End
			}
			continue
		}
		// Different type: truncate or split the existing lock.
		if h.Start < lk.Start {
			// Left portion survives.
			left := h
			left.End = lk.Start - 1
			kept = append(kept, left)
		}
		if h.End > lk.End {
			// Right portion survives.
			right := h
			right.Start = lk.End + 1
			kept = append(kept, right)
		}
	}

	kept = append(kept, lk)
	sort.Slice(kept, func(i, j int) bool {
		return kept[i].Start < kept[j].Start
	})
	il.locks = kept
}

// removeLocks removes or splits locks owned by the given owner in the given range.
// Caller must hold il.mu.
func removeLocks(il *inodeLocks, owner uint64, start, end uint64) {
	var kept []lockRange
	for _, h := range il.locks {
		if h.Owner != owner || !rangesOverlap(h.Start, h.End, start, end) {
			kept = append(kept, h)
			continue
		}
		// h overlaps the unlock range.
		if h.Start < start {
			// Left portion survives.
			left := h
			left.End = start - 1
			kept = append(kept, left)
		}
		if h.End > end {
			// Right portion survives.
			right := h
			right.Start = end + 1
			kept = append(kept, right)
		}
		// If fully contained, it's simply dropped.
	}
	il.locks = kept
}

// wakeWaiters wakes all blocked SetLkw callers so they re-check.
// Caller must hold il.mu.
func wakeWaiters(il *inodeLocks) {
	for _, w := range il.waiters {
		select {
		case <-w.ch:
			// already closed
		default:
			close(w.ch)
		}
	}
	il.waiters = nil
}

// removeWaiter removes a specific waiter from the list.
// Caller must hold il.mu.
func removeWaiter(il *inodeLocks, w *lockWaiter) {
	for i, existing := range il.waiters {
		if existing == w {
			il.waiters = append(il.waiters[:i], il.waiters[i+1:]...)
			return
		}
	}
}

// GetLk checks for a conflicting lock. If found, it populates out with the
// conflict details. If no conflict, out.Typ is set to F_UNLCK.
func (plt *PosixLockTable) GetLk(inode uint64, proposed lockRange, out *fuse.LkOut) {
	il := plt.getInodeLocks(inode)
	if il == nil {
		out.Lk.Typ = syscall.F_UNLCK
		return
	}
	il.mu.Lock()
	conflict, found := findConflict(il.locks, proposed)
	il.mu.Unlock()

	if found {
		out.Lk.Start = conflict.Start
		out.Lk.End = conflict.End
		out.Lk.Typ = conflict.Typ
		out.Lk.Pid = conflict.Pid
	} else {
		out.Lk.Typ = syscall.F_UNLCK
	}
}

// SetLk attempts a non-blocking lock or unlock.
// For unlock (F_UNLCK): removes locks in the given range for the owner.
// For lock: returns fuse.EAGAIN if a conflict exists, fuse.OK on success.
func (plt *PosixLockTable) SetLk(inode uint64, lk lockRange) fuse.Status {
	if lk.Typ == syscall.F_UNLCK {
		il := plt.getInodeLocks(inode)
		if il == nil {
			return fuse.OK
		}
		il.mu.Lock()
		removeLocks(il, lk.Owner, lk.Start, lk.End)
		wakeWaiters(il)
		il.mu.Unlock()
		plt.maybeCleanupInode(inode, il)
		return fuse.OK
	}

	il := plt.getOrCreateInodeLocks(inode)
	il.mu.Lock()
	if _, found := findConflict(il.locks, lk); found {
		il.mu.Unlock()
		return fuse.EAGAIN
	}
	insertAndCoalesce(il, lk)
	il.mu.Unlock()
	return fuse.OK
}

// SetLkw attempts a blocking lock. It waits until the lock can be acquired
// or the cancel channel is closed.
func (plt *PosixLockTable) SetLkw(inode uint64, lk lockRange, cancel <-chan struct{}) fuse.Status {
	if lk.Typ == syscall.F_UNLCK {
		return plt.SetLk(inode, lk)
	}

	il := plt.getOrCreateInodeLocks(inode)
	for {
		il.mu.Lock()
		if _, found := findConflict(il.locks, lk); !found {
			insertAndCoalesce(il, lk)
			il.mu.Unlock()
			return fuse.OK
		}
		// Register waiter.
		waiter := &lockWaiter{ch: make(chan struct{})}
		il.waiters = append(il.waiters, waiter)
		il.mu.Unlock()

		// Block until woken or cancelled.
		select {
		case <-waiter.ch:
			// Woken — retry.
			continue
		case <-cancel:
			// Request cancelled.
			il.mu.Lock()
			removeWaiter(il, waiter)
			il.mu.Unlock()
			return fuse.EINTR
		}
	}
}

// ReleaseOwner removes all locks held by the given owner on the given inode.
// Called from FUSE Release to clean up when a file description is closed.
func (plt *PosixLockTable) ReleaseOwner(inode uint64, owner uint64) {
	il := plt.getInodeLocks(inode)
	if il == nil {
		return
	}
	il.mu.Lock()
	removeLocks(il, owner, 0, math.MaxUint64)
	wakeWaiters(il)
	il.mu.Unlock()
	plt.maybeCleanupInode(inode, il)
}
