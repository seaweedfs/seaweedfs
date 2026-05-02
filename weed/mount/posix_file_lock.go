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
	// flock and fcntl locks have different ownership and close semantics.
	// Keep them in separate namespaces inside the table.
	IsFlock bool
}

// inodeLocks holds all locks for one inode plus a waiter queue for SetLkw.
type inodeLocks struct {
	mu       sync.Mutex
	locks    []lockRange   // currently held locks, sorted by Start
	waiters  []*lockWaiter // blocked SetLkw callers
	wakeRefs int           // woken waiters still retrying on this inodeLocks
	// dead marks an inodeLocks that has been removed from the table's map.
	// A caller holding a pointer returned by getInodeLocks/getOrCreateInodeLocks
	// must recheck this flag after acquiring il.mu: if true, the pointer is
	// orphaned and must be refreshed from the table so findConflict runs
	// against the live state.
	dead bool
}

// lockWaiter represents a blocked SetLkw caller.
type lockWaiter struct {
	requested   lockRange     // the lock this waiter is trying to acquire
	ch          chan struct{} // closed when the waiter should re-check
	wakeRefHeld bool
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

// getOrCreateInodeLocks returns the lock state for an inode, creating it if
// needed. A dead entry (which maybeCleanupInode normally evicts together
// with setting dead) is replaced transparently so callers never observe one
// via this path and the SetLk retry loop cannot spin on a stale map entry.
// The il.dead read is safe without il.mu: dead transitions only under both
// plt.mu and il.mu, so holding plt.mu here is sufficient happens-before.
func (plt *PosixLockTable) getOrCreateInodeLocks(inode uint64) *inodeLocks {
	plt.mu.Lock()
	defer plt.mu.Unlock()
	il, ok := plt.inodes[inode]
	if !ok || il.dead {
		il = &inodeLocks{}
		plt.inodes[inode] = il
	}
	return il
}

// getInodeLocks returns the lock state for an inode, or nil if none exists.
// A dead entry (which maybeCleanupInode normally evicts together with setting
// dead) is dropped so callers never observe an orphaned inodeLocks via this
// path. The il.dead read is safe without il.mu for the same reason as in
// getOrCreateInodeLocks: transitions happen under both plt.mu and il.mu.
func (plt *PosixLockTable) getInodeLocks(inode uint64) *inodeLocks {
	plt.mu.Lock()
	defer plt.mu.Unlock()
	il, ok := plt.inodes[inode]
	if !ok {
		return nil
	}
	if il.dead {
		delete(plt.inodes, inode)
		return nil
	}
	return il
}

// maybeCleanupInode removes the inodeLocks entry if it has no locks, no waiters,
// and no woken waiters still retrying against this inodeLocks. The removed
// inodeLocks is marked dead so any caller that is mid-way through acquiring
// il.mu with a stale pointer will notice and refetch from the map instead of
// mutating an orphaned instance (which would be invisible to future callers).
func (plt *PosixLockTable) maybeCleanupInode(inode uint64, il *inodeLocks) {
	// Caller must NOT hold il.mu. We acquire both locks in the correct order.
	plt.mu.Lock()
	defer plt.mu.Unlock()
	il.mu.Lock()
	defer il.mu.Unlock()
	if len(il.locks) == 0 && len(il.waiters) == 0 && il.wakeRefs == 0 {
		if plt.inodes[inode] == il {
			delete(plt.inodes, inode)
		}
		il.dead = true
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
		if h.IsFlock != proposed.IsFlock {
			continue
		}
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
		if h.Owner != owner || h.IsFlock != lk.IsFlock {
			kept = append(kept, h)
			continue
		}
		if !rangesOverlap(h.Start, h.End, lk.Start, lk.End) {
			// Check for adjacency with same type for merging.
			if h.Typ == lk.Typ && ((h.End < ^uint64(0) && h.End+1 == lk.Start) || (lk.End < ^uint64(0) && lk.End+1 == h.Start)) {
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

// removeLocks removes or splits matching locks in the given range.
// Caller must hold il.mu.
func removeLocks(il *inodeLocks, matches func(lockRange) bool, start, end uint64) {
	var kept []lockRange
	for _, h := range il.locks {
		if !matches(h) || !rangesOverlap(h.Start, h.End, start, end) {
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

func (plt *PosixLockTable) releaseMatching(inode uint64, matches func(lockRange) bool) {
	il := plt.getInodeLocks(inode)
	if il == nil {
		return
	}
	il.mu.Lock()
	removeLocks(il, matches, 0, math.MaxUint64)
	wakeEligibleWaiters(il)
	il.mu.Unlock()
	plt.maybeCleanupInode(inode, il)
}

// HasPosixOwner reports whether owner currently holds any POSIX byte-range
// locks on inode. FUSE may provide a non-zero FlushIn.LockOwner even when no
// locks were taken, so callers should consult the lock table before treating a
// flush as lock-sensitive.
func (plt *PosixLockTable) HasPosixOwner(inode uint64, owner uint64) bool {
	if owner == 0 {
		return false
	}
	il := plt.getInodeLocks(inode)
	if il == nil {
		return false
	}
	il.mu.Lock()
	defer il.mu.Unlock()
	if il.dead {
		return false
	}
	for _, lk := range il.locks {
		if !lk.IsFlock && lk.Owner == owner {
			return true
		}
	}
	return false
}

// releaseWakeRef drops the temporary reference that keeps inodeLocks live while
// a woken waiter retries its SetLkw acquisition.
func releaseWakeRef(il *inodeLocks, waiter *lockWaiter) {
	if waiter == nil || !waiter.wakeRefHeld {
		return
	}
	waiter.wakeRefHeld = false
	il.wakeRefs--
}

// wakeEligibleWaiters selectively wakes blocked SetLkw callers that can now
// succeed given the current lock state. Waiters whose requests still conflict
// with held locks remain in the queue, avoiding a thundering herd.
// Caller must hold il.mu.
func wakeEligibleWaiters(il *inodeLocks) {
	remaining := il.waiters[:0]
	for _, w := range il.waiters {
		if _, conflicted := findConflict(il.locks, w.requested); !conflicted {
			w.wakeRefHeld = true
			il.wakeRefs++
			close(w.ch)
		} else {
			remaining = append(remaining, w)
		}
	}
	il.waiters = remaining
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
	for {
		il := plt.getInodeLocks(inode)
		if il == nil {
			out.Lk.Typ = syscall.F_UNLCK
			return
		}
		il.mu.Lock()
		if il.dead {
			// Orphaned by a concurrent cleanup: the map may already hold a
			// fresh inodeLocks with different state, so answering from this
			// stale pointer would miss real conflicts. Refetch.
			il.mu.Unlock()
			continue
		}
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
		return
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
		if il.dead {
			// Orphaned by a concurrent cleanup: any lock we may have held
			// is gone with it, and there is no point waking waiters on a
			// detached inodeLocks.
			il.mu.Unlock()
			return fuse.OK
		}
		removeLocks(il, func(existing lockRange) bool {
			return existing.Owner == lk.Owner && existing.IsFlock == lk.IsFlock
		}, lk.Start, lk.End)
		wakeEligibleWaiters(il)
		il.mu.Unlock()
		plt.maybeCleanupInode(inode, il)
		return fuse.OK
	}

	for {
		il := plt.getOrCreateInodeLocks(inode)
		il.mu.Lock()
		if il.dead {
			il.mu.Unlock()
			continue
		}
		if _, found := findConflict(il.locks, lk); found {
			il.mu.Unlock()
			return fuse.EAGAIN
		}
		insertAndCoalesce(il, lk)
		il.mu.Unlock()
		return fuse.OK
	}
}

// SetLkw attempts a blocking lock. It waits until the lock can be acquired
// or the cancel channel is closed.
func (plt *PosixLockTable) SetLkw(inode uint64, lk lockRange, cancel <-chan struct{}) fuse.Status {
	if lk.Typ == syscall.F_UNLCK {
		return plt.SetLk(inode, lk)
	}

	il := plt.getOrCreateInodeLocks(inode)
	var waiter *lockWaiter
	for {
		il.mu.Lock()
		if il.dead {
			// The inodeLocks we were holding was orphaned by a concurrent
			// cleanup. Refetch the live instance from the table and retry.
			// A dead il never has waiters or wakeRefs (the cleanup condition
			// requires both to be zero), so waiter is guaranteed nil here.
			il.mu.Unlock()
			il = plt.getOrCreateInodeLocks(inode)
			continue
		}
		releaseWakeRef(il, waiter)
		if _, found := findConflict(il.locks, lk); !found {
			insertAndCoalesce(il, lk)
			il.mu.Unlock()
			return fuse.OK
		}
		// Register waiter with the requested lock details for selective waking.
		waiter = &lockWaiter{requested: lk, ch: make(chan struct{})}
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
			releaseWakeRef(il, waiter)
			removeWaiter(il, waiter)
			il.mu.Unlock()
			plt.maybeCleanupInode(inode, il)
			return fuse.EINTR
		}
	}
}

// ReleaseOwner removes all locks held by the given owner on the given inode.
// Used for same-owner cleanup in tests and lock-table operations.
func (plt *PosixLockTable) ReleaseOwner(inode uint64, owner uint64) {
	plt.releaseMatching(inode, func(lk lockRange) bool {
		return lk.Owner == owner
	})
}

// ReleaseFlockOwner removes flock locks for a released file description.
// FUSE only provides LockOwner on RELEASE when FUSE_RELEASE_FLOCK_UNLOCK is set.
func (plt *PosixLockTable) ReleaseFlockOwner(inode uint64, owner uint64) {
	plt.releaseMatching(inode, func(lk lockRange) bool {
		return lk.IsFlock && lk.Owner == owner
	})
}

// ReleasePosixOwner removes POSIX fcntl locks for a closing lock owner.
// FUSE passes the closing fi->owner on FLUSH, which is the correct close-time
// identity for POSIX byte-range lock cleanup.
func (plt *PosixLockTable) ReleasePosixOwner(inode uint64, owner uint64) {
	plt.releaseMatching(inode, func(lk lockRange) bool {
		return !lk.IsFlock && lk.Owner == owner
	})
}
