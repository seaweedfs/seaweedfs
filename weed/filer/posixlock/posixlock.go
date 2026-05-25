// Package posixlock implements the conflict, coalescing, and range-split logic
// for POSIX advisory file locks — fcntl byte-range and flock whole-file — as a
// pure per-inode lock set with no concurrency control of its own.
//
// It is the server-side authority for distributed FUSE locking: the owner filer
// for an inode holds one Set and serializes access to it under that inode's
// per-path lock, so each operation runs to completion without concurrent
// mutation. The same algorithm can back the per-mount table; blocking (SetLkw)
// and any wait queue belong to the caller, not here.
package posixlock

import (
	"math"
	"sort"
)

// Lock types, kept independent of the platform syscall package (whose F_RDLCK /
// F_WRLCK / F_UNLCK values differ per OS and are absent on some) so the filer
// builds everywhere. Callers map the syscall constants onto these at the edge.
// Zero is intentionally unused so a zero-value Range reads as "unset".
const (
	Read   uint32 = 1
	Write  uint32 = 2
	Unlock uint32 = 3
)

// Range is one held advisory byte-range lock. Owner identity is the (Sid, Owner)
// pair: Sid is the mount session, Owner the FUSE lock owner within it, so owners
// from different mounts never alias. End is inclusive; math.MaxUint64 means EOF.
// IsFlock separates the flock and fcntl namespaces, which never conflict.
type Range struct {
	Start   uint64
	End     uint64
	Type    uint32
	Sid     uint64
	Owner   uint64
	Pid     uint32
	IsFlock bool
}

func (r Range) sameOwner(o Range) bool {
	return r.Sid == o.Sid && r.Owner == o.Owner
}

// Set is the authoritative set of advisory locks held on one inode. The zero
// value is an empty set. Set has no internal locking; the caller serializes it.
type Set struct {
	locks []Range // sorted by Start
}

func overlap(aStart, aEnd, bStart, bEnd uint64) bool {
	return aStart <= bEnd && bStart <= aEnd
}

// Conflict returns the first held lock that blocks proposed, if any. Two locks
// conflict when they share a namespace, have different owners, overlap, and at
// least one is a write lock.
func (s *Set) Conflict(proposed Range) (Range, bool) {
	for _, h := range s.locks {
		if h.IsFlock != proposed.IsFlock || h.sameOwner(proposed) {
			continue
		}
		if !overlap(h.Start, h.End, proposed.Start, proposed.End) {
			continue
		}
		if h.Type == Read && proposed.Type == Read {
			continue
		}
		return h, true
	}
	return Range{}, false
}

// Acquire grants lk when it does not conflict, inserting it and coalescing the
// owner's adjacent/overlapping ranges. On conflict it returns the blocking lock
// and false, leaving the set unchanged.
func (s *Set) Acquire(lk Range) (Range, bool) {
	if c, found := s.Conflict(lk); found {
		return c, false
	}
	s.insert(lk)
	return Range{}, true
}

// Grant inserts lk without a conflict check. It is for a client mirroring locks
// the server already granted (so they are conflict-free), not for arbitration.
func (s *Set) Grant(lk Range) {
	s.insert(lk)
}

// insert adds lk, absorbing same-owner same-type overlaps and merging adjacent
// same-type ranges, and truncating/splitting a same-owner range of a different
// type that overlaps (an in-place type change).
func (s *Set) insert(lk Range) {
	var kept []Range
	for _, h := range s.locks {
		if !h.sameOwner(lk) || h.IsFlock != lk.IsFlock {
			kept = append(kept, h)
			continue
		}
		if !overlap(h.Start, h.End, lk.Start, lk.End) {
			// Merge only ranges that are adjacent and the same type. The
			// End < MaxUint64 guards stop +1 from wrapping at EOF.
			if h.Type == lk.Type && ((h.End < math.MaxUint64 && h.End+1 == lk.Start) || (lk.End < math.MaxUint64 && lk.End+1 == h.Start)) {
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
		if h.Type == lk.Type {
			// Same type: absorb into lk by widening its range.
			if h.Start < lk.Start {
				lk.Start = h.Start
			}
			if h.End > lk.End {
				lk.End = h.End
			}
			continue
		}
		// Different type: the surviving portions of h outside lk's range stay.
		if h.Start < lk.Start {
			left := h
			left.End = lk.Start - 1
			kept = append(kept, left)
		}
		if h.End > lk.End {
			right := h
			right.Start = lk.End + 1
			kept = append(kept, right)
		}
	}
	kept = append(kept, lk)
	sort.Slice(kept, func(i, j int) bool { return kept[i].Start < kept[j].Start })
	s.locks = kept
}

// remove drops or splits matching locks within [start,end]. A match that
// straddles the range keeps its non-overlapping head and/or tail.
func (s *Set) remove(matches func(Range) bool, start, end uint64) {
	var kept []Range
	for _, h := range s.locks {
		if !matches(h) || !overlap(h.Start, h.End, start, end) {
			kept = append(kept, h)
			continue
		}
		if h.Start < start {
			left := h
			left.End = start - 1
			kept = append(kept, left)
		}
		if h.End > end {
			right := h
			right.Start = end + 1
			kept = append(kept, right)
		}
		// Fully covered: dropped.
	}
	s.locks = kept
}

// Release clears lk's owner's locks within lk's namespace over [lk.Start,lk.End]
// — the F_UNLCK path, which may split a straddling range.
func (s *Set) Release(lk Range) {
	s.remove(func(h Range) bool {
		return h.sameOwner(lk) && h.IsFlock == lk.IsFlock
	}, lk.Start, lk.End)
}

// ReleaseOwner removes every lock held by (sid, owner) in both namespaces.
func (s *Set) ReleaseOwner(sid, owner uint64) {
	s.remove(func(h Range) bool {
		return h.Sid == sid && h.Owner == owner
	}, 0, math.MaxUint64)
}

// ReleaseFlockOwner removes only the flock locks of (sid, owner) — the close-time
// path for a released file description (FUSE_RELEASE_FLOCK_UNLOCK).
func (s *Set) ReleaseFlockOwner(sid, owner uint64) {
	s.remove(func(h Range) bool {
		return h.IsFlock && h.Sid == sid && h.Owner == owner
	}, 0, math.MaxUint64)
}

// ReleasePosixOwner removes only the fcntl locks of (sid, owner) — the close-time
// path for a flushing POSIX lock owner.
func (s *Set) ReleasePosixOwner(sid, owner uint64) {
	s.remove(func(h Range) bool {
		return !h.IsFlock && h.Sid == sid && h.Owner == owner
	}, 0, math.MaxUint64)
}

// ReleaseSession removes every lock held by a session, reaping a mount that has
// died or disconnected (its lease expired).
func (s *Set) ReleaseSession(sid uint64) {
	s.remove(func(h Range) bool { return h.Sid == sid }, 0, math.MaxUint64)
}

// HasPosix reports whether (sid, owner) holds any fcntl lock, mirroring the
// mount's flush-time check that avoids treating a lock-free flush as
// lock-sensitive.
func (s *Set) HasPosix(sid, owner uint64) bool {
	for _, h := range s.locks {
		if !h.IsFlock && h.Sid == sid && h.Owner == owner {
			return true
		}
	}
	return false
}

// Locks returns the held locks, sorted by Start. The slice aliases internal
// state; the caller must not mutate it.
func (s *Set) Locks() []Range { return s.locks }

// Empty reports whether no locks are held, so the caller can drop the inode's
// entry from its table.
func (s *Set) Empty() bool { return len(s.locks) == 0 }
