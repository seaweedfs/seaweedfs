package posixlock

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerGrantAndConflict(t *testing.T) {
	m := NewManager()
	if _, granted := m.TryLock("a", Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 1}); !granted {
		t.Fatal("first lock should be granted")
	}
	if c, granted := m.TryLock("a", Range{Start: 50, End: 149, Type: Write, Sid: 2, Owner: 1}); granted {
		t.Fatalf("overlapping lock from another session should conflict, got grant; conflict=%+v", c)
	}
	// A different key is independent.
	if _, granted := m.TryLock("b", Range{Start: 0, End: 99, Type: Write, Sid: 2, Owner: 1}); !granted {
		t.Fatal("lock on a different key should be granted")
	}
}

func TestManagerUnlockCleansEmptyKeyAndIndex(t *testing.T) {
	m := NewManager()
	lk := Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 1}
	m.TryLock("a", lk)

	if !m.bySid[1]["a"] {
		t.Fatal("session index should record the held key")
	}
	m.Unlock("a", Range{Start: 0, End: 99, Type: Unlock, Sid: 1, Owner: 1})

	if _, ok := m.byKey["a"]; ok {
		t.Fatal("empty set should be dropped from byKey")
	}
	if _, ok := m.bySid[1]; ok {
		t.Fatal("session index should be pruned when it holds nothing")
	}
}

func TestManagerPartialUnlockKeepsIndex(t *testing.T) {
	m := NewManager()
	m.TryLock("a", Range{Start: 0, End: 49, Type: Write, Sid: 1, Owner: 1})
	m.TryLock("a", Range{Start: 100, End: 149, Type: Write, Sid: 1, Owner: 1})
	// Release one of the two ranges; the session still holds the other.
	m.Unlock("a", Range{Start: 0, End: 49, Type: Unlock, Sid: 1, Owner: 1})

	if !m.bySid[1]["a"] {
		t.Fatal("session still holds a lock on the key; index must remain")
	}
	if _, ok := m.byKey["a"]; !ok {
		t.Fatal("key should remain while a lock is held")
	}
}

func TestManagerGetLk(t *testing.T) {
	m := NewManager()
	m.TryLock("a", Range{Start: 10, End: 50, Type: Write, Sid: 1, Owner: 1, Pid: 7})
	c, found := m.GetLk("a", Range{Start: 30, End: 70, Type: Read, Sid: 2, Owner: 1})
	if !found || c.Pid != 7 {
		t.Fatalf("expected conflict from pid 7, got %+v found=%v", c, found)
	}
	if _, found := m.GetLk("missing", Range{Start: 0, End: 1, Type: Write, Sid: 9, Owner: 9}); found {
		t.Fatal("missing key should report no conflict")
	}
}

func TestManagerReleasePosixOwnerKeepsFlockAndIndex(t *testing.T) {
	m := NewManager()
	m.TryLock("a", Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 1})
	m.TryLock("a", Range{Start: 0, End: math.MaxUint64, Type: Write, Sid: 1, Owner: 1, IsFlock: true})

	m.ReleasePosixOwner("a", 1, 1)

	// flock lock for the same session remains, so the index must remain too.
	if !m.bySid[1]["a"] {
		t.Fatal("session still holds the flock lock; index must remain")
	}
	if _, found := m.GetLk("a", Range{Start: 0, End: 10, Type: Write, Sid: 2, Owner: 2, IsFlock: true}); !found {
		t.Fatal("flock lock should survive ReleasePosixOwner")
	}
	if _, found := m.GetLk("a", Range{Start: 0, End: 10, Type: Write, Sid: 2, Owner: 2}); found {
		t.Fatal("fcntl lock should be gone after ReleasePosixOwner")
	}
}

func TestManagerReleaseSessionReapsAcrossKeys(t *testing.T) {
	m := NewManager()
	m.TryLock("a", Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 1})
	m.TryLock("b", Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 2})
	m.TryLock("b", Range{Start: 200, End: 299, Type: Write, Sid: 2, Owner: 1})

	m.ReleaseSession(1)

	if _, ok := m.bySid[1]; ok {
		t.Fatal("reaped session should be gone from the index")
	}
	if _, ok := m.byKey["a"]; ok {
		t.Fatal("key a held only session 1's lock and should be dropped")
	}
	// Session 2's lock on b survives.
	if _, found := m.GetLk("b", Range{Start: 200, End: 299, Type: Write, Sid: 9, Owner: 9}); !found {
		t.Fatal("session 2's lock on b should remain after reaping session 1")
	}
	if !m.bySid[2]["b"] {
		t.Fatal("session 2 index entry should remain")
	}
}

func TestManagerReapsOnlyStaleLeasedSessions(t *testing.T) {
	m := NewManager()
	// Session 1: holds a lock, leased but stale (renewed long ago).
	m.TryLock("a", Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 1})
	m.Renew(1)
	m.lastSeen[1] = time.Now().Add(-time.Hour)
	// Session 2: holds a lock, leased and fresh.
	m.TryLock("b", Range{Start: 0, End: 99, Type: Write, Sid: 2, Owner: 1})
	m.Renew(2)
	// Session 3: holds a lock but never renewed (no lease) — must not be reaped.
	m.TryLock("c", Range{Start: 0, End: 99, Type: Write, Sid: 3, Owner: 1})

	reaped := m.ReapExpired(30 * time.Second)

	if len(reaped) != 1 || reaped[0] != 1 {
		t.Fatalf("only the stale leased session should be reaped, got %v", reaped)
	}
	if _, ok := m.byKey["a"]; ok {
		t.Fatal("stale session's lock should be gone")
	}
	if _, ok := m.byKey["b"]; !ok {
		t.Fatal("fresh session's lock must remain")
	}
	if _, ok := m.byKey["c"]; !ok {
		t.Fatal("never-renewed session must not be reaped")
	}
	if _, ok := m.lastSeen[1]; ok {
		t.Fatal("reaped session's lease entry should be cleared")
	}
}

// Mutual exclusion under concurrent whole-file flock churn through the Manager:
// at most one owner may believe it holds the exclusive lock at any instant.
func TestManagerConcurrentFlockMutualExclusion(t *testing.T) {
	m := NewManager()
	const (
		key     = "inode"
		workers = 16
		iters   = 400
	)
	var (
		wg      sync.WaitGroup
		holder  atomic.Int64
		overlap atomic.Int32
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lk := Range{Start: 0, End: math.MaxUint64, Type: Write, Sid: uint64(id + 1), Owner: 1, IsFlock: true}
			unlock := lk
			unlock.Type = Unlock
			token := int64(id + 1)
			for i := 0; i < iters; i++ {
				for {
					if _, granted := m.TryLock(key, lk); granted {
						break
					}
					runtime.Gosched()
				}
				if prev := holder.Swap(token); prev != 0 {
					overlap.Add(1)
				}
				runtime.Gosched()
				if !holder.CompareAndSwap(token, 0) {
					overlap.Add(1)
				}
				m.Unlock(key, unlock)
			}
		}(w)
	}
	wg.Wait()
	if n := overlap.Load(); n != 0 {
		t.Fatalf("mutual exclusion violated %d times", n)
	}
	if len(m.byKey) != 0 {
		t.Fatalf("all locks released; byKey should be empty, got %d", len(m.byKey))
	}
	if len(m.bySid) != 0 {
		t.Fatalf("all locks released; bySid should be empty, got %d", len(m.bySid))
	}
}
