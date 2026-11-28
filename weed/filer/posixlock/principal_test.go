package posixlock

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func newScopedTestManager() (*Manager, *testClock, time.Time) {
	start := time.Unix(1_000_000, 0)
	clock := &testClock{now: start}
	return NewManagerWithClock(clock.Now), clock, start
}

func TestManagerOwnersWithEqualSidAndOwnerDifferUnderAnotherPrincipal(t *testing.T) {
	m, _, start := newScopedTestManager()
	lease := Lease{TTL: 15 * time.Second, NotAfter: start.Add(time.Hour)}
	holder := Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 1, Pid: 7, Principal: "a"}
	if _, granted := m.TryLock("k", holder, lease); !granted {
		t.Fatal("holder should be granted")
	}
	for _, other := range []string{"b", ""} {
		same := holder
		same.Principal = other
		if c, granted := m.TryLock("k", same, lease); granted || c.Pid != 7 {
			t.Fatalf("principal %q with the holder's sid/owner must conflict, got granted=%v conflict=%+v", other, granted, c)
		}
		if _, found := m.GetLk("k", same); !found {
			t.Fatalf("GET_LK under principal %q must report the holder", other)
		}
		unlock := same
		unlock.Type, unlock.Start, unlock.End = Unlock, 10, 20
		m.Unlock("k", unlock)
		m.ReleasePosixOwner("k", same.Session(), same.Owner)
		m.ReleaseSession(same.Session())
		m.Reassert("k", same.Session(), []Range{same}, lease)
		m.Renew("k", same.Session(), lease)
	}
	if got := m.Snapshot()["k"]; !reflect.DeepEqual(got, []Range{holder}) {
		t.Fatalf("holder's lock changed by other principals: %+v", got)
	}
	if _, found := m.GetLk("k", holder); found {
		t.Fatal("the holder must not conflict with itself")
	}
}

func TestManagerAdjacentRangesOfDifferentPrincipalsNeverCoalesce(t *testing.T) {
	m, _, start := newScopedTestManager()
	lease := Lease{TTL: 15 * time.Second, NotAfter: start.Add(time.Hour)}
	m.TryLock("k", Range{Start: 0, End: 9, Type: Read, Sid: 1, Owner: 1, Principal: "a"}, lease)
	m.TryLock("k", Range{Start: 10, End: 19, Type: Read, Sid: 1, Owner: 1, Principal: "b"}, lease)
	m.TryLock("k", Range{Start: 20, End: 29, Type: Read, Sid: 1, Owner: 1}, lease)
	if got := m.Snapshot()["k"]; len(got) != 3 {
		t.Fatalf("ranges of different principals must stay separate, got %+v", got)
	}
	// A partial unlock by one principal splits only its own range.
	m.Unlock("k", Range{Start: 3, End: 5, Type: Unlock, Sid: 1, Owner: 1, Principal: "a"})
	if got := m.Snapshot()["k"]; len(got) != 4 || got[0].End != 2 || got[1].Start != 6 || got[2].Principal != "b" {
		t.Fatalf("partial unlock should split only principal a's range, got %+v", got)
	}
}

func TestManagerScopedLeaseEndsAtEarlierOfTTLAndNotAfter(t *testing.T) {
	cases := []struct {
		name     string
		notAfter time.Duration
		ends     time.Duration
	}{
		{name: "ttl first", notAfter: time.Hour, ends: 15 * time.Second},
		{name: "token expiry first", notAfter: 5 * time.Second, ends: 5 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, clock, start := newScopedTestManager()
			lk := Range{Start: 0, End: 99, Type: Write, Sid: 1, Owner: 1, Principal: "a"}
			m.TryLock("k", lk, Lease{TTL: 15 * time.Second, NotAfter: start.Add(tc.notAfter)})
			other := Range{Start: 0, End: 99, Type: Write, Sid: 2, Owner: 1, Principal: "b"}

			clock.Set(start.Add(tc.ends - time.Nanosecond))
			if _, found := m.GetLk("k", other); !found {
				t.Fatal("lease must hold until its deadline")
			}
			clock.Set(start.Add(tc.ends))
			if _, granted := m.TryLock("k", other, Lease{TTL: 15 * time.Second, NotAfter: start.Add(time.Hour)}); !granted {
				t.Fatal("an expired lease must not block a new holder")
			}
			if _, ok := m.bySession[lk.Session()]; ok {
				t.Fatal("expired session should be gone from the index")
			}
			if _, ok := m.leases[lk.Session()]; ok {
				t.Fatal("expired lease should be forgotten")
			}
		})
	}
}

func TestManagerScopedRenewalExtendsOnlyTheRenewedKey(t *testing.T) {
	m, clock, start := newScopedTestManager()
	lease := Lease{TTL: 15 * time.Second, NotAfter: start.Add(time.Hour)}
	ss := Session{Principal: "a", Sid: 1}
	m.TryLock("a", Range{Start: 0, End: 9, Type: Write, Sid: 1, Owner: 1, Principal: "a"}, lease)
	m.TryLock("b", Range{Start: 0, End: 9, Type: Write, Sid: 1, Owner: 1, Principal: "a"}, lease)

	clock.Set(start.Add(10 * time.Second))
	m.Renew("a", ss, lease)
	clock.Set(start.Add(15 * time.Second))
	other := Range{Start: 0, End: 9, Type: Write, Sid: 2, Owner: 1, Principal: "b"}
	if _, found := m.GetLk("a", other); !found {
		t.Fatal("renewed key a must still be held")
	}
	if _, found := m.GetLk("b", other); found {
		t.Fatal("key b was not renewed and must have expired")
	}
	if m.leases[ss]["b"] != (time.Time{}) || m.bySession[ss]["b"] {
		t.Fatal("key b's lease and index entry should be gone")
	}
	clock.Set(start.Add(25 * time.Second))
	if _, found := m.GetLk("a", other); found {
		t.Fatal("key a's renewed lease ends 15s after the renewal")
	}
}

func TestManagerScopedRenewWithoutLocksLeasesNothing(t *testing.T) {
	m, _, start := newScopedTestManager()
	m.Renew("k", Session{Principal: "a", Sid: 1}, Lease{TTL: 15 * time.Second, NotAfter: start.Add(time.Hour)})
	if len(m.leases) != 0 || len(m.lastSeen) != 0 {
		t.Fatalf("a renewal of nothing must record nothing: leases=%v lastSeen=%v", m.leases, m.lastSeen)
	}
}

func TestManagerReapExpiredDropsScopedLeasesAndKeepsUnleasedUnscopedLocks(t *testing.T) {
	m, clock, start := newScopedTestManager()
	lease := Lease{TTL: 15 * time.Second, NotAfter: start.Add(time.Hour)}
	m.TryLock("scoped", Range{Start: 0, End: 9, Type: Write, Sid: 1, Owner: 1, Principal: "a"}, lease)
	m.TryLock("unscoped", Range{Start: 0, End: 9, Type: Write, Sid: 1, Owner: 1}, lease)

	clock.Set(start.Add(time.Hour))
	if reaped := m.ReapExpired(15 * time.Second); len(reaped) != 1 || reaped[0] != 1 {
		t.Fatalf("only the scoped lease should be reaped, got %v", reaped)
	}
	held := m.Snapshot()
	if len(held["scoped"]) != 0 || len(held["unscoped"]) != 1 {
		t.Fatalf("sweep should drop the scoped lock and keep the never-renewed unscoped one: %+v", held)
	}
	if len(m.leases) != 0 {
		t.Fatalf("reaped leases should be forgotten: %v", m.leases)
	}
	if !m.bySession[Session{Sid: 1}]["unscoped"] {
		t.Fatal("the unscoped session's index must survive")
	}
}

func TestManagerExpiredScopedSessionWithManyRangesIsDroppedWhole(t *testing.T) {
	m, clock, start := newScopedTestManager()
	lease := Lease{TTL: 15 * time.Second, NotAfter: start.Add(time.Hour)}
	keep := Range{Start: 0, End: 9, Type: Read, Sid: 2, Owner: 2, Principal: "b"}
	if _, granted := m.TryLock("k", keep, lease); !granted {
		t.Fatal("unexpired holder should be granted")
	}
	// Disjoint ranges that never coalesce, interleaved with another session's.
	for i := uint64(1); i <= 50; i++ {
		lk := Range{Start: i * 100, End: i*100 + 9, Type: Write, Sid: 1, Owner: 1, Principal: "a"}
		if _, granted := m.TryLock("k", lk, lease); !granted {
			t.Fatalf("range %d should be granted", i)
		}
	}
	clock.Set(start.Add(10 * time.Second))
	m.Renew("k", keep.Session(), lease)
	clock.Set(start.Add(16 * time.Second))

	other := Range{Start: 100, End: 5009, Type: Write, Sid: 3, Owner: 3, Principal: "c"}
	if c, granted := m.TryLock("k", other, lease); !granted {
		t.Fatalf("expired session's ranges should be gone, conflict %+v", c)
	}
	expired := Session{Principal: "a", Sid: 1}
	if _, indexed := m.bySession[expired]; indexed {
		t.Fatal("expired session is still indexed")
	}
	if got := m.Snapshot()["k"]; len(got) != 2 || got[0] != keep {
		t.Fatalf("unexpired holder or new grant missing: %+v", got)
	}
}

func TestManagerSeveralExpiredScopedSessionsAreDroppedTogether(t *testing.T) {
	m, clock, start := newScopedTestManager()
	lease := Lease{TTL: 15 * time.Second, NotAfter: start.Add(time.Hour)}
	keep := Range{Start: 0, End: 9, Type: Read, Sid: 9, Owner: 9, Principal: "keep"}
	m.TryLock("k", keep, lease)
	for i := uint64(1); i <= 20; i++ {
		principal := []string{"a", "b", "c"}[i%3]
		lk := Range{Start: i * 100, End: i*100 + 9, Type: Read, Sid: i % 3, Owner: 1, Principal: principal}
		if _, granted := m.TryLock("k", lk, lease); !granted {
			t.Fatalf("range %d should be granted", i)
		}
	}
	clock.Set(start.Add(10 * time.Second))
	m.Renew("k", keep.Session(), lease)
	clock.Set(start.Add(16 * time.Second))

	if got := m.Snapshot()["k"]; len(got) != 21 {
		t.Fatalf("snapshot should not reap, got %d ranges", len(got))
	}
	if _, found := m.GetLk("k", Range{Start: 100, End: 2009, Type: Write, Sid: 5, Owner: 5, Principal: "w"}); found {
		t.Fatal("ranges of the expired sessions are still held")
	}
	for _, p := range []string{"a", "b", "c"} {
		for sid := uint64(0); sid < 3; sid++ {
			if _, indexed := m.bySession[Session{Principal: p, Sid: sid}]; indexed {
				t.Fatalf("expired session %s/%d is still indexed", p, sid)
			}
		}
	}
	if got := m.Snapshot()["k"]; len(got) != 1 || got[0] != keep {
		t.Fatalf("only the renewed holder should remain: %+v", got)
	}
}
