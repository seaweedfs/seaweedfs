package posixlock

import (
	"reflect"
	"testing"
	"time"
)

// A mount's tracked locks round-trip through Snapshot back to a fresh owner via
// Reassert — the owner-restart / ring-change recovery path.
func TestReassertRebuildsOnFreshOwner(t *testing.T) {
	const sid = uint64(7)

	// Client mirror: two granted locks on one key, one on another.
	client := NewManager()
	client.Track("a", Range{Start: 0, End: 99, Type: Write, Sid: sid, Owner: 1})
	client.Track("a", Range{Start: 200, End: 299, Type: Read, Sid: sid, Owner: 2})
	client.Track("b", Range{Start: 0, End: maxEnd, Type: Write, Sid: sid, Owner: 1, IsFlock: true})

	// Fresh owner (post-restart / new ring owner) knows nothing.
	owner := NewManager()
	for key, locks := range client.Snapshot() {
		if c := owner.Reassert(key, sid, locks); c != nil {
			t.Fatalf("unexpected conflict reasserting %s: %+v", key, c)
		}
	}

	// The owner now reports the same conflicts a foreign session would hit.
	if _, granted := owner.TryLock("a", Range{Start: 50, End: 60, Type: Write, Sid: 99, Owner: 1}); granted {
		t.Fatal("owner should block a foreign write after rebuild")
	}
	if _, granted := owner.TryLock("b", Range{Start: 0, End: 0, Type: Read, Sid: 99, Owner: 1, IsFlock: true}); granted {
		t.Fatal("owner should block a foreign flock read after rebuild")
	}
}

// Re-asserting every tick is idempotent: the owner's view is unchanged.
func TestReassertIdempotent(t *testing.T) {
	const sid = uint64(1)
	m := NewManager()
	m.TryLock("k", Range{Start: 0, End: 99, Type: Write, Sid: sid, Owner: 1})
	before := append([]Range(nil), m.byKey["k"].locks...)

	m.Reassert("k", sid, before)
	m.Reassert("k", sid, before)

	if !reflect.DeepEqual(m.byKey["k"].locks, before) {
		t.Fatalf("reassert not idempotent:\n got %+v\nwant %+v", m.byKey["k"].locks, before)
	}
}

// A lock another session grabbed in the migration window is reported as a
// conflict and not double-granted.
func TestReassertReportsConflict(t *testing.T) {
	const mine, other = uint64(1), uint64(2)
	m := NewManager()
	// Another mount took the lock on this (new) owner during the gap.
	m.TryLock("k", Range{Start: 0, End: 99, Type: Write, Sid: other, Owner: 1})

	conflicts := m.Reassert("k", mine, []Range{{Start: 0, End: 99, Type: Write, Sid: mine, Owner: 1}})
	if len(conflicts) != 1 {
		t.Fatalf("expected 1 conflict, got %d: %+v", len(conflicts), conflicts)
	}
	// The other session keeps the lock; mine was not installed.
	if got := len(m.byKey["k"].locks); got != 1 {
		t.Fatalf("expected only the incumbent lock, got %d", got)
	}
}

// Reassert renews the lease, so a re-asserting mount is not reaped.
func TestReassertRenewsLease(t *testing.T) {
	const sid = uint64(1)
	m := NewManager()
	m.Renew(sid)
	m.Reassert("k", sid, []Range{{Start: 0, End: 9, Type: Write, Sid: sid, Owner: 1}})

	if reaped := m.ReapExpired(time.Hour); len(reaped) != 0 {
		t.Fatalf("freshly re-asserted session should not be reaped: %v", reaped)
	}
}

const maxEnd = ^uint64(0)
