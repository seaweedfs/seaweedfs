package filer

import (
	"sort"
	"testing"
	"time"
)

func testClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestPeerRegistry_RegisterAndList(t *testing.T) {
	start := time.Unix(1000, 0)
	current := start
	r := newPeerRegistryWithClock(func() time.Time { return current })

	r.Register("mount-a:18080", "rack1", 30*time.Second)
	r.Register("mount-b:18080", "rack2", 30*time.Second)

	list := r.List()
	if len(list) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(list))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].PeerAddr < list[j].PeerAddr })
	if list[0].PeerAddr != "mount-a:18080" || list[0].Rack != "rack1" {
		t.Errorf("entry 0 unexpected: %+v", list[0])
	}
	if list[1].PeerAddr != "mount-b:18080" || list[1].Rack != "rack2" {
		t.Errorf("entry 1 unexpected: %+v", list[1])
	}
}

func TestPeerRegistry_RenewExtendsExpiry(t *testing.T) {
	current := time.Unix(1000, 0)
	r := newPeerRegistryWithClock(func() time.Time { return current })

	r.Register("mount-a:18080", "rack1", 30*time.Second)

	// Advance time past original expiry.
	current = current.Add(25 * time.Second)
	// Renew — should push expiry to now+30.
	r.Register("mount-a:18080", "rack1-updated", 30*time.Second)

	// Advance to where original expiry would have triggered eviction.
	current = current.Add(10 * time.Second)
	list := r.List()
	if len(list) != 1 {
		t.Fatalf("expected 1 entry after renew, got %d", len(list))
	}
	if list[0].Rack != "rack1-updated" {
		t.Errorf("rack not updated on renew: %q", list[0].Rack)
	}
}

func TestPeerRegistry_ExpirationDropsEntry(t *testing.T) {
	current := time.Unix(1000, 0)
	r := newPeerRegistryWithClock(func() time.Time { return current })

	r.Register("mount-a:18080", "", 10*time.Second)
	if n := r.Len(); n != 1 {
		t.Fatalf("expected 1 entry, got %d", n)
	}

	current = current.Add(15 * time.Second)
	list := r.List()
	if len(list) != 0 {
		t.Errorf("expected 0 entries after expiry, got %d", len(list))
	}
	if n := r.Len(); n != 0 {
		t.Errorf("List should have swept expired entry; Len=%d", n)
	}
}

func TestPeerRegistry_SweepCountsEvictions(t *testing.T) {
	current := time.Unix(1000, 0)
	r := newPeerRegistryWithClock(func() time.Time { return current })

	r.Register("mount-a:18080", "", 10*time.Second)
	r.Register("mount-b:18080", "", 60*time.Second)

	current = current.Add(30 * time.Second)
	got := r.Sweep()
	if got != 1 {
		t.Errorf("expected 1 eviction, got %d", got)
	}
	if n := r.Len(); n != 1 {
		t.Errorf("expected 1 surviving entry, got %d", n)
	}
}

func TestPeerRegistry_NegativeTTLFallsBackToDefault(t *testing.T) {
	current := time.Unix(1000, 0)
	r := newPeerRegistryWithClock(func() time.Time { return current })

	r.Register("mount-a:18080", "", -5*time.Second)
	if n := r.Len(); n != 1 {
		t.Fatalf("expected 1 entry even with bad ttl, got %d", n)
	}

	// Advance past the default (60s) and confirm it expires.
	current = current.Add(61 * time.Second)
	list := r.List()
	if len(list) != 0 {
		t.Errorf("expected entry to expire after default ttl, got %d entries", len(list))
	}
}

func TestPeerRegistry_EmptyList(t *testing.T) {
	r := NewPeerRegistry()
	list := r.List()
	if len(list) != 0 {
		t.Errorf("expected empty list, got %d", len(list))
	}
	_ = testClock // keep helper exported for future tests
}
