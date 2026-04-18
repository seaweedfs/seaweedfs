package filer

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func testClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestMountPeerRegistry_RegisterAndList(t *testing.T) {
	start := time.Unix(1000, 0)
	current := start
	r := newMountPeerRegistryWithClock(func() time.Time { return current })

	r.Register("mount-a:18080", "", "rack1", 30*time.Second)
	r.Register("mount-b:18080", "", "rack2", 30*time.Second)

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

func TestMountPeerRegistry_RenewExtendsExpiry(t *testing.T) {
	current := time.Unix(1000, 0)
	r := newMountPeerRegistryWithClock(func() time.Time { return current })

	r.Register("mount-a:18080", "", "rack1", 30*time.Second)

	// Advance time past original expiry.
	current = current.Add(25 * time.Second)
	// Renew — should push expiry to now+30.
	r.Register("mount-a:18080", "", "rack1-updated", 30*time.Second)

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

func TestMountPeerRegistry_ExpirationDropsEntry(t *testing.T) {
	current := time.Unix(1000, 0)
	r := newMountPeerRegistryWithClock(func() time.Time { return current })

	r.Register("mount-a:18080", "", "", 10*time.Second)
	if n := r.Len(); n != 1 {
		t.Fatalf("expected 1 entry, got %d", n)
	}

	current = current.Add(15 * time.Second)
	list := r.List()
	if len(list) != 0 {
		t.Errorf("expected 0 entries after expiry, got %d", len(list))
	}
	// List no longer deletes — it's RLock-only so concurrent callers can
	// proceed in parallel. Sweep is the sole reclamation path.
	if n := r.Len(); n != 1 {
		t.Errorf("List should not delete expired entries; Len=%d want 1", n)
	}
	if evicted := r.Sweep(); evicted != 1 {
		t.Errorf("Sweep should have evicted the expired entry; got %d", evicted)
	}
	if n := r.Len(); n != 0 {
		t.Errorf("after Sweep, Len=%d want 0", n)
	}
}

func TestMountPeerRegistry_SweepCountsEvictions(t *testing.T) {
	current := time.Unix(1000, 0)
	r := newMountPeerRegistryWithClock(func() time.Time { return current })

	r.Register("mount-a:18080", "", "", 10*time.Second)
	r.Register("mount-b:18080", "", "", 60*time.Second)

	current = current.Add(30 * time.Second)
	got := r.Sweep()
	if got != 1 {
		t.Errorf("expected 1 eviction, got %d", got)
	}
	if n := r.Len(); n != 1 {
		t.Errorf("expected 1 surviving entry, got %d", n)
	}
}

func TestMountPeerRegistry_NegativeTTLFallsBackToDefault(t *testing.T) {
	current := time.Unix(1000, 0)
	r := newMountPeerRegistryWithClock(func() time.Time { return current })

	r.Register("mount-a:18080", "", "", -5*time.Second)
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

func TestMountPeerRegistry_EmptyList(t *testing.T) {
	r := NewMountPeerRegistry()
	list := r.List()
	if len(list) != 0 {
		t.Errorf("expected empty list, got %d", len(list))
	}
	_ = testClock // keep helper exported for future tests
}

func TestMountPeerRegistry_EmptyPeerAddrRejected(t *testing.T) {
	r := NewMountPeerRegistry()
	r.Register("", "", "rack", 30*time.Second)
	if n := r.Len(); n != 0 {
		t.Errorf("empty peer_addr should not insert; Len=%d", n)
	}
}

func TestMountPeerRegistry_TTLCapped(t *testing.T) {
	current := time.Unix(1000, 0)
	r := newMountPeerRegistryWithClock(func() time.Time { return current })

	// Request a ridiculous TTL; should be capped to maxMountPeerRegistryTTL.
	r.Register("mount-a:18080", "", "", 24*time.Hour)

	// Advance past the cap; entry should now be expired.
	current = current.Add(maxMountPeerRegistryTTL + time.Second)
	if list := r.List(); len(list) != 0 {
		t.Errorf("expected entry to expire after maxMountPeerRegistryTTL, got %d", len(list))
	}
}

func TestMountPeerRegistry_CapacityLimit(t *testing.T) {
	r := NewMountPeerRegistry()
	// Fill to capacity.
	for i := 0; i < maxMountPeerRegistryEntries; i++ {
		r.Register(fmt.Sprintf("mount-%d:18080", i), "", "", 60*time.Second)
	}
	if n := r.Len(); n != maxMountPeerRegistryEntries {
		t.Fatalf("expected %d entries, got %d", maxMountPeerRegistryEntries, n)
	}
	// A brand-new address beyond the cap is rejected.
	r.Register("new-mount:18080", "", "", 60*time.Second)
	if n := r.Len(); n != maxMountPeerRegistryEntries {
		t.Errorf("new entry past cap should be rejected; Len=%d", n)
	}
	// A renewal of an existing entry still succeeds.
	r.Register("mount-0:18080", "", "rack-renewed", 60*time.Second)
	if n := r.Len(); n != maxMountPeerRegistryEntries {
		t.Errorf("renewal should not increase size; Len=%d", n)
	}
}
