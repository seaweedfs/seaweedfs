package mount

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// alwaysOwner is a trivial OwnerCheck used by tests that aren't exercising
// ownership rejection.
func alwaysOwner(string) bool { return true }

// ownerSet returns an OwnerCheck that accepts only the listed fids.
func ownerSet(fids ...string) OwnerCheck {
	m := map[string]struct{}{}
	for _, f := range fids {
		m[f] = struct{}{}
	}
	return func(fid string) bool {
		_, ok := m[fid]
		return ok
	}
}

func TestPeerDirectory_AnnounceAndLookup(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newPeerDirectoryWithClock(func() time.Time { return now })

	res := d.Announce("mount-a:18080", "", "r1", []string{"3,a", "3,b"}, 60*time.Second, alwaysOwner)
	if len(res.Rejected) != 0 {
		t.Errorf("expected 0 rejected, got %v", res.Rejected)
	}

	lookup := d.Lookup([]string{"3,a", "3,b", "3,c"}, alwaysOwner)
	if got := len(lookup.PeersByFid["3,a"]); got != 1 {
		t.Errorf("3,a: expected 1 holder, got %d", got)
	}
	if got := len(lookup.PeersByFid["3,b"]); got != 1 {
		t.Errorf("3,b: expected 1 holder, got %d", got)
	}
	if got := len(lookup.PeersByFid["3,c"]); got != 0 {
		t.Errorf("3,c (unknown): expected 0 holders, got %d", got)
	}
}

func TestPeerDirectory_OwnerRejection(t *testing.T) {
	d := NewPeerDirectory()

	res := d.Announce("mount-a:18080", "", "", []string{"3,mine", "3,yours"}, time.Minute, ownerSet("3,mine"))
	if len(res.Rejected) != 1 || res.Rejected[0] != "3,yours" {
		t.Errorf("expected 3,yours rejected, got %v", res.Rejected)
	}

	lookup := d.Lookup([]string{"3,mine", "3,yours"}, ownerSet("3,mine"))
	if got := len(lookup.PeersByFid["3,mine"]); got != 1 {
		t.Errorf("3,mine should have 1 holder, got %d", got)
	}
	if len(lookup.NotOwnerFids) != 1 || lookup.NotOwnerFids[0] != "3,yours" {
		t.Errorf("expected 3,yours in not-owner list, got %v", lookup.NotOwnerFids)
	}
}

func TestPeerDirectory_MultipleHolders(t *testing.T) {
	d := NewPeerDirectory()
	d.Announce("mount-a:18080", "", "r1", []string{"3,x"}, time.Minute, alwaysOwner)
	d.Announce("mount-b:18080", "", "r2", []string{"3,x"}, time.Minute, alwaysOwner)

	holders := d.Lookup([]string{"3,x"}, alwaysOwner).PeersByFid["3,x"]
	if len(holders) != 2 {
		t.Fatalf("expected 2 holders, got %d", len(holders))
	}
	sort.Slice(holders, func(i, j int) bool { return holders[i].PeerAddr < holders[j].PeerAddr })
	if holders[0].PeerAddr != "mount-a:18080" || holders[1].PeerAddr != "mount-b:18080" {
		t.Errorf("unexpected holder addrs: %+v", holders)
	}
}

func TestPeerDirectory_RenewExtendsExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newPeerDirectoryWithClock(func() time.Time { return now })

	d.Announce("mount-a:18080", "", "", []string{"3,a"}, 30*time.Second, alwaysOwner)

	now = now.Add(25 * time.Second)
	d.Announce("mount-a:18080", "", "", []string{"3,a"}, 30*time.Second, alwaysOwner)

	now = now.Add(10 * time.Second)
	lookup := d.Lookup([]string{"3,a"}, alwaysOwner)
	if len(lookup.PeersByFid["3,a"]) != 1 {
		t.Errorf("renew should keep entry alive, got %d holders", len(lookup.PeersByFid["3,a"]))
	}
}

func TestPeerDirectory_TTLExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newPeerDirectoryWithClock(func() time.Time { return now })

	d.Announce("mount-a:18080", "", "", []string{"3,a"}, 10*time.Second, alwaysOwner)
	now = now.Add(15 * time.Second)
	holders := d.Lookup([]string{"3,a"}, alwaysOwner).PeersByFid["3,a"]
	if len(holders) != 0 {
		t.Errorf("expected TTL-expired holder to be gone, got %+v", holders)
	}
}

func TestPeerDirectory_Sweep(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newPeerDirectoryWithClock(func() time.Time { return now })

	d.Announce("mount-a:18080", "", "", []string{"3,a"}, 10*time.Second, alwaysOwner)
	d.Announce("mount-b:18080", "", "", []string{"3,b"}, 60*time.Second, alwaysOwner)

	now = now.Add(30 * time.Second)
	got := d.Sweep()
	if got != 1 {
		t.Errorf("expected 1 swept entry, got %d", got)
	}
	_, _, _, _, entries := d.Stats()
	if entries != 1 {
		t.Errorf("expected 1 surviving entry, got %d", entries)
	}
}

// TestPeerDirectory_LookupOrdersByRecency guards the LRU contract: the
// most recently-announced holder comes first in the lookup response, so
// fetchers that try holders in order hit the holder most likely to still
// have the chunk cached locally.
func TestPeerDirectory_LookupOrdersByRecency(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newPeerDirectoryWithClock(func() time.Time { return now })

	d.Announce("mount-a:18080", "", "", []string{"3,x"}, 60*time.Second, alwaysOwner)
	now = now.Add(10 * time.Second)
	d.Announce("mount-b:18080", "", "", []string{"3,x"}, 60*time.Second, alwaysOwner)
	now = now.Add(10 * time.Second)
	d.Announce("mount-c:18080", "", "", []string{"3,x"}, 60*time.Second, alwaysOwner)

	got := d.Lookup([]string{"3,x"}, alwaysOwner).PeersByFid["3,x"]
	if len(got) != 3 {
		t.Fatalf("expected 3 holders, got %d", len(got))
	}
	want := []string{"mount-c:18080", "mount-b:18080", "mount-a:18080"}
	for i, w := range want {
		if got[i].PeerAddr != w {
			t.Errorf("holder[%d] = %q, want %q (LRU order)", i, got[i].PeerAddr, w)
		}
	}
}

// TestPeerDirectory_LookupOrderAfterRenewal ensures renewing an older
// holder promotes it to the head — matches LRU touch-on-access semantics
// and keeps the freshest liveness signal in front.
func TestPeerDirectory_LookupOrderAfterRenewal(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newPeerDirectoryWithClock(func() time.Time { return now })

	d.Announce("mount-a:18080", "", "", []string{"3,x"}, 60*time.Second, alwaysOwner)
	now = now.Add(10 * time.Second)
	d.Announce("mount-b:18080", "", "", []string{"3,x"}, 60*time.Second, alwaysOwner)
	now = now.Add(10 * time.Second)
	d.Announce("mount-a:18080", "", "", []string{"3,x"}, 60*time.Second, alwaysOwner)

	got := d.Lookup([]string{"3,x"}, alwaysOwner).PeersByFid["3,x"]
	if len(got) != 2 {
		t.Fatalf("expected 2 holders, got %d", len(got))
	}
	if got[0].PeerAddr != "mount-a:18080" {
		t.Errorf("after renewal, head should be mount-a; got %q", got[0].PeerAddr)
	}
}

func TestPeerDirectory_StatsCountersIncrement(t *testing.T) {
	d := NewPeerDirectory()
	d.Announce("mount-a:18080", "", "", []string{"3,a", "3,b"}, time.Minute, alwaysOwner)
	d.Lookup([]string{"3,a", "3,c"}, alwaysOwner)
	d.Announce("mount-a:18080", "", "", []string{"3,nope"}, time.Minute, ownerSet("nothing"))

	announces, lookups, rejected, _, _ := d.Stats()
	if announces != 2 {
		t.Errorf("announces counter: %d", announces)
	}
	if lookups != 2 {
		t.Errorf("lookups counter: %d", lookups)
	}
	if rejected != 1 {
		t.Errorf("rejected counter: %d", rejected)
	}
}

// TestPeerDirectory_LookupCapsHolderList verifies that a hot fid with
// more than maxLookupHolders holders returns only the top-N LRU entries.
// The fetcher typically tries only the first 1-3 anyway, so caching
// every holder on the wire is overhead.
func TestPeerDirectory_LookupCapsHolderList(t *testing.T) {
	now := time.Unix(1000, 0)
	d := newPeerDirectoryWithClock(func() time.Time { return now })

	// Announce 32 holders at strictly increasing timestamps so LRU order
	// is well-defined and deterministic.
	total := 2 * maxLookupHolders
	for i := 0; i < total; i++ {
		addr := fmt.Sprintf("mount-%02d:18080", i)
		d.Announce(addr, "", "", []string{"3,hot"}, 60*time.Second, alwaysOwner)
		now = now.Add(time.Millisecond)
	}

	got := d.Lookup([]string{"3,hot"}, alwaysOwner).PeersByFid["3,hot"]
	if len(got) != maxLookupHolders {
		t.Fatalf("expected %d holders in response, got %d", maxLookupHolders, len(got))
	}
	// Head should be the most recent (last announced).
	want := fmt.Sprintf("mount-%02d:18080", total-1)
	if got[0].PeerAddr != want {
		t.Errorf("head of list: got %q want %q (LRU order)", got[0].PeerAddr, want)
	}
}
