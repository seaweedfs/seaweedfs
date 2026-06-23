package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// The gateway must resolve a lock key to the same primary the filers do,
// otherwise it dials the wrong filer and the lock still gets forwarded. Both
// sides use the same HashRing over the same server set, so for every key the
// client's hostForKey must equal the filer ring's GetPrimary.
func TestLockClientHostMatchesFilerRing(t *testing.T) {
	servers := []pb.ServerAddress{
		"filer-a:8888", "filer-b:8888", "filer-c:8888", "filer-d:8888",
	}

	filerRing := lock_manager.NewHashRing(lock_manager.DefaultVnodeCount)
	filerRing.SetServers(servers)

	lc := NewLockClient(nil, "seed:8888")
	lc.SetRing(servers, 1)

	for _, key := range []string{
		"s3.object.write:/buckets/b/obj-0",
		"s3.object.write:/buckets/b/obj-1",
		"s3.object.write:/buckets/b/obj-2",
		"s3.object.write:/buckets/gosbench-0/w0obj-kilo-0877",
		"some/other/key",
	} {
		if got, want := lc.hostForKey(key), filerRing.GetPrimary(key); got != want {
			t.Errorf("key %q: client host %q != filer primary %q", key, got, want)
		}
	}
}

// Without a ring view, the client falls back to the seed filer (which the filer
// forwards from), preserving the pre-optimization behavior.
func TestLockClientHostFallsBackToSeed(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")
	if got := lc.hostForKey("any-key"); got != "seed:8888" {
		t.Errorf("expected seed fallback, got %q", got)
	}

	// An empty ring (no members yet) also falls back to the seed.
	lc.SetRing(nil, 1)
	if got := lc.hostForKey("any-key"); got != "seed:8888" {
		t.Errorf("expected seed fallback on empty ring, got %q", got)
	}
}

// A stale (older-version) update must not regress a newer ring view, while
// version 0 always applies as a bootstrap.
func TestLockClientSetRingVersionGuard(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")

	newer := []pb.ServerAddress{"filer-a:8888", "filer-b:8888"}
	lc.SetRing(newer, 10)
	primaryAt10 := lc.hostForKey("k")

	// Older version is ignored.
	lc.SetRing([]pb.ServerAddress{"filer-z:8888"}, 5)
	if got := lc.hostForKey("k"); got != primaryAt10 {
		t.Errorf("stale update applied: host changed to %q", got)
	}

	// version 0 is always accepted.
	lc.SetRing([]pb.ServerAddress{"filer-z:8888"}, 0)
	if got := lc.hostForKey("k"); got != "filer-z:8888" {
		t.Errorf("bootstrap update not applied, got %q", got)
	}
}

// PrimaryForKey returns "" before any ring is received (so a route-by-key
// caller falls back to the distributed lock) and the ring owner afterwards,
// unlike hostForKey which falls back to the seed.
func TestLockClientPrimaryForKey(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")
	if got := lc.PrimaryForKey("k"); got != "" {
		t.Errorf("expected empty before ring, got %q", got)
	}

	lc.SetRing([]pb.ServerAddress{"filer-a:8888", "filer-b:8888"}, 1)
	got := lc.PrimaryForKey("k")
	if got == "" {
		t.Fatal("expected an owner after ring set")
	}
	if got != lc.hostForKey("k") {
		t.Errorf("PrimaryForKey %q disagrees with hostForKey %q", got, lc.hostForKey("k"))
	}
}

// A moved key reports its previous owner within the cooling-off window; an unmoved
// key reports none.
func TestLockClientPriorOwnerForKey(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")

	setA := []pb.ServerAddress{"filer-a:8888", "filer-b:8888", "filer-c:8888"}
	lc.SetRing(setA, 1)
	// One ring: nothing to fall back to.
	if got := lc.PriorOwnerForKey("any"); got != "" {
		t.Fatalf("single ring should have no prior owner, got %q", got)
	}

	priorRing := lock_manager.NewHashRing(lock_manager.DefaultVnodeCount)
	priorRing.SetServers(setA)

	// Add a server so some keys' ownership moves.
	setB := []pb.ServerAddress{"filer-a:8888", "filer-b:8888", "filer-c:8888", "filer-d:8888"}
	lc.SetRing(setB, 2)

	var moved, stable string
	for i := 0; i < 2000 && (moved == "" || stable == ""); i++ {
		key := fmt.Sprintf("key-%d", i)
		if lc.PrimaryForKey(key) != priorRing.GetPrimary(key) {
			if moved == "" {
				moved = key
			}
		} else if stable == "" {
			stable = key
		}
	}
	if moved == "" || stable == "" {
		t.Skip("could not find both a moved and a stable key")
	}

	if got, want := lc.PriorOwnerForKey(moved), priorRing.GetPrimary(moved); got != want {
		t.Fatalf("PriorOwnerForKey(moved)=%q, want %q", got, want)
	}
	if got := lc.PriorOwnerForKey(stable); got != "" {
		t.Fatalf("unmoved key should have no prior owner, got %q", got)
	}
}

// The prior owner is only offered within the cooling-off window.
func TestLockClientPriorOwnerForKeyExpires(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")
	lc.priorWindow = 20 * time.Millisecond

	lc.SetRing([]pb.ServerAddress{"filer-a:8888", "filer-b:8888", "filer-c:8888"}, 1)
	lc.SetRing([]pb.ServerAddress{"filer-a:8888", "filer-b:8888", "filer-c:8888", "filer-d:8888"}, 2)

	time.Sleep(40 * time.Millisecond)
	for i := 0; i < 2000; i++ {
		if got := lc.PriorOwnerForKey(fmt.Sprintf("key-%d", i)); got != "" {
			t.Fatalf("prior owner should expire after the cooling window, got %q", got)
		}
	}
}
