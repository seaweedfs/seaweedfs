package cluster

import (
	"testing"

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
