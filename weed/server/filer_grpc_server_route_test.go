package weed_server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// routeTestServer builds a filer whose ring holds the given snapshots, newest
// last. Host is deliberately outside the ring in the forwarding tests so no key
// is ever owned locally.
func routeTestServer(host pb.ServerAddress, snapshots ...[]pb.ServerAddress) *FilerServer {
	dlm := lock_manager.NewDistributedLockManager(host)
	for i, servers := range snapshots {
		dlm.LockRing.SetSnapshot(servers, int64(i+1))
	}
	return &FilerServer{
		filer:  &filer.Filer{Dlm: dlm},
		option: &FilerOption{Host: host},
	}
}

var (
	routeSetA = []pb.ServerAddress{"f1:8888", "f2:8888", "f3:8888"}
	routeSetB = []pb.ServerAddress{"f1:8888", "f2:8888", "f3:8888", "f4:8888"}
)

// firstKey returns the first synthetic path the ring answers match for.
func firstKey(t *testing.T, match func(key string) bool) string {
	t.Helper()
	for i := 0; i < 4000; i++ {
		key := fmt.Sprintf("/buckets/b/key-%d", i)
		if match(key) {
			return key
		}
	}
	t.Skip("no key satisfying the ring condition")
	return ""
}

func TestWriteOwnerNoRing(t *testing.T) {
	fs := routeTestServer("f9:8888")
	if got := fs.writeOwner("/any"); got != "" {
		t.Fatalf("no ring snapshot must leave the write local, got %v", got)
	}
}

func TestWriteOwnerStableKeyUsesPrimary(t *testing.T) {
	fs := routeTestServer("f9:8888", routeSetA, routeSetB)
	ring := fs.filer.Dlm.LockRing

	stable := firstKey(t, func(key string) bool { return ring.PriorOwner(key) == "" })
	if got := fs.writeOwner(stable); got != ring.GetPrimary(stable) {
		t.Fatalf("stable key must route to the primary, got %v want %v", got, ring.GetPrimary(stable))
	}
}

// A key whose ownership just moved must keep going to the prior owner: the new
// owner has not rebuilt the locks the prior one still holds.
func TestWriteOwnerMovedKeyUsesPriorOwner(t *testing.T) {
	fs := routeTestServer("f9:8888", routeSetA, routeSetB)
	ring := fs.filer.Dlm.LockRing

	moved := firstKey(t, func(key string) bool { return ring.PriorOwner(key) != "" })
	if got := fs.writeOwner(moved); got != ring.PriorOwner(moved) {
		t.Fatalf("moved key must route to the prior owner, got %v want %v", got, ring.PriorOwner(moved))
	}
}

func TestForwardToWriteOwnerAppliesLocallyWhenOwned(t *testing.T) {
	fs := routeTestServer("f2:8888", routeSetA)
	ring := fs.filer.Dlm.LockRing

	key := firstKey(t, func(key string) bool { return ring.GetPrimary(key) == "f2:8888" })
	handled, err := fs.forwardToWriteOwner(context.Background(), key, func(pb.ServerAddress) error {
		t.Fatal("must not forward a key this filer owns")
		return nil
	})
	if handled || err != nil {
		t.Fatalf("owned key must be applied locally, got handled=%v err=%v", handled, err)
	}
}

// A failed forward must surface, never re-send to a second filer: gRPC cannot
// tell a lost response from an unsent request, and an owner unreachable from
// here may be partitioned rather than down.
func TestForwardToWriteOwnerNeverTriesASecondFiler(t *testing.T) {
	fs := routeTestServer("f9:8888", routeSetA, routeSetB)
	ring := fs.filer.Dlm.LockRing

	moved := firstKey(t, func(key string) bool { return ring.PriorOwner(key) != "" })
	var tried []pb.ServerAddress
	handled, err := fs.forwardToWriteOwner(context.Background(), moved, func(owner pb.ServerAddress) error {
		tried = append(tried, owner)
		return errors.New("dial tcp: connection refused")
	})
	if !handled || err == nil {
		t.Fatalf("unreachable owner must surface an error, got handled=%v err=%v", handled, err)
	}
	if len(tried) != 1 || tried[0] != ring.PriorOwner(moved) {
		t.Fatalf("expected exactly the prior owner, tried %v", tried)
	}
}

// windowlessRouteServer is routeTestServer with no cooling-off window, so a
// ring change moves keys at once and the latest window has already closed.
func windowlessRouteServer(host pb.ServerAddress, servers []pb.ServerAddress) *FilerServer {
	fs := routeTestServer(host)
	fs.filer.Dlm.LockRing = lock_manager.NewLockRing(0)
	fs.filer.Dlm.LockRing.SetSnapshot(servers, 1)
	return fs
}

// A refused forward is not applied locally once this filer's ring names
// itself: the caller checked the entry before routing, and the wait made that
// check stale, so the refusal surfaces for the caller to retry.
func TestForwardToWriteOwnerSurfacesARefusalOnceOwned(t *testing.T) {
	fs := windowlessRouteServer("f2:8888", routeSetA)
	ring := fs.filer.Dlm.LockRing

	key := firstKey(t, func(key string) bool { return ring.GetPrimary(key) != "f2:8888" })
	var tried []pb.ServerAddress
	handled, err := fs.forwardToWriteOwner(context.Background(), key, func(owner pb.ServerAddress) error {
		tried = append(tried, owner)
		ring.SetSnapshot([]pb.ServerAddress{"f2:8888"}, 2)
		return notWriteOwnerError(key, "f2:8888")
	})
	if !handled || !isNotWriteOwner(err) {
		t.Fatalf("a refused forward must not fall back to a local apply, got handled=%v err=%v", handled, err)
	}
	if len(tried) != 1 {
		t.Fatalf("expected one forward before the ring named this filer, tried %v", tried)
	}
}

func TestForwardToWriteOwnerRetriesARefusal(t *testing.T) {
	fs := windowlessRouteServer("f9:8888", routeSetA)
	ring := fs.filer.Dlm.LockRing

	key := firstKey(t, func(string) bool { return true })
	var tried []pb.ServerAddress
	handled, err := fs.forwardToWriteOwner(context.Background(), key, func(owner pb.ServerAddress) error {
		tried = append(tried, owner)
		if len(tried) < 3 {
			return notWriteOwnerError(key, "f9:8888")
		}
		return nil
	})
	if !handled || err != nil {
		t.Fatalf("the owner accepting a retried forward must succeed, got handled=%v err=%v", handled, err)
	}
	if len(tried) != 3 || tried[0] != ring.GetPrimary(key) || tried[2] != ring.GetPrimary(key) {
		t.Fatalf("expected three forwards to %s, tried %v", ring.GetPrimary(key), tried)
	}
}

// Rings that stay apart (a filer cut off from the master) must not hold the
// write forever: the refusal surfaces once the retry budget is spent.
func TestForwardToWriteOwnerSurfacesAPersistentRefusal(t *testing.T) {
	wait, slack := notWriteOwnerRetryWait, notWriteOwnerRetrySlack
	notWriteOwnerRetryWait, notWriteOwnerRetrySlack = 5*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { notWriteOwnerRetryWait, notWriteOwnerRetrySlack = wait, slack })
	fs := windowlessRouteServer("f9:8888", routeSetA)

	key := firstKey(t, func(string) bool { return true })
	tries := 0
	start := time.Now()
	handled, err := fs.forwardToWriteOwner(context.Background(), key, func(pb.ServerAddress) error {
		tries++
		return notWriteOwnerError(key, "f9:8888")
	})
	if !handled || !isNotWriteOwner(err) {
		t.Fatalf("a persistent refusal must surface, got handled=%v err=%v", handled, err)
	}
	if tries < 2 {
		t.Fatalf("expected the refusal to be retried, tried %d times", tries)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("retries outlived their budget: %v", elapsed)
	}
}

// A ring change that arrives while a refused forward is being retried starts a
// new cooling-off window, and the retries wait for that one too.
func TestForwardToWriteOwnerWaitsOutANewWindow(t *testing.T) {
	wait, slack := notWriteOwnerRetryWait, notWriteOwnerRetrySlack
	notWriteOwnerRetryWait, notWriteOwnerRetrySlack = 5*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { notWriteOwnerRetryWait, notWriteOwnerRetrySlack = wait, slack })
	const window = 100 * time.Millisecond
	fs := routeTestServer("f9:8888")
	ring := lock_manager.NewLockRing(window)
	ring.SetSnapshot(routeSetA, 1)
	fs.filer.Dlm.LockRing = ring
	time.Sleep(window + 10*time.Millisecond)

	key := firstKey(t, func(string) bool { return true })
	tries := 0
	start := time.Now()
	handled, err := fs.forwardToWriteOwner(context.Background(), key, func(pb.ServerAddress) error {
		tries++
		if tries == 2 {
			ring.SetSnapshot(routeSetB, 2)
		}
		if time.Since(start) < window {
			return notWriteOwnerError(key, "f9:8888")
		}
		return nil
	})
	if !handled || err != nil {
		t.Fatalf("a refusal must be retried through a window that opened during the retries, got handled=%v err=%v after %d tries", handled, err, tries)
	}
}
