package weed_server

import (
	"context"
	"errors"
	"fmt"
	"testing"

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
