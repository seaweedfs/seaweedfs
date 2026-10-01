package wdclient

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func newTestFilerClient(addrs ...pb.ServerAddress) *FilerClient {
	health := make([]*filerHealth, len(addrs))
	for i := range health {
		health[i] = &filerHealth{}
	}
	return &FilerClient{
		filerAddresses: addrs,
		filerHealth:    health,
	}
}

func filerAddressList(fc *FilerClient) []pb.ServerAddress {
	out := make([]pb.ServerAddress, len(fc.filerAddresses))
	copy(out, fc.filerAddresses)
	return out
}

func TestApplyDiscoveredFilersPrunesStaleAddress(t *testing.T) {
	a := pb.ServerAddress("10.0.0.1:18888")
	b := pb.ServerAddress("10.0.0.2:18888") // gets replaced by c
	c := pb.ServerAddress("10.0.0.3:18888")

	fc := newTestFilerClient(a, b)
	// Give b a non-trivial failure count so we can confirm it leaves with its health.
	atomic.StoreInt32(&fc.filerHealth[1].failureCount, 7)
	// Give a a known failure count so we can confirm survivor health is preserved.
	atomic.StoreInt32(&fc.filerHealth[0].failureCount, 2)
	atomic.StoreInt32(&fc.filerIndex, 1) // active filer is b

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{
		a: {},
		c: {},
	})

	got := filerAddressList(fc)
	if len(got) != 2 || got[0] != a || got[1] != c {
		t.Fatalf("expected [%s %s], got %v", a, c, got)
	}
	if len(fc.filerHealth) != 2 {
		t.Fatalf("expected 2 health entries, got %d", len(fc.filerHealth))
	}
	if got := atomic.LoadInt32(&fc.filerHealth[0].failureCount); got != 2 {
		t.Errorf("survivor health was reset: want 2, got %d", got)
	}
	if got := atomic.LoadInt32(&fc.filerHealth[1].failureCount); got != 0 {
		t.Errorf("newly added filer should start with fresh health, got failureCount=%d", got)
	}
	// Active filer (b) disappeared; index must reset rather than dangle.
	if idx := atomic.LoadInt32(&fc.filerIndex); idx != 0 {
		t.Errorf("expected filerIndex reset to 0 after active filer removed, got %d", idx)
	}
}

func TestApplyDiscoveredFilersKeepsIndexOnSurvivor(t *testing.T) {
	a := pb.ServerAddress("10.0.0.1:18888") // gets replaced
	b := pb.ServerAddress("10.0.0.2:18888") // active, survives
	c := pb.ServerAddress("10.0.0.3:18888") // new

	fc := newTestFilerClient(a, b)
	atomic.StoreInt32(&fc.filerIndex, 1) // active filer is b

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{
		b: {},
		c: {},
	})

	got := filerAddressList(fc)
	if len(got) != 2 || got[0] != b || got[1] != c {
		t.Fatalf("expected [%s %s], got %v", b, c, got)
	}
	// b moved to index 0 after a was pruned; index should follow.
	if idx := atomic.LoadInt32(&fc.filerIndex); idx != 0 {
		t.Errorf("expected filerIndex to follow surviving active filer to position 0, got %d", idx)
	}
}

func TestApplyDiscoveredFilersNoChangeIsNoop(t *testing.T) {
	a := pb.ServerAddress("10.0.0.1:18888")
	b := pb.ServerAddress("10.0.0.2:18888")

	fc := newTestFilerClient(a, b)
	originalHealthA := fc.filerHealth[0]
	originalHealthB := fc.filerHealth[1]

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{
		a: {},
		b: {},
	})

	if fc.filerHealth[0] != originalHealthA || fc.filerHealth[1] != originalHealthB {
		t.Errorf("no-op refresh should not reallocate health entries")
	}
}

func filerUpdate(addr pb.ServerAddress, isAdd bool) *master_pb.ClusterNodeUpdate {
	return &master_pb.ClusterNodeUpdate{NodeType: cluster.FilerType, Address: string(addr), IsAdd: isAdd}
}

// A rolling restart replaces every filer within one discovery interval; the
// pushed updates must keep the list live without waiting for the next poll.
func TestOnPeerUpdateTracksRollingReplacement(t *testing.T) {
	old1 := pb.ServerAddress("10.0.0.1:8888")
	old2 := pb.ServerAddress("10.0.0.2:8888")
	new1 := pb.ServerAddress("10.0.1.1:8888")
	new2 := pb.ServerAddress("10.0.1.2:8888")

	fc := newTestFilerClient(old1, old2)

	fc.OnPeerUpdate(filerUpdate(old1, false), time.Now())
	fc.OnPeerUpdate(filerUpdate(new1, true), time.Now())
	fc.OnPeerUpdate(filerUpdate(old2, false), time.Now())
	fc.OnPeerUpdate(filerUpdate(new2, true), time.Now())

	got := filerAddressList(fc)
	if len(got) != 2 || got[0] != new1 || got[1] != new2 {
		t.Fatalf("expected [%s %s], got %v", new1, new2, got)
	}
}

func TestOnPeerUpdateKeepsLastFiler(t *testing.T) {
	only := pb.ServerAddress("10.0.0.1:8888")
	fc := newTestFilerClient(only)

	fc.OnPeerUpdate(filerUpdate(only, false), time.Now())

	if got := filerAddressList(fc); len(got) != 1 || got[0] != only {
		t.Fatalf("removing the last filer must keep it, got %v", got)
	}
}

func TestOnPeerUpdateIgnoresOtherNodeTypes(t *testing.T) {
	a := pb.ServerAddress("10.0.0.1:8888")
	fc := newTestFilerClient(a)

	fc.OnPeerUpdate(&master_pb.ClusterNodeUpdate{NodeType: cluster.S3Type, Address: "10.0.0.9:18333", IsAdd: true}, time.Now())

	if got := filerAddressList(fc); len(got) != 1 || got[0] != a {
		t.Fatalf("non-filer update changed the list: %v", got)
	}
}

func TestOnPeerUpdateDuplicateAddKeepsHealth(t *testing.T) {
	a := pb.ServerAddress("10.0.0.1:8888")
	fc := newTestFilerClient(a)
	atomic.StoreInt32(&fc.filerHealth[0].failureCount, 2)

	fc.OnPeerUpdate(filerUpdate(a, true), time.Now())

	if len(fc.filerHealth) != 1 || atomic.LoadInt32(&fc.filerHealth[0].failureCount) != 2 {
		t.Fatalf("re-announced filer lost its health state")
	}
}

func TestDiscoverySnapshotTakenBeforePushIsDiscarded(t *testing.T) {
	old := pb.ServerAddress("10.0.0.1:8888")
	joined := pb.ServerAddress("10.0.1.1:8888")
	fc := newTestFilerClient(old)

	generation := fc.peerUpdateGeneration()
	fc.OnPeerUpdate(filerUpdate(joined, true), time.Now())
	fc.OnPeerUpdate(filerUpdate(old, false), time.Now())
	fc.applyDiscoverySnapshot(map[pb.ServerAddress]struct{}{old: {}}, generation)

	if got := filerAddressList(fc); len(got) != 1 || got[0] != joined {
		t.Fatalf("stale snapshot overwrote pushed membership: %v", got)
	}
}

func TestDiscoverySnapshotWithoutInterveningPushIsApplied(t *testing.T) {
	old := pb.ServerAddress("10.0.0.1:8888")
	replacement := pb.ServerAddress("10.0.1.1:8888")
	fc := newTestFilerClient(old)

	fc.applyDiscoverySnapshot(map[pb.ServerAddress]struct{}{replacement: {}}, fc.peerUpdateGeneration())

	if got := filerAddressList(fc); len(got) != 1 || got[0] != replacement {
		t.Fatalf("expected snapshot to replace list, got %v", got)
	}
}

// A leave suppressed to keep the last filer is deferred, then honored as soon
// as a replacement joins, so the departed address stops being a candidate.
func TestOnPeerUpdateDeferredLeaveFlushesOnJoin(t *testing.T) {
	old := pb.ServerAddress("10.0.0.1:8888")
	joined := pb.ServerAddress("10.0.1.1:8888")
	fc := newTestFilerClient(old)

	fc.OnPeerUpdate(filerUpdate(old, false), time.Now())
	if got := filerAddressList(fc); len(got) != 1 || got[0] != old {
		t.Fatalf("leave of the last filer must be deferred, got %v", got)
	}
	if len(fc.deferredLeaves) != 1 {
		t.Fatalf("expected the leave to be deferred, got %v", fc.deferredLeaves)
	}

	fc.OnPeerUpdate(filerUpdate(joined, true), time.Now())

	if got := filerAddressList(fc); len(got) != 1 || got[0] != joined {
		t.Fatalf("deferred leave should flush on join, got %v", got)
	}
	if len(fc.deferredLeaves) != 0 {
		t.Fatalf("deferred leaves should be empty after flush, got %v", fc.deferredLeaves)
	}
}

// A pushed add for an already-known filer changes nothing and must not bump
// the generation that guards an in-flight discovery snapshot.
func TestOnPeerUpdateNoopDoesNotDiscardSnapshot(t *testing.T) {
	old := pb.ServerAddress("10.0.0.1:8888")
	replacement := pb.ServerAddress("10.0.1.1:8888")
	fc := newTestFilerClient(old)

	generation := fc.peerUpdateGeneration()
	fc.OnPeerUpdate(filerUpdate(old, true), time.Now())
	fc.applyDiscoverySnapshot(map[pb.ServerAddress]struct{}{replacement: {}}, generation)

	if got := filerAddressList(fc); len(got) != 1 || got[0] != replacement {
		t.Fatalf("no-op push should not discard the snapshot, got %v", got)
	}
}
