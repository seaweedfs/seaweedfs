package wdclient

import (
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
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
