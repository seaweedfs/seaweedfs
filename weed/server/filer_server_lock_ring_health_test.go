package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestLockRingHealthy(t *testing.T) {
	self := pb.ServerAddress("filer-0:8888")
	dlm := lock_manager.NewDistributedLockManager(self)
	fs := &FilerServer{filer: &filer.Filer{Dlm: dlm}}

	// Empty ring, within grace period: healthy.
	if !fs.lockRingHealthy() {
		t.Fatal("expected healthy during grace period")
	}

	// Empty ring past the grace period: unhealthy.
	fs.lockRingUnhealthySince.Store(time.Now().Add(-lockRingUnhealthyGracePeriod - time.Second).UnixNano())
	if fs.lockRingHealthy() {
		t.Fatal("expected unhealthy after grace period with empty ring")
	}

	// Ring without self past the grace period: still unhealthy.
	dlm.LockRing.SetSnapshot([]pb.ServerAddress{"filer-1:8888"}, 1)
	if fs.lockRingHealthy() {
		t.Fatal("expected unhealthy when self is not in ring")
	}

	// Self back in ring: healthy and timer reset.
	dlm.LockRing.SetSnapshot([]pb.ServerAddress{self}, 2)
	if !fs.lockRingHealthy() {
		t.Fatal("expected healthy when self is in ring")
	}
	if fs.lockRingUnhealthySince.Load() != 0 {
		t.Fatal("expected unhealthy timer to reset")
	}
}
