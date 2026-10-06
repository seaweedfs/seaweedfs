package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
)

func newLeaveTestFilerServer(priorOwnerWindow time.Duration, ring ...pb.ServerAddress) *FilerServer {
	const self = pb.ServerAddress("filer1:18888")
	dlm := lock_manager.NewDistributedLockManager(self)
	dlm.LockRing = lock_manager.NewLockRing(priorOwnerWindow)
	dlm.LockRing.SetSnapshot(ring, 1)
	return &FilerServer{
		option: &FilerOption{Host: self},
		filer: &filer.Filer{
			Dlm:          dlm,
			MasterClient: wdclient.NewMasterClient(grpc.EmptyDialOption{}, "", "filer", self, "", "", pb.ServerDiscovery{}),
		},
	}
}

func TestLeaveLockRingWaitsForRemovalThenPriorOwnerWindow(t *testing.T) {
	const window = 300 * time.Millisecond
	fs := newLeaveTestFilerServer(window, "filer1:18888", "filer2:18888")

	removed := make(chan time.Time, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		before := time.Now()
		fs.filer.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{"filer2:18888"}, 2)
		removed <- before
	}()

	fs.leaveLockRing(5 * time.Second)
	returned := time.Now()

	select {
	case removedAt := <-removed:
		if want := window + priorOwnerWindowSkew; returned.Sub(removedAt) < want-20*time.Millisecond {
			t.Errorf("returned %v after the ring dropped this filer, want the %v prior-owner window plus peer skew", returned.Sub(removedAt), want)
		}
	default:
		t.Fatal("returned before the master dropped this filer from the lock ring")
	}
}

func TestLeaveLockRingGivesUpWhenTheMasterKeepsTheFiler(t *testing.T) {
	fs := newLeaveTestFilerServer(5*time.Second, "filer1:18888", "filer2:18888")

	start := time.Now()
	fs.leaveLockRing(200 * time.Millisecond)
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond || elapsed > 2*time.Second {
		t.Errorf("leave took %v, want about the 200ms removal timeout", elapsed)
	}
}

func TestLeaveLockRingSkipsALoneFiler(t *testing.T) {
	fs := newLeaveTestFilerServer(5*time.Second, "filer1:18888")

	start := time.Now()
	fs.leaveLockRing(5 * time.Second)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("a lone filer waited %v to leave a ring with no peer to take its keys", elapsed)
	}
}

func TestLeaveLockRingIsBoundedBySlowLockTransfers(t *testing.T) {
	fs := newLeaveTestFilerServer(5*time.Second, "filer1:18888", "filer2:18888")
	transferring := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	fs.filer.Dlm.LockRing.SetTakeSnapshotCallback(func([]pb.ServerAddress) {
		close(transferring)
		<-release
	})
	go fs.filer.Dlm.LockRing.SetSnapshot([]pb.ServerAddress{"filer2:18888"}, 2)
	<-transferring

	start := time.Now()
	fs.leaveLockRingWithin(5*time.Second, 300*time.Millisecond)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("leave blocked %v behind lock transfers, want the 300ms budget", elapsed)
	}
}
