package weed_server

import (
	"slices"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestLockRingHealthy(t *testing.T) {
	self := pb.ServerAddress("filer-0:8888")
	dlm := lock_manager.NewDistributedLockManager(self)
	fs := &FilerServer{filer: &filer.Filer{Dlm: dlm}, option: &FilerOption{}}

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

func TestLockRingHealthyWaitsForAnnounce(t *testing.T) {
	dlm := lock_manager.NewDistributedLockManager("filer-0:8888")
	announce := make(chan struct{})
	fs := &FilerServer{filer: &filer.Filer{Dlm: dlm}, option: &FilerOption{AnnounceCh: announce}}
	expired := time.Now().Add(-lockRingUnhealthyGracePeriod - time.Second).UnixNano()

	if !fs.lockRingHealthy() || fs.lockRingUnhealthySince.Load() != 0 {
		t.Fatal("expected healthy with no absence clock while registration is held")
	}
	fs.lockRingUnhealthySince.Store(expired)
	if !fs.lockRingHealthy() {
		t.Fatal("expected healthy while registration is held")
	}

	close(announce)
	fs.lockRingUnhealthySince.Store(0)
	if !fs.lockRingHealthy() || fs.lockRingUnhealthySince.Load() == 0 {
		t.Fatal("expected the first probe after announcing to start the grace period")
	}
	fs.lockRingUnhealthySince.Store(expired)
	if fs.lockRingHealthy() {
		t.Fatal("expected unhealthy after announcing with an empty ring")
	}
}

func TestLockRingHealthyRestartsGraceAfterUnprobedRejoin(t *testing.T) {
	self := pb.ServerAddress("filer-0:8888")
	dlm := lock_manager.NewDistributedLockManager(self)
	fs := &FilerServer{filer: &filer.Filer{Dlm: dlm}, option: &FilerOption{Host: self}}
	dlm.LockRing.SetTakeSnapshotCallback(fs.OnDlmChangeSnapshot)

	fs.lockRingUnhealthySince.Store(time.Now().Add(-lockRingUnhealthyGracePeriod - time.Second).UnixNano())
	dlm.LockRing.SetSnapshot([]pb.ServerAddress{self, "filer-1:8888"}, 1)
	dlm.LockRing.SetSnapshot([]pb.ServerAddress{"filer-1:8888"}, 2)
	if !fs.lockRingHealthy() {
		t.Fatal("expected a fresh grace period after rejoining the ring between probes")
	}
}

func TestLockRingAbsenceRingUpdatesDuringProbe(t *testing.T) {
	self := pb.ServerAddress("filer-0:8888")
	other := pb.ServerAddress("filer-1:8888")
	dlm := lock_manager.NewDistributedLockManager(self)
	fs := &FilerServer{filer: &filer.Filer{Dlm: dlm}, option: &FilerOption{Host: self}}
	dlm.LockRing.SetTakeSnapshotCallback(fs.OnDlmChangeSnapshot)
	var version int64
	install := func(servers ...pb.ServerAddress) {
		version++
		dlm.LockRing.SetSnapshot(servers, version)
	}
	probe := func(afterFirstRead, beforeSecondRead func()) time.Duration {
		reads := 0
		return fs.lockRingAbsence(func() bool {
			reads++
			if reads == 2 && beforeSecondRead != nil {
				beforeSecondRead()
			}
			in := slices.Contains(dlm.LockRing.Ring.GetServers(), self)
			if reads == 1 && afterFirstRead != nil {
				afterFirstRead()
			}
			return in
		})
	}
	install(other)

	fs.lockRingUnhealthySince.Store(time.Now().Add(-lockRingUnhealthyGracePeriod - time.Second).UnixNano())
	if absent := probe(func() { install(self, other) }, nil); absent != 0 || fs.lockRingUnhealthySince.Load() != 0 {
		t.Fatalf("rejoin after the first read: absent %v, clock %d; want 0, 0", absent, fs.lockRingUnhealthySince.Load())
	}

	install(other)
	fs.lockRingUnhealthySince.Store(time.Now().Add(-lockRingUnhealthyGracePeriod - time.Second).UnixNano())
	if absent := probe(nil, func() { install(self, other); install(other) }); absent >= lockRingUnhealthyGracePeriod {
		t.Fatalf("rejoin and removal between the reads: absent %v, want a fresh grace period", absent)
	}
}
