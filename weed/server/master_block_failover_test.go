package weed_server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// testMasterServerForFailover creates a MasterServer with replica-aware mocks.
func testMasterServerForFailover(t *testing.T) *MasterServer {
	t.Helper()
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:      fmt.Sprintf("/data/%s.blk", name),
			IQN:       fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: server,
		}, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		return nil
	}
	return ms
}

// registerVolumeWithReplica creates a volume entry with primary + replica for tests.
func registerVolumeWithReplica(t *testing.T, ms *MasterServer, name, primary, replica string, epoch uint64, leaseTTL time.Duration) {
	t.Helper()
	entry := &BlockVolumeEntry{
		Name:             name,
		VolumeServer:     primary,
		Path:             fmt.Sprintf("/data/%s.blk", name),
		IQN:              fmt.Sprintf("iqn.2024.test:%s", name),
		ISCSIAddr:        primary + ":3260",
		SizeBytes:        1 << 30,
		Epoch:            epoch,
		Role:             blockvol.RoleToWire(blockvol.RolePrimary),
		Status:           StatusActive,
		ReplicaServer:    replica,
		ReplicaPath:      fmt.Sprintf("/data/%s.blk", name),
		ReplicaIQN:       fmt.Sprintf("iqn.2024.test:%s-replica", name),
		ReplicaISCSIAddr: replica + ":3260",
		LeaseTTL:         leaseTTL,
		LastLeaseGrant:   time.Now().Add(-2 * leaseTTL), // expired
	}
	if err := ms.blockRegistry.Register(entry); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
}

func TestFailover_PrimaryDies_ReplicaPromoted(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.failoverBlockVolumes("vs1")

	entry, ok := ms.blockRegistry.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 should still exist")
	}
	if entry.VolumeServer != "vs2" {
		t.Fatalf("VolumeServer: got %q, want vs2 (promoted replica)", entry.VolumeServer)
	}
}

func TestFailover_ReplicaDies_NoAction(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	// vs2 dies (replica server). Primary is vs1, so no failover for vol1.
	ms.failoverBlockVolumes("vs2")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs1" {
		t.Fatalf("primary should remain vs1, got %q", entry.VolumeServer)
	}
}

func TestFailover_NoReplica_NoPromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	// Single-copy volume (no replica).
	entry := &BlockVolumeEntry{
		Name:           "vol1",
		VolumeServer:   "vs1",
		Path:           "/data/vol1.blk",
		SizeBytes:      1 << 30,
		Epoch:          1,
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		Status:         StatusActive,
		LeaseTTL:       5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
	}
	ms.blockRegistry.Register(entry)

	ms.failoverBlockVolumes("vs1")

	// Volume still points to vs1, no promotion possible.
	e, _ := ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs1" {
		t.Fatalf("should remain vs1 (no replica), got %q", e.VolumeServer)
	}
}

func TestFailover_EpochBumped(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 5, 5*time.Second)

	ms.failoverBlockVolumes("vs1")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.Epoch != 6 {
		t.Fatalf("Epoch: got %d, want 6 (bumped from 5)", entry.Epoch)
	}
}

func TestFailover_RegistryUpdated(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.failoverBlockVolumes("vs1")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	// After swap: new primary = vs2, old primary (vs1) becomes replica.
	if entry.VolumeServer != "vs2" {
		t.Fatalf("VolumeServer: got %q, want vs2", entry.VolumeServer)
	}
	if entry.ReplicaServer != "vs1" {
		t.Fatalf("ReplicaServer: got %q, want vs1 (old primary)", entry.ReplicaServer)
	}
}

func TestFailover_AssignmentQueued(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.failoverBlockVolumes("vs1")

	// New primary (vs2) should have a pending assignment.
	pending := ms.blockAssignmentQueue.Pending("vs2")
	if pending < 1 {
		t.Fatalf("expected pending assignment for vs2, got %d", pending)
	}

	// Verify the assignment has the right epoch and role.
	assignments := ms.blockAssignmentQueue.Peek("vs2")
	found := false
	for _, a := range assignments {
		if a.Epoch == 2 && blockvol.RoleFromWire(a.Role) == blockvol.RolePrimary {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected Primary assignment with epoch=2 for vs2")
	}
}

func TestFailover_MultipleVolumes(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)
	registerVolumeWithReplica(t, ms, "vol2", "vs1", "vs3", 3, 5*time.Second)

	ms.failoverBlockVolumes("vs1")

	e1, _ := ms.blockRegistry.Lookup("vol1")
	if e1.VolumeServer != "vs2" {
		t.Fatalf("vol1 primary: got %q, want vs2", e1.VolumeServer)
	}
	e2, _ := ms.blockRegistry.Lookup("vol2")
	if e2.VolumeServer != "vs3" {
		t.Fatalf("vol2 primary: got %q, want vs3", e2.VolumeServer)
	}
}

func TestFailover_LeaseNotExpired_DeferredPromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	entry := &BlockVolumeEntry{
		Name:             "vol1",
		VolumeServer:     "vs1",
		Path:             "/data/vol1.blk",
		SizeBytes:        1 << 30,
		Epoch:            1,
		Role:             blockvol.RoleToWire(blockvol.RolePrimary),
		Status:           StatusActive,
		ReplicaServer:    "vs2",
		ReplicaPath:      "/data/vol1.blk",
		ReplicaIQN:       "iqn:vol1-r",
		ReplicaISCSIAddr: "vs2:3260",
		LeaseTTL:         200 * time.Millisecond,
		LastLeaseGrant:   time.Now(), // just granted, NOT expired yet
	}
	ms.blockRegistry.Register(entry)

	ms.failoverBlockVolumes("vs1")

	// Immediately after, promotion should NOT have happened (lease not expired).
	e, _ := ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs1" {
		t.Fatalf("VolumeServer should still be vs1 (lease not expired), got %q", e.VolumeServer)
	}

	// Wait for lease to expire + promotion delay.
	time.Sleep(350 * time.Millisecond)

	e, _ = ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs2" {
		t.Fatalf("VolumeServer should be vs2 after deferred promotion, got %q", e.VolumeServer)
	}
}

func TestFailover_LeaseExpired_ImmediatePromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)
	// registerVolumeWithReplica sets LastLeaseGrant in the past → expired.

	ms.failoverBlockVolumes("vs1")

	// Promotion should be immediate (lease expired).
	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("expected immediate promotion, got primary=%q", entry.VolumeServer)
	}
}

// ============================================================
// Rebuild tests (Task 7)
// ============================================================

func TestRebuild_PendingRecordedOnFailover(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.failoverBlockVolumes("vs1")

	// Check that a pending rebuild was recorded for vs1.
	ms.blockFailover.mu.Lock()
	rebuilds := ms.blockFailover.pendingRebuilds["vs1"]
	ms.blockFailover.mu.Unlock()
	if len(rebuilds) != 1 {
		t.Fatalf("expected 1 pending rebuild for vs1, got %d", len(rebuilds))
	}
	if rebuilds[0].VolumeName != "vol1" {
		t.Fatalf("pending rebuild volume: got %q, want vol1", rebuilds[0].VolumeName)
	}
}

func TestRebuild_ReconnectTriggersDrain(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.failoverBlockVolumes("vs1")

	// Simulate vs1 reconnection.
	ms.recoverBlockVolumes("vs1")

	// Pending rebuilds should be drained.
	ms.blockFailover.mu.Lock()
	rebuilds := ms.blockFailover.pendingRebuilds["vs1"]
	ms.blockFailover.mu.Unlock()
	if len(rebuilds) != 0 {
		t.Fatalf("expected 0 pending rebuilds after drain, got %d", len(rebuilds))
	}
}

func TestRebuild_StaleAndRebuildingAssignments(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.failoverBlockVolumes("vs1")
	ms.recoverBlockVolumes("vs1")

	// vs1 should have a Rebuilding assignment queued.
	assignments := ms.blockAssignmentQueue.Peek("vs1")
	found := false
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected Rebuilding assignment for vs1 after reconnect")
	}
}

func TestRebuild_VolumeDeletedWhileDown(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.failoverBlockVolumes("vs1")

	// Delete volume while vs1 is down.
	ms.blockRegistry.Unregister("vol1")

	// vs1 reconnects.
	ms.recoverBlockVolumes("vs1")

	// No assignment should be queued for deleted volume.
	assignments := ms.blockAssignmentQueue.Peek("vs1")
	for _, a := range assignments {
		if a.Path == "/data/vol1.blk" {
			t.Fatal("should not enqueue assignment for deleted volume")
		}
	}
}

func TestRebuild_PendingClearedAfterDrain(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.failoverBlockVolumes("vs1")
	rebuilds := ms.drainPendingRebuilds("vs1")
	if len(rebuilds) != 1 {
		t.Fatalf("first drain: got %d, want 1", len(rebuilds))
	}

	// Second drain should return empty.
	rebuilds = ms.drainPendingRebuilds("vs1")
	if len(rebuilds) != 0 {
		t.Fatalf("second drain: got %d, want 0", len(rebuilds))
	}
}

func TestRebuild_NoPendingRebuilds_NoAction(t *testing.T) {
	ms := testMasterServerForFailover(t)

	// No failover happened, so no pending rebuilds.
	ms.recoverBlockVolumes("vs1")

	// No assignments should be queued.
	if ms.blockAssignmentQueue.Pending("vs1") != 0 {
		t.Fatal("expected no pending assignments")
	}
}

func TestRebuild_MultipleVolumes(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)
	registerVolumeWithReplica(t, ms, "vol2", "vs1", "vs3", 2, 5*time.Second)

	ms.failoverBlockVolumes("vs1")
	ms.recoverBlockVolumes("vs1")

	// vs1 should have 2 rebuild assignments.
	assignments := ms.blockAssignmentQueue.Peek("vs1")
	rebuildCount := 0
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			rebuildCount++
		}
	}
	if rebuildCount != 2 {
		t.Fatalf("expected 2 rebuild assignments, got %d", rebuildCount)
	}
}

func TestRebuild_RegistryUpdatedWithNewReplica(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.failoverBlockVolumes("vs1")
	ms.recoverBlockVolumes("vs1")

	// After recovery, vs1 should be the new replica for vol1.
	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("primary should be vs2, got %q", entry.VolumeServer)
	}
	if entry.ReplicaServer != "vs1" {
		t.Fatalf("replica should be vs1 (reconnected), got %q", entry.ReplicaServer)
	}
}

func TestRebuild_AssignmentContainsRebuildAddr(t *testing.T) {
	ms := testMasterServerForFailover(t)
	entry := &BlockVolumeEntry{
		Name:              "vol1",
		VolumeServer:      "vs1",
		Path:              "/data/vol1.blk",
		SizeBytes:         1 << 30,
		Epoch:             1,
		Role:              blockvol.RoleToWire(blockvol.RolePrimary),
		Status:            StatusActive,
		ReplicaServer:     "vs2",
		ReplicaPath:       "/data/vol1.blk",
		ReplicaIQN:        "iqn:vol1-r",
		ReplicaISCSIAddr:  "vs2:3260",
		RebuildListenAddr: "vs1:15000",
		LeaseTTL:          5 * time.Second,
		LastLeaseGrant:    time.Now().Add(-10 * time.Second),
	}
	ms.blockRegistry.Register(entry)

	ms.failoverBlockVolumes("vs1")

	// Check new primary's rebuild listen addr is preserved.
	updated, _ := ms.blockRegistry.Lookup("vol1")
	// After swap, RebuildListenAddr should remain.

	ms.recoverBlockVolumes("vs1")

	assignments := ms.blockAssignmentQueue.Peek("vs1")
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			if a.RebuildAddr != updated.RebuildListenAddr {
				t.Fatalf("RebuildAddr: got %q, want %q", a.RebuildAddr, updated.RebuildListenAddr)
			}
			return
		}
	}
	t.Fatal("no Rebuilding assignment found")
}

// QA: Transient disconnect — if VS disconnects and reconnects before lease expires,
// the old primary should remain without failover.
func TestFailover_TransientDisconnect_NoPromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	entry := &BlockVolumeEntry{
		Name:             "vol1",
		VolumeServer:     "vs1",
		Path:             "/data/vol1.blk",
		SizeBytes:        1 << 30,
		Epoch:            1,
		Role:             blockvol.RoleToWire(blockvol.RolePrimary),
		Status:           StatusActive,
		ReplicaServer:    "vs2",
		ReplicaPath:      "/data/vol1.blk",
		ReplicaIQN:       "iqn:vol1-r",
		ReplicaISCSIAddr: "vs2:3260",
		LeaseTTL:         30 * time.Second,
		LastLeaseGrant:   time.Now(), // just granted
	}
	ms.blockRegistry.Register(entry)

	// VS disconnects. Lease has 30s left — should not promote immediately.
	ms.failoverBlockVolumes("vs1")

	e, _ := ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs1" {
		t.Fatalf("should NOT promote during transient disconnect, got %q", e.VolumeServer)
	}
}

// ============================================================
// QA: Regression — ensure CreateBlockVolume + failover integration
// ============================================================

func TestFailover_NoPrimary_NoAction(t *testing.T) {
	ms := testMasterServerForFailover(t)
	// Register a volume as replica (not primary).
	entry := &BlockVolumeEntry{
		Name:           "vol1",
		VolumeServer:   "vs1",
		Path:           "/data/vol1.blk",
		SizeBytes:      1 << 30,
		Epoch:          1,
		Role:           blockvol.RoleToWire(blockvol.RoleReplica),
		Status:         StatusActive,
		LeaseTTL:       5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
	}
	ms.blockRegistry.Register(entry)

	ms.failoverBlockVolumes("vs1")

	// No promotion should happen for replica-role volumes.
	e, _ := ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs1" {
		t.Fatalf("replica volume should not be swapped, got %q", e.VolumeServer)
	}
}

// Test full lifecycle: create with replica → failover → rebuild
func TestLifecycle_CreateFailoverRebuild(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")

	// Create volume with replica.
	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	primary := resp.VolumeServer
	replica := resp.ReplicaServer
	if replica == "" {
		t.Fatal("expected replica")
	}

	// Update lease so it's expired (simulate time passage).
	entry, _ := ms.blockRegistry.Lookup("vol1")
	entry.LastLeaseGrant = time.Now().Add(-1 * time.Minute)

	// Primary dies.
	ms.failoverBlockVolumes(primary)

	entry, _ = ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != replica {
		t.Fatalf("after failover: primary=%q, want %q", entry.VolumeServer, replica)
	}

	// Old primary reconnects.
	ms.recoverBlockVolumes(primary)

	// Verify rebuild assignment for old primary.
	assignments := ms.blockAssignmentQueue.Peek(primary)
	foundRebuild := false
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			foundRebuild = true
		}
	}
	if !foundRebuild {
		t.Fatal("expected rebuild assignment for reconnected server")
	}
}
