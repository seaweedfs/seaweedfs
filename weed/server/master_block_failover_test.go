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
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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
	// Mark both servers as block-capable so promotion Gate 4 (liveness) passes.
	ms.blockRegistry.MarkBlockCapable(primary)
	ms.blockRegistry.MarkBlockCapable(replica)
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
		// CP8-2: also populate Replicas[] for PromoteBestReplica.
		Replicas: []ReplicaInfo{
			{
				Server:        replica,
				Path:          fmt.Sprintf("/data/%s.blk", name),
				IQN:           fmt.Sprintf("iqn.2024.test:%s-replica", name),
				ISCSIAddr:     replica + ":3260",
				HealthScore:   1.0,
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				LastHeartbeat: time.Now(),
			},
		},
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
	// After PromoteBestReplica: new primary = vs2.
	// Old primary (vs1) is NOT added back to Replicas (needs rebuild).
	if entry.VolumeServer != "vs2" {
		t.Fatalf("VolumeServer: got %q, want vs2", entry.VolumeServer)
	}
	// Replicas should be empty (only had 1 replica which was promoted).
	if len(entry.Replicas) != 0 {
		t.Fatalf("Replicas should be empty after promotion, got %d", len(entry.Replicas))
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
	// Mark servers as block-capable so promotion Gate 4 (liveness) passes.
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
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
		Replicas: []ReplicaInfo{
			{Server: "vs2", Path: "/data/vol1.blk", IQN: "iqn:vol1-r", ISCSIAddr: "vs2:3260", HealthScore: 1.0, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
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

	// After recovery, vs1 should be a replica for vol1 (added back via AddReplica).
	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("primary should be vs2, got %q", entry.VolumeServer)
	}
	// Replicas[] should contain vs1 (added by recoverBlockVolumes).
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != "vs1" {
		t.Fatalf("replica should be vs1 (reconnected), got %+v", entry.Replicas)
	}
}

func TestRebuild_AssignmentContainsRebuildAddr(t *testing.T) {
	ms := testMasterServerForFailover(t)
	// Mark servers as block-capable so promotion Gate 4 (liveness) passes.
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
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
		Replicas: []ReplicaInfo{
			{Server: "vs2", Path: "/data/vol1.blk", IQN: "iqn:vol1-r", ISCSIAddr: "vs2:3260", HealthScore: 1.0, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
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
		Replicas: []ReplicaInfo{
			{Server: "vs2", Path: "/data/vol1.blk", IQN: "iqn:vol1-r", ISCSIAddr: "vs2:3260", HealthScore: 1.0, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
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

// ============================================================
// CP8-2 T8: RF=3 (N>2) Failover Tests
// ============================================================

// registerVolumeRF3 creates a volume entry with primary + 2 replicas for RF=3 tests.
func registerVolumeRF3(t *testing.T, ms *MasterServer, name, primary, replica1, replica2 string, epoch uint64, leaseTTL time.Duration) {
	t.Helper()
	// Mark all servers as block-capable so promotion Gate 4 (liveness) passes.
	ms.blockRegistry.MarkBlockCapable(primary)
	ms.blockRegistry.MarkBlockCapable(replica1)
	ms.blockRegistry.MarkBlockCapable(replica2)
	entry := &BlockVolumeEntry{
		Name:          name,
		VolumeServer:  primary,
		Path:          fmt.Sprintf("/data/%s.blk", name),
		IQN:           fmt.Sprintf("iqn.2024.test:%s", name),
		ISCSIAddr:     primary + ":3260",
		SizeBytes:     1 << 30,
		Epoch:         epoch,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		Status:        StatusActive,
		ReplicaFactor: 3,
		LeaseTTL:      leaseTTL,
		LastLeaseGrant: time.Now().Add(-2 * leaseTTL), // expired
		// Deprecated scalar fields (backward compat): first replica only.
		ReplicaServer:    replica1,
		ReplicaPath:      fmt.Sprintf("/data/%s.blk", name),
		ReplicaIQN:       fmt.Sprintf("iqn.2024.test:%s-r1", name),
		ReplicaISCSIAddr: replica1 + ":3260",
		Replicas: []ReplicaInfo{
			{
				Server:        replica1,
				Path:          fmt.Sprintf("/data/%s.blk", name),
				IQN:           fmt.Sprintf("iqn.2024.test:%s-r1", name),
				ISCSIAddr:     replica1 + ":3260",
				HealthScore:   1.0,
				WALHeadLSN:    100,
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				LastHeartbeat: time.Now(),
			},
			{
				Server:        replica2,
				Path:          fmt.Sprintf("/data/%s.blk", name),
				IQN:           fmt.Sprintf("iqn.2024.test:%s-r2", name),
				ISCSIAddr:     replica2 + ":3260",
				HealthScore:   1.0,
				WALHeadLSN:    100,
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				LastHeartbeat: time.Now(),
			},
		},
	}
	if err := ms.blockRegistry.Register(entry); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
}

// RF3: Primary dies → best replica promoted, other replica survives.
func TestRF3_PrimaryDies_BestReplicaPromoted(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeRF3(t, ms, "vol1", "vs1", "vs2", "vs3", 1, 5*time.Second)

	// Give vs3 a higher health score so it should be promoted.
	entry, _ := ms.blockRegistry.Lookup("vol1")
	entry.Replicas[0].HealthScore = 0.8 // vs2
	entry.Replicas[1].HealthScore = 1.0 // vs3

	ms.failoverBlockVolumes("vs1")

	entry, _ = ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs3" {
		t.Fatalf("primary should be vs3 (highest health), got %q", entry.VolumeServer)
	}
	if entry.Epoch != 2 {
		t.Fatalf("epoch: got %d, want 2", entry.Epoch)
	}
	// vs2 should remain as the only replica.
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 remaining replica, got %d", len(entry.Replicas))
	}
	if entry.Replicas[0].Server != "vs2" {
		t.Fatalf("remaining replica should be vs2, got %q", entry.Replicas[0].Server)
	}
}

// RF3: One replica dies → primary stays, dead replica removed, pending rebuild.
func TestRF3_OneReplicaDies_PrimaryUnchanged(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeRF3(t, ms, "vol1", "vs1", "vs2", "vs3", 1, 5*time.Second)

	// vs3 dies (a replica, not the primary).
	ms.failoverBlockVolumes("vs3")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	// Primary should remain vs1.
	if entry.VolumeServer != "vs1" {
		t.Fatalf("primary should remain vs1, got %q", entry.VolumeServer)
	}
	// Epoch should NOT change (no promotion).
	if entry.Epoch != 1 {
		t.Fatalf("epoch should remain 1, got %d", entry.Epoch)
	}
	// vs3 should be removed from replicas, leaving only vs2.
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 replica after vs3 death, got %d", len(entry.Replicas))
	}
	if entry.Replicas[0].Server != "vs2" {
		t.Fatalf("remaining replica should be vs2, got %q", entry.Replicas[0].Server)
	}

	// Pending rebuild should be recorded for vs3.
	ms.blockFailover.mu.Lock()
	rebuilds := ms.blockFailover.pendingRebuilds["vs3"]
	ms.blockFailover.mu.Unlock()
	if len(rebuilds) != 1 || rebuilds[0].VolumeName != "vol1" {
		t.Fatalf("expected 1 pending rebuild for vs3/vol1, got %+v", rebuilds)
	}
}

// RF3: Dead replica reconnects → rebuilds, rejoins replica list.
func TestRF3_RecoverRebuildsDeadReplica(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeRF3(t, ms, "vol1", "vs1", "vs2", "vs3", 1, 5*time.Second)

	// vs3 dies.
	ms.failoverBlockVolumes("vs3")

	// vs3 reconnects.
	ms.recoverBlockVolumes("vs3")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	// Primary still vs1.
	if entry.VolumeServer != "vs1" {
		t.Fatalf("primary should remain vs1, got %q", entry.VolumeServer)
	}
	// vs3 should be back in replicas (added by recoverBlockVolumes via AddReplica).
	if len(entry.Replicas) != 2 {
		t.Fatalf("expected 2 replicas after recovery, got %d", len(entry.Replicas))
	}
	// Verify rebuild assignment was queued for vs3.
	assignments := ms.blockAssignmentQueue.Peek("vs3")
	foundRebuild := false
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			foundRebuild = true
		}
	}
	if !foundRebuild {
		t.Fatal("expected rebuild assignment for vs3")
	}
}

// RF3: Other replicas survive promotion.
func TestRF3_OtherReplicasSurvivePromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeRF3(t, ms, "vol1", "vs1", "vs2", "vs3", 1, 5*time.Second)

	// Primary (vs1) dies → one replica promoted.
	ms.failoverBlockVolumes("vs1")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	newPrimary := entry.VolumeServer
	// New primary should be vs2 or vs3 (both have equal health/LSN, so first wins).
	if newPrimary != "vs2" && newPrimary != "vs3" {
		t.Fatalf("new primary should be vs2 or vs3, got %q", newPrimary)
	}
	// The other replica should still be in the list.
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 surviving replica, got %d", len(entry.Replicas))
	}
	// The surviving replica is whichever wasn't promoted.
	expectedSurvivor := "vs3"
	if newPrimary == "vs3" {
		expectedSurvivor = "vs2"
	}
	if entry.Replicas[0].Server != expectedSurvivor {
		t.Fatalf("surviving replica should be %q, got %q", expectedSurvivor, entry.Replicas[0].Server)
	}

	// Assignment for new primary should include ReplicaAddrs for the surviving replica.
	assignments := ms.blockAssignmentQueue.Peek(newPrimary)
	foundPrimary := false
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RolePrimary {
			foundPrimary = true
			if len(a.ReplicaAddrs) != 1 {
				t.Fatalf("expected 1 ReplicaAddr in assignment, got %d", len(a.ReplicaAddrs))
			}
		}
	}
	if !foundPrimary {
		t.Fatal("expected primary assignment for new primary")
	}
}

// RF2 unchanged: verify RF=2 still works identically.
func TestRF2_Unchanged_AfterCP82(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.failoverBlockVolumes("vs1")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("RF2: primary should be vs2, got %q", entry.VolumeServer)
	}
	if entry.Epoch != 2 {
		t.Fatalf("RF2: epoch should be 2, got %d", entry.Epoch)
	}
	// No replicas left (RF=2, one promoted).
	if len(entry.Replicas) != 0 {
		t.Fatalf("RF2: expected 0 replicas after promotion, got %d", len(entry.Replicas))
	}

	// Rebuild works the same.
	ms.recoverBlockVolumes("vs1")
	entry, _ = ms.blockRegistry.Lookup("vol1")
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != "vs1" {
		t.Fatalf("RF2: vs1 should be back as replica after recovery, got %+v", entry.Replicas)
	}
}

// RF3: All replicas dead → no promotion possible.
func TestRF3_AllReplicasDead_NoPromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeRF3(t, ms, "vol1", "vs1", "vs2", "vs3", 1, 5*time.Second)

	// Both replicas die first (not primary).
	ms.failoverBlockVolumes("vs2")
	ms.failoverBlockVolumes("vs3")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	// Primary still vs1, replicas removed.
	if entry.VolumeServer != "vs1" {
		t.Fatalf("primary should remain vs1, got %q", entry.VolumeServer)
	}
	if len(entry.Replicas) != 0 {
		t.Fatalf("expected 0 replicas, got %d", len(entry.Replicas))
	}

	// Now primary dies → no promotion possible (no replicas left).
	ms.failoverBlockVolumes("vs1")

	entry, _ = ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs1" {
		t.Fatalf("should remain vs1 (no replicas to promote), got %q", entry.VolumeServer)
	}
}

// RF3: Lease deferred promotion with RF=3.
func TestRF3_LeaseDeferred_Promotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	// Mark servers as block-capable so promotion Gate 4 (liveness) passes.
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.MarkBlockCapable("vs3")
	entry := &BlockVolumeEntry{
		Name:          "vol1",
		VolumeServer:  "vs1",
		Path:          "/data/vol1.blk",
		IQN:           "iqn:vol1",
		ISCSIAddr:     "vs1:3260",
		SizeBytes:     1 << 30,
		Epoch:         1,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		Status:        StatusActive,
		ReplicaFactor: 3,
		LeaseTTL:      200 * time.Millisecond,
		LastLeaseGrant: time.Now(), // just granted → NOT expired
		Replicas: []ReplicaInfo{
			{Server: "vs2", Path: "/data/vol1.blk", ISCSIAddr: "vs2:3260", HealthScore: 1.0, WALHeadLSN: 50, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
			{Server: "vs3", Path: "/data/vol1.blk", ISCSIAddr: "vs3:3260", HealthScore: 0.9, WALHeadLSN: 50, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
		// Deprecated scalar fields.
		ReplicaServer: "vs2", ReplicaPath: "/data/vol1.blk", ReplicaISCSIAddr: "vs2:3260",
	}
	ms.blockRegistry.Register(entry)

	ms.failoverBlockVolumes("vs1")

	// Immediately: no promotion (lease not expired).
	e, _ := ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs1" {
		t.Fatalf("should NOT promote yet (lease active), got %q", e.VolumeServer)
	}

	// Wait for lease to expire + deferred timer.
	time.Sleep(350 * time.Millisecond)

	e, _ = ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs2" {
		t.Fatalf("should promote vs2 (highest health) after lease expires, got %q", e.VolumeServer)
	}
	// vs3 should survive as the remaining replica.
	if len(e.Replicas) != 1 || e.Replicas[0].Server != "vs3" {
		t.Fatalf("vs3 should remain as replica, got %+v", e.Replicas)
	}
}

// RF3: Cancel deferred timers on reconnect.
func TestRF3_CancelDeferredOnReconnect(t *testing.T) {
	ms := testMasterServerForFailover(t)
	entry := &BlockVolumeEntry{
		Name:          "vol1",
		VolumeServer:  "vs1",
		Path:          "/data/vol1.blk",
		IQN:           "iqn:vol1",
		ISCSIAddr:     "vs1:3260",
		SizeBytes:     1 << 30,
		Epoch:         1,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		Status:        StatusActive,
		ReplicaFactor: 3,
		LeaseTTL:      5 * time.Second,
		LastLeaseGrant: time.Now(), // just granted → long lease
		Replicas: []ReplicaInfo{
			{Server: "vs2", Path: "/data/vol1.blk", ISCSIAddr: "vs2:3260", HealthScore: 1.0, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
			{Server: "vs3", Path: "/data/vol1.blk", ISCSIAddr: "vs3:3260", HealthScore: 1.0, Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
		ReplicaServer: "vs2", ReplicaPath: "/data/vol1.blk", ReplicaISCSIAddr: "vs2:3260",
	}
	ms.blockRegistry.Register(entry)

	// vs1 disconnects → deferred timer created (lease not expired).
	ms.failoverBlockVolumes("vs1")

	ms.blockFailover.mu.Lock()
	timerCount := len(ms.blockFailover.deferredTimers["vs1"])
	ms.blockFailover.mu.Unlock()
	if timerCount == 0 {
		t.Fatal("expected deferred timer for vs1")
	}

	// vs1 reconnects before lease expires → timers cancelled.
	ms.recoverBlockVolumes("vs1")

	ms.blockFailover.mu.Lock()
	timerCount = len(ms.blockFailover.deferredTimers["vs1"])
	ms.blockFailover.mu.Unlock()
	if timerCount != 0 {
		t.Fatalf("expected 0 deferred timers after reconnect, got %d", timerCount)
	}

	// Wait past original lease time — no promotion should happen.
	time.Sleep(200 * time.Millisecond)

	e, _ := ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs1" {
		t.Fatalf("vs1 should remain primary (timer cancelled), got %q", e.VolumeServer)
	}
}

// ============================================================
// CP11B-3 T2: Re-evaluate on Replica Registration (B-06)
// ============================================================

// T2: Orphaned primary + replica reconnects → automatic promotion.
func TestT2_OrphanedPrimary_ReplicaReconnect_Promotes(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	// Simulate vs1 dying without proper failover (e.g., promotion failed at the time).
	// Mark vs1 as dead but DON'T call failoverBlockVolumes (simulates missed/failed failover).
	ms.blockRegistry.UnmarkBlockCapable("vs1")

	// vs2 reconnects (sends heartbeat). reevaluateOrphanedPrimaries should detect orphaned primary.
	ms.recoverBlockVolumes("vs2")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("expected promotion to vs2 (orphaned primary), got %q", entry.VolumeServer)
	}
	if entry.Epoch != 2 {
		t.Fatalf("expected epoch 2 after promotion, got %d", entry.Epoch)
	}
}

// T2: Replica reconnects but primary is alive → no unnecessary promotion.
func TestT2_PrimaryAlive_NoPromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	// Both servers alive. vs2 reconnects — no orphaned primary.
	ms.recoverBlockVolumes("vs2")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs1" {
		t.Fatalf("primary should remain vs1 (alive), got %q", entry.VolumeServer)
	}
	if entry.Epoch != 1 {
		t.Fatalf("epoch should remain 1, got %d", entry.Epoch)
	}
}

// T2: Multiple orphaned volumes, all promoted on reconnect.
func TestT2_MultipleOrphanedVolumes(t *testing.T) {
	ms := testMasterServerForFailover(t)
	// vol1: vs1=primary, vs2=replica
	// vol2: vs3=primary, vs2=replica
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)
	ms.blockRegistry.MarkBlockCapable("vs3")
	entry2 := &BlockVolumeEntry{
		Name: "vol2", VolumeServer: "vs3", Path: "/data/vol2.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol2.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	}
	ms.blockRegistry.Register(entry2)

	// Both primaries die.
	ms.blockRegistry.UnmarkBlockCapable("vs1")
	ms.blockRegistry.UnmarkBlockCapable("vs3")

	// vs2 reconnects → both orphaned volumes should be promoted.
	ms.recoverBlockVolumes("vs2")

	e1, _ := ms.blockRegistry.Lookup("vol1")
	e2, _ := ms.blockRegistry.Lookup("vol2")
	if e1.VolumeServer != "vs2" {
		t.Fatalf("vol1: expected promotion to vs2, got %q", e1.VolumeServer)
	}
	if e2.VolumeServer != "vs2" {
		t.Fatalf("vol2: expected promotion to vs2, got %q", e2.VolumeServer)
	}
}

// T2: Repeated heartbeats do NOT cause duplicate promotions.
func TestT2_RepeatedHeartbeats_NoDuplicatePromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	ms.blockRegistry.UnmarkBlockCapable("vs1")

	// First reconnect promotes.
	ms.reevaluateOrphanedPrimaries("vs2")
	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("first call: expected promotion to vs2, got %q", entry.VolumeServer)
	}
	epochAfterFirst := entry.Epoch

	// Second call: vs2 is now the primary AND block-capable. No orphan detected.
	ms.reevaluateOrphanedPrimaries("vs2")
	entry, _ = ms.blockRegistry.Lookup("vol1")
	if entry.Epoch != epochAfterFirst {
		t.Fatalf("second call should not bump epoch: got %d, want %d", entry.Epoch, epochAfterFirst)
	}
}

// T2: Dead primary with active lease, replica reconnects → no immediate promotion.
// Regression test for lease-bypass bug: reevaluateOrphanedPrimaries must respect
// lease expiry, not promote immediately.
func TestT2_OrphanedPrimary_LeaseNotExpired_DefersPromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 300 * time.Millisecond,
		LastLeaseGrant: time.Now(), // lease still active
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	})

	// vs1 dies (unmark block-capable).
	ms.blockRegistry.UnmarkBlockCapable("vs1")

	// vs2 reconnects — orphan detected, but lease still active → should NOT promote immediately.
	ms.reevaluateOrphanedPrimaries("vs2")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs1" {
		t.Fatalf("should NOT promote while lease active, got primary=%q", entry.VolumeServer)
	}
	if entry.Epoch != 1 {
		t.Fatalf("epoch should remain 1, got %d", entry.Epoch)
	}

	// Verify a deferred timer was created for the dead primary.
	ms.blockFailover.mu.Lock()
	timerCount := len(ms.blockFailover.deferredTimers["vs1"])
	ms.blockFailover.mu.Unlock()
	if timerCount != 1 {
		t.Fatalf("expected 1 deferred timer for vs1, got %d", timerCount)
	}

	// Wait for lease to expire + margin → timer fires, promotion happens.
	time.Sleep(450 * time.Millisecond)

	entry, _ = ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("after lease expiry, expected promotion to vs2, got %q", entry.VolumeServer)
	}
	if entry.Epoch != 2 {
		t.Fatalf("expected epoch 2, got %d", entry.Epoch)
	}
}

// ============================================================
// CP11B-3 T3: Deferred Timer Safety
// ============================================================

// T3: Delete/recreate volume before deferred timer fires → no wrong promotion.
func TestT3_DeferredTimer_VolumeDeleted_NoPromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	entry := &BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 5, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 200 * time.Millisecond,
		LastLeaseGrant: time.Now(),
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	}
	ms.blockRegistry.Register(entry)

	// vs1 dies → deferred timer created (lease not expired, epoch=5).
	ms.failoverBlockVolumes("vs1")

	// Delete the volume before timer fires.
	ms.blockRegistry.Unregister("vol1")

	// Wait for timer to fire.
	time.Sleep(350 * time.Millisecond)

	// Volume should not exist (timer found it deleted, no-op).
	_, ok := ms.blockRegistry.Lookup("vol1")
	if ok {
		t.Fatal("volume should have been deleted, timer should not recreate it")
	}
}

// T3: Epoch changes before deferred timer fires → timer rejected.
func TestT3_DeferredTimer_EpochChanged_NoPromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.MarkBlockCapable("vs3")
	entry := &BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 5, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 200 * time.Millisecond,
		LastLeaseGrant: time.Now(),
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	}
	ms.blockRegistry.Register(entry)

	// vs1 dies → deferred timer created (captures epoch=5).
	ms.failoverBlockVolumes("vs1")

	// Before timer fires, manually bump the epoch (simulating another event).
	e, _ := ms.blockRegistry.Lookup("vol1")
	e.Epoch = 99

	// Wait for timer to fire.
	time.Sleep(350 * time.Millisecond)

	// Timer should have been rejected (epoch mismatch). Epoch stays at 99.
	e, _ = ms.blockRegistry.Lookup("vol1")
	if e.Epoch != 99 {
		t.Fatalf("epoch should remain 99 (timer rejected), got %d", e.Epoch)
	}
	// Primary should NOT have changed (deferred promotion was rejected).
	if e.VolumeServer != "vs1" {
		t.Fatalf("primary should remain vs1 (timer rejected), got %q", e.VolumeServer)
	}
}

// ============================================================
// CP11B-3 T4: Rebuild with empty RebuildListenAddr
// ============================================================

// T4: Rebuild queued with empty RebuildListenAddr after promotion.
func TestT4_RebuildEmptyAddr_StillQueued(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	// Failover: vs1 dies, vs2 promoted. PromoteBestReplica clears RebuildListenAddr.
	ms.failoverBlockVolumes("vs1")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.RebuildListenAddr != "" {
		t.Fatalf("RebuildListenAddr should be empty after promotion, got %q", entry.RebuildListenAddr)
	}

	// vs1 reconnects. Rebuild should still be queued (even with empty addr).
	ms.recoverBlockVolumes("vs1")

	assignments := ms.blockAssignmentQueue.Peek("vs1")
	foundRebuild := false
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			foundRebuild = true
			if a.RebuildAddr != "" {
				t.Fatalf("RebuildAddr should be empty (new primary hasn't heartbeated), got %q", a.RebuildAddr)
			}
		}
	}
	if !foundRebuild {
		t.Fatal("rebuild assignment should still be queued even with empty addr")
	}
}
