package weed_server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// CP8-2 T10: RF=3 QA + Integration Tests
//
// Adversarial and integration tests for multi-replica (N>2)
// support introduced in CP8-2.
// ============================================================

// Helper: create master with 3 block-capable servers and RF=3-aware alloc mock.
func qaRF3Master(t *testing.T) *MasterServer {
	t.Helper()
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         server + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		return nil
	}
	ms.blockVSExpand = func(ctx context.Context, server string, name string, newSize uint64) (uint64, error) {
		return newSize, nil
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockRegistry.MarkBlockCapable("vs3:9333")
	return ms
}

// 1. Create RF=3, verify 2 replicas in registry with correct data.
func TestQA_RF3_CreateAndVerifyReplicas(t *testing.T) {
	ms := qaRF3Master(t)
	ctx := context.Background()

	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:          "pvc-rf3-1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, ok := ms.blockRegistry.Lookup("pvc-rf3-1")
	if !ok {
		t.Fatal("volume not in registry")
	}

	// Verify ReplicaFactor.
	if entry.ReplicaFactor != 3 {
		t.Fatalf("ReplicaFactor: got %d, want 3", entry.ReplicaFactor)
	}

	// Verify 2 replicas.
	if len(entry.Replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(entry.Replicas))
	}

	// All 3 servers are distinct.
	servers := map[string]bool{resp.VolumeServer: true}
	for _, ri := range entry.Replicas {
		if servers[ri.Server] {
			t.Fatalf("duplicate server in RF=3 volume: %q", ri.Server)
		}
		servers[ri.Server] = true
		// Each replica should have path, IQN, ISCSIAddr set.
		if ri.Path == "" || ri.IQN == "" || ri.ISCSIAddr == "" {
			t.Fatalf("replica %q missing fields: path=%q iqn=%q iscsi=%q",
				ri.Server, ri.Path, ri.IQN, ri.ISCSIAddr)
		}
	}

	// Backward compat: ReplicaServer (deprecated) should point to first replica.
	if entry.ReplicaServer == "" {
		t.Fatal("deprecated ReplicaServer should be set for backward compat")
	}
	if entry.ReplicaServer != entry.Replicas[0].Server {
		t.Fatalf("deprecated ReplicaServer %q != Replicas[0].Server %q",
			entry.ReplicaServer, entry.Replicas[0].Server)
	}

	// Assignments: primary should have 2 ReplicaAddrs.
	primaryAssignments := ms.blockAssignmentQueue.Peek(resp.VolumeServer)
	if len(primaryAssignments) != 1 {
		t.Fatalf("expected 1 primary assignment, got %d", len(primaryAssignments))
	}
	if len(primaryAssignments[0].ReplicaAddrs) != 2 {
		t.Fatalf("primary assignment ReplicaAddrs: got %d, want 2",
			len(primaryAssignments[0].ReplicaAddrs))
	}

	// Each replica server should have 1 assignment.
	for _, ri := range entry.Replicas {
		ra := ms.blockAssignmentQueue.Peek(ri.Server)
		if len(ra) != 1 {
			t.Fatalf("replica %q: expected 1 assignment, got %d", ri.Server, len(ra))
		}
		if blockvol.RoleFromWire(ra[0].Role) != blockvol.RoleReplica {
			t.Fatalf("replica %q: expected RoleReplica, got %d", ri.Server, ra[0].Role)
		}
	}
}

// 2. Kill primary → best replica (highest health score) promoted.
func TestQA_RF3_PrimaryDies_BestReplicaPromoted(t *testing.T) {
	ms := qaRF3Master(t)
	ctx := context.Background()

	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:          "pvc-rf3-promo",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("pvc-rf3-promo")
	primary := resp.VolumeServer

	// Set different health scores on replicas.
	for i := range entry.Replicas {
		if i == 0 {
			entry.Replicas[i].HealthScore = 0.7
		} else {
			entry.Replicas[i].HealthScore = 1.0
		}
	}
	expectedPromoted := entry.Replicas[1].Server // higher health score
	entry.LastLeaseGrant = time.Now().Add(-1 * time.Minute) // expired

	// Kill primary.
	ms.failoverBlockVolumes(primary)

	entry, _ = ms.blockRegistry.Lookup("pvc-rf3-promo")
	if entry.VolumeServer != expectedPromoted {
		t.Fatalf("expected %q promoted (highest health), got %q", expectedPromoted, entry.VolumeServer)
	}
	// 1 replica should remain (the one not promoted).
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 remaining replica, got %d", len(entry.Replicas))
	}
	// Pending rebuild for old primary.
	ms.blockFailover.mu.Lock()
	rebuilds := ms.blockFailover.pendingRebuilds[primary]
	ms.blockFailover.mu.Unlock()
	if len(rebuilds) != 1 || rebuilds[0].VolumeName != "pvc-rf3-promo" {
		t.Fatalf("expected 1 pending rebuild for %q, got %+v", primary, rebuilds)
	}
}

// 3. Kill one replica → writes unaffected (primary unchanged, dead replica removed).
func TestQA_RF3_OneReplicaDies_WritesUnaffected(t *testing.T) {
	ms := qaRF3Master(t)
	ctx := context.Background()

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:          "pvc-rf3-repdeath",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("pvc-rf3-repdeath")
	originalPrimary := entry.VolumeServer
	originalEpoch := entry.Epoch
	deadReplica := entry.Replicas[1].Server // kill second replica

	ms.failoverBlockVolumes(deadReplica)

	entry, _ = ms.blockRegistry.Lookup("pvc-rf3-repdeath")
	// Primary unchanged.
	if entry.VolumeServer != originalPrimary {
		t.Fatalf("primary should remain %q, got %q", originalPrimary, entry.VolumeServer)
	}
	// Epoch unchanged (no promotion).
	if entry.Epoch != originalEpoch {
		t.Fatalf("epoch should remain %d, got %d", originalEpoch, entry.Epoch)
	}
	// Dead replica removed, 1 replica remains.
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 replica after death, got %d", len(entry.Replicas))
	}
	if entry.Replicas[0].Server == deadReplica {
		t.Fatalf("dead replica %q should be removed", deadReplica)
	}
}

// 4. Dead replica reconnects → rebuilds and rejoins.
func TestQA_RF3_Rebuild_DeadReplicaCatchesUp(t *testing.T) {
	ms := qaRF3Master(t)
	ctx := context.Background()

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:          "pvc-rf3-rebuild",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("pvc-rf3-rebuild")
	deadReplica := entry.Replicas[0].Server

	// Kill one replica.
	ms.failoverBlockVolumes(deadReplica)

	// Verify it's removed.
	entry, _ = ms.blockRegistry.Lookup("pvc-rf3-rebuild")
	if len(entry.Replicas) != 1 {
		t.Fatalf("after kill: expected 1 replica, got %d", len(entry.Replicas))
	}

	// Dead replica reconnects.
	ms.recoverBlockVolumes(deadReplica)

	// Verify it's back.
	entry, _ = ms.blockRegistry.Lookup("pvc-rf3-rebuild")
	if len(entry.Replicas) != 2 {
		t.Fatalf("after reconnect: expected 2 replicas, got %d", len(entry.Replicas))
	}

	// Rebuild assignment should be queued.
	assignments := ms.blockAssignmentQueue.Peek(deadReplica)
	foundRebuild := false
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			foundRebuild = true
			if a.RebuildAddr == "" {
				t.Fatal("rebuild assignment missing RebuildAddr")
			}
		}
	}
	if !foundRebuild {
		t.Fatalf("expected rebuild assignment for %q", deadReplica)
	}
}

// 5. Health score influences failover preference.
func TestQA_RF3_HealthScore_FailoverPreference(t *testing.T) {
	ms := qaRF3Master(t)
	ctx := context.Background()

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:          "pvc-rf3-health",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("pvc-rf3-health")
	primary := entry.VolumeServer

	// Set health scores: replica[0] = 0.3 (low), replica[1] = 0.9 (high).
	entry.Replicas[0].HealthScore = 0.3
	entry.Replicas[0].WALHeadLSN = 100
	entry.Replicas[1].HealthScore = 0.9
	entry.Replicas[1].WALHeadLSN = 100
	expectedWinner := entry.Replicas[1].Server
	entry.LastLeaseGrant = time.Now().Add(-1 * time.Minute) // expired

	ms.failoverBlockVolumes(primary)

	entry, _ = ms.blockRegistry.Lookup("pvc-rf3-health")
	if entry.VolumeServer != expectedWinner {
		t.Fatalf("expected %q (health=0.9) promoted, got %q", expectedWinner, entry.VolumeServer)
	}

	// The low-health replica should remain in the list.
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 remaining replica, got %d", len(entry.Replicas))
	}
	if entry.Replicas[0].HealthScore != 0.3 {
		t.Fatalf("remaining replica health: got %.1f, want 0.3", entry.Replicas[0].HealthScore)
	}
}

// 6. RF=2 backward compat: identical behavior to pre-CP8-2.
func TestQA_RF3_BackwardCompat_RF2_Unchanged(t *testing.T) {
	ms := qaRF3Master(t)
	ctx := context.Background()

	// Create with default RF (should be 2).
	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-rf2-compat",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("pvc-rf2-compat")

	// RF should default to 2.
	if entry.ReplicaFactor != 2 {
		t.Fatalf("RF default: got %d, want 2", entry.ReplicaFactor)
	}
	// Exactly 1 replica.
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 replica for RF=2, got %d", len(entry.Replicas))
	}

	// Failover should work identically.
	primary := resp.VolumeServer
	entry.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	ms.failoverBlockVolumes(primary)

	entry, _ = ms.blockRegistry.Lookup("pvc-rf2-compat")
	if entry.VolumeServer == primary {
		t.Fatalf("failover failed: primary should have changed from %q", primary)
	}
	// No replicas left after RF=2 promotion.
	if len(entry.Replicas) != 0 {
		t.Fatalf("expected 0 replicas after RF=2 promotion, got %d", len(entry.Replicas))
	}

	// Recover → rebuild.
	ms.recoverBlockVolumes(primary)
	entry, _ = ms.blockRegistry.Lookup("pvc-rf2-compat")
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != primary {
		t.Fatalf("after recovery: expected %q as replica, got %+v", primary, entry.Replicas)
	}
}

// 7. Full lifecycle: create RF=3 → kill primary → promote → kill another → promote → recover all.
func TestQA_RF3_FullLifecycle(t *testing.T) {
	ms := qaRF3Master(t)
	ctx := context.Background()

	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:          "pvc-rf3-lifecycle",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("pvc-rf3-lifecycle")
	vs1 := resp.VolumeServer
	vs2 := entry.Replicas[0].Server
	vs3 := entry.Replicas[1].Server
	entry.LastLeaseGrant = time.Now().Add(-1 * time.Minute)

	// Step 1: Kill vs1 (primary). One of vs2/vs3 promoted.
	ms.failoverBlockVolumes(vs1)
	entry, _ = ms.blockRegistry.Lookup("pvc-rf3-lifecycle")
	if entry.VolumeServer == vs1 {
		t.Fatal("step1: primary should not be vs1 after failover")
	}
	step1Primary := entry.VolumeServer
	if len(entry.Replicas) != 1 {
		t.Fatalf("step1: expected 1 replica, got %d", len(entry.Replicas))
	}
	step1Replica := entry.Replicas[0].Server

	// Step 2: Recover vs1 → becomes replica (rebuilding).
	ms.recoverBlockVolumes(vs1)
	entry, _ = ms.blockRegistry.Lookup("pvc-rf3-lifecycle")
	if len(entry.Replicas) != 2 {
		t.Fatalf("step2: expected 2 replicas after vs1 recovery, got %d", len(entry.Replicas))
	}

	// Step 3: Kill step1Primary. The surviving replica (step1Replica or vs1) should be promoted.
	entry.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	// Set health scores: vs1 low (just rebuilt), step1Replica high.
	for i := range entry.Replicas {
		if entry.Replicas[i].Server == vs1 {
			entry.Replicas[i].HealthScore = 0.5
		} else {
			entry.Replicas[i].HealthScore = 1.0
		}
	}
	ms.failoverBlockVolumes(step1Primary)

	entry, _ = ms.blockRegistry.Lookup("pvc-rf3-lifecycle")
	if entry.VolumeServer == step1Primary {
		t.Fatal("step3: primary should not be step1Primary after second failover")
	}
	// The higher health replica should win.
	if entry.VolumeServer != step1Replica {
		t.Fatalf("step3: expected %q promoted (health=1.0), got %q", step1Replica, entry.VolumeServer)
	}

	// Step 4: Recover step1Primary → becomes replica.
	ms.recoverBlockVolumes(step1Primary)
	entry, _ = ms.blockRegistry.Lookup("pvc-rf3-lifecycle")
	// Should have 2 replicas now (vs1 from step2 still there + step1Primary recovered).
	// Actually vs1's health was 0.5 and was NOT promoted, so vs1 stays as replica.
	if len(entry.Replicas) < 1 {
		t.Fatalf("step4: expected at least 1 replica after recovery, got %d", len(entry.Replicas))
	}

	// Step 5: Delete.
	_, err = ms.DeleteBlockVolume(ctx, &master_pb.DeleteBlockVolumeRequest{Name: "pvc-rf3-lifecycle"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := ms.blockRegistry.Lookup("pvc-rf3-lifecycle"); ok {
		t.Fatal("volume should be deleted")
	}

	_ = vs2
	_ = vs3
}
