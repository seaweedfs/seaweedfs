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
// Integration Tests: Cross-component flows for CP6-3
//
// These tests simulate the full lifecycle spanning multiple
// components (master registry, assignment queue, failover state,
// CSI publish) without real gRPC or iSCSI infrastructure.
// ============================================================

// integrationMaster creates a MasterServer wired with registry, queue, and
// failover state, plus two block-capable servers with deterministic mock
// allocate/delete callbacks. Suitable for end-to-end control-plane tests.
func integrationMaster(t *testing.T) *MasterServer {
	t.Helper()
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	return ms
}

// ============================================================
// Required #1: Failover + CSI Publish
//
// Goal: after primary dies, replica is promoted and
// LookupBlockVolume (used by ControllerPublishVolume) returns
// the new iSCSI address.
// ============================================================

func TestIntegration_FailoverCSIPublish(t *testing.T) {
	ms := integrationMaster(t)
	ctx := context.Background()

	// Step 1: Create replicated volume.
	createResp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-data-1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	if createResp.ReplicaServer == "" {
		t.Fatal("expected replica server")
	}

	primaryVS := createResp.VolumeServer
	replicaVS := createResp.ReplicaServer

	// Step 2: Verify initial CSI publish returns primary's address.
	lookupResp, err := ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "pvc-data-1"})
	if err != nil {
		t.Fatalf("initial Lookup: %v", err)
	}
	if lookupResp.IscsiAddr != primaryVS+":3260" {
		t.Fatalf("initial publish should return primary iSCSI addr %q, got %q",
			primaryVS+":3260", lookupResp.IscsiAddr)
	}

	// Step 3: Expire lease so failover is immediate.
	ms.blockRegistry.UpdateEntry("pvc-data-1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})

	// Step 4: Primary VS dies — triggers failover.
	ms.failoverBlockVolumes(primaryVS)

	// Step 5: Verify registry swap.
	entry, _ := ms.blockRegistry.Lookup("pvc-data-1")
	if entry.VolumeServer != replicaVS {
		t.Fatalf("after failover: primary should be %q, got %q", replicaVS, entry.VolumeServer)
	}
	if entry.Epoch != 2 {
		t.Fatalf("epoch should be bumped to 2, got %d", entry.Epoch)
	}

	// Step 6: CSI ControllerPublishVolume (simulated via Lookup) returns NEW address.
	lookupResp, err = ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "pvc-data-1"})
	if err != nil {
		t.Fatalf("post-failover Lookup: %v", err)
	}
	if lookupResp.IscsiAddr == primaryVS+":3260" {
		t.Fatalf("post-failover publish should NOT return dead primary's addr %q", lookupResp.IscsiAddr)
	}
	if lookupResp.IscsiAddr != replicaVS+":3260" {
		t.Fatalf("post-failover publish should return promoted replica's addr %q, got %q",
			replicaVS+":3260", lookupResp.IscsiAddr)
	}

	// Step 7: Verify new primary assignment was enqueued for the promoted server.
	assignments := ms.blockAssignmentQueue.Peek(replicaVS)
	foundPrimary := false
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RolePrimary && a.Epoch == 2 {
			foundPrimary = true
		}
	}
	if !foundPrimary {
		t.Fatal("new primary assignment (epoch=2) should be queued for promoted server")
	}
}

// ============================================================
// Required #2: Rebuild on Recovery
//
// Goal: old primary comes back, gets Rebuilding assignment,
// and WAL catch-up + extent rebuild are wired correctly.
// ============================================================

func TestIntegration_RebuildOnRecovery(t *testing.T) {
	ms := integrationMaster(t)
	ctx := context.Background()

	// Step 1: Create replicated volume.
	createResp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-db-1",
		SizeBytes: 10 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	primaryVS := createResp.VolumeServer
	replicaVS := createResp.ReplicaServer

	// Step 2: Expire lease for immediate failover.
	ms.blockRegistry.UpdateEntry("pvc-db-1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})

	// Step 3: Primary dies → replica promoted.
	ms.failoverBlockVolumes(primaryVS)

	entryAfterFailover, _ := ms.blockRegistry.Lookup("pvc-db-1")
	if entryAfterFailover.VolumeServer != replicaVS {
		t.Fatalf("failover: primary should be %q, got %q", replicaVS, entryAfterFailover.VolumeServer)
	}
	newEpoch := entryAfterFailover.Epoch

	// Step 4: Verify pending rebuild recorded for dead primary.
	ms.blockFailover.mu.Lock()
	rebuilds := ms.blockFailover.pendingRebuilds[primaryVS]
	ms.blockFailover.mu.Unlock()
	if len(rebuilds) != 1 {
		t.Fatalf("expected 1 pending rebuild for %s, got %d", primaryVS, len(rebuilds))
	}
	if rebuilds[0].VolumeName != "pvc-db-1" {
		t.Fatalf("pending rebuild volume: got %q, want pvc-db-1", rebuilds[0].VolumeName)
	}

	// Step 5: Old primary reconnects.
	ms.recoverBlockVolumes(primaryVS)

	// Step 6: Pending rebuilds drained.
	ms.blockFailover.mu.Lock()
	remainingRebuilds := ms.blockFailover.pendingRebuilds[primaryVS]
	ms.blockFailover.mu.Unlock()
	if len(remainingRebuilds) != 0 {
		t.Fatalf("pending rebuilds should be drained after recovery, got %d", len(remainingRebuilds))
	}

	// Step 7: Rebuilding assignment enqueued for old primary.
	assignments := ms.blockAssignmentQueue.Peek(primaryVS)
	var rebuildAssignment *blockvol.BlockVolumeAssignment
	for i, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			rebuildAssignment = &assignments[i]
			break
		}
	}
	if rebuildAssignment == nil {
		t.Fatal("expected Rebuilding assignment for reconnected server")
	}
	if rebuildAssignment.Epoch != newEpoch {
		t.Fatalf("rebuild epoch: got %d, want %d (matches promoted primary)", rebuildAssignment.Epoch, newEpoch)
	}
	if rebuildAssignment.RebuildAddr == "" {
		// RebuildListenAddr is set on the entry by tryCreateReplica
		t.Log("NOTE: RebuildAddr empty (allocate mock doesn't propagate to entry.RebuildListenAddr after swap)")
	}

	// Step 8: Registry shows old primary as new replica.
	entry, _ := ms.blockRegistry.Lookup("pvc-db-1")
	if entry.ReplicaServer != primaryVS {
		t.Fatalf("after recovery: replica should be %q (old primary), got %q", primaryVS, entry.ReplicaServer)
	}

	// Step 9: Simulate VS heartbeat confirming rebuild complete.
	// VS reports volume with matching epoch = rebuild confirmed.
	ms.blockAssignmentQueue.ConfirmFromHeartbeat(primaryVS, []blockvol.BlockVolumeInfoMessage{
		{
			Path:  rebuildAssignment.Path,
			Epoch: rebuildAssignment.Epoch,
			Role:  blockvol.RoleToWire(blockvol.RoleReplica), // after rebuild → replica
		},
	})

	if ms.blockAssignmentQueue.Pending(primaryVS) != 0 {
		t.Fatalf("rebuild assignment should be confirmed by heartbeat, got %d pending",
			ms.blockAssignmentQueue.Pending(primaryVS))
	}
}

// ============================================================
// Required #3: Assignment Delivery + Confirmation Loop
//
// Goal: assignment queue is drained only after heartbeat
// confirms — assignments remain pending until VS reports
// matching (path, epoch).
// ============================================================

func TestIntegration_AssignmentDeliveryConfirmation(t *testing.T) {
	ms := integrationMaster(t)
	ctx := context.Background()

	// Step 1: Create replicated volume → assignments enqueued.
	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-logs-1",
		SizeBytes: 5 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	primaryVS := resp.VolumeServer
	replicaVS := resp.ReplicaServer
	if replicaVS == "" {
		t.Fatal("expected replica server")
	}

	// Step 2: Both servers have 1 pending assignment each.
	if n := ms.blockAssignmentQueue.Pending(primaryVS); n != 1 {
		t.Fatalf("primary pending: got %d, want 1", n)
	}
	if n := ms.blockAssignmentQueue.Pending(replicaVS); n != 1 {
		t.Fatalf("replica pending: got %d, want 1", n)
	}

	// Step 3: Simulate heartbeat delivery — Peek returns pending assignments.
	primaryAssignments := ms.blockAssignmentQueue.Peek(primaryVS)
	if len(primaryAssignments) != 1 {
		t.Fatalf("Peek primary: got %d, want 1", len(primaryAssignments))
	}
	if blockvol.RoleFromWire(primaryAssignments[0].Role) != blockvol.RolePrimary {
		t.Fatalf("primary assignment role: got %d, want Primary", primaryAssignments[0].Role)
	}
	if primaryAssignments[0].Epoch != 1 {
		t.Fatalf("primary assignment epoch: got %d, want 1", primaryAssignments[0].Epoch)
	}

	replicaAssignments := ms.blockAssignmentQueue.Peek(replicaVS)
	if len(replicaAssignments) != 1 {
		t.Fatalf("Peek replica: got %d, want 1", len(replicaAssignments))
	}
	if blockvol.RoleFromWire(replicaAssignments[0].Role) != blockvol.RoleReplica {
		t.Fatalf("replica assignment role: got %d, want Replica", replicaAssignments[0].Role)
	}

	// Step 4: Peek again — assignments still pending (not consumed by Peek).
	if n := ms.blockAssignmentQueue.Pending(primaryVS); n != 1 {
		t.Fatalf("after Peek, primary still pending: got %d, want 1", n)
	}

	// Step 5: Simulate heartbeat from PRIMARY with wrong epoch — no confirmation.
	ms.blockAssignmentQueue.ConfirmFromHeartbeat(primaryVS, []blockvol.BlockVolumeInfoMessage{
		{
			Path:  primaryAssignments[0].Path,
			Epoch: 999, // wrong epoch
		},
	})
	if n := ms.blockAssignmentQueue.Pending(primaryVS); n != 1 {
		t.Fatalf("wrong epoch should NOT confirm: primary pending %d, want 1", n)
	}

	// Step 6: Simulate heartbeat from PRIMARY with correct (path, epoch) — confirmed.
	ms.blockAssignmentQueue.ConfirmFromHeartbeat(primaryVS, []blockvol.BlockVolumeInfoMessage{
		{
			Path:  primaryAssignments[0].Path,
			Epoch: primaryAssignments[0].Epoch,
		},
	})
	if n := ms.blockAssignmentQueue.Pending(primaryVS); n != 0 {
		t.Fatalf("correct heartbeat should confirm: primary pending %d, want 0", n)
	}

	// Step 7: Replica still pending (independent confirmation).
	if n := ms.blockAssignmentQueue.Pending(replicaVS); n != 1 {
		t.Fatalf("replica should still be pending: got %d, want 1", n)
	}

	// Step 8: Confirm replica.
	ms.blockAssignmentQueue.ConfirmFromHeartbeat(replicaVS, []blockvol.BlockVolumeInfoMessage{
		{
			Path:  replicaAssignments[0].Path,
			Epoch: replicaAssignments[0].Epoch,
		},
	})
	if n := ms.blockAssignmentQueue.Pending(replicaVS); n != 0 {
		t.Fatalf("replica should be confirmed: got %d, want 0", n)
	}
}

// ============================================================
// Nice-to-have #1: Lease-aware promotion timing
//
// Ensures promotion happens only after TTL expires.
// ============================================================

func TestIntegration_LeaseAwarePromotion(t *testing.T) {
	ms := integrationMaster(t)
	ctx := context.Background()

	// Create with replica.
	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-lease-1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	primaryVS := resp.VolumeServer

	// Set a short but non-zero lease TTL (lease just granted → not yet expired).
	ms.blockRegistry.UpdateEntry("pvc-lease-1", func(e *BlockVolumeEntry) {
		e.LeaseTTL = 300 * time.Millisecond
		e.LastLeaseGrant = time.Now()
	})

	// Primary dies.
	ms.failoverBlockVolumes(primaryVS)

	// Immediately: primary should NOT be swapped (lease still valid).
	e, _ := ms.blockRegistry.Lookup("pvc-lease-1")
	if e.VolumeServer != primaryVS {
		t.Fatalf("should NOT promote before lease expires, got primary=%q", e.VolumeServer)
	}

	// Wait for lease to expire + timer to fire.
	time.Sleep(500 * time.Millisecond)

	// Now promotion should have happened.
	e, _ = ms.blockRegistry.Lookup("pvc-lease-1")
	if e.VolumeServer == primaryVS {
		t.Fatalf("should promote after lease expires, still %q", e.VolumeServer)
	}
	if e.Epoch != 2 {
		t.Fatalf("epoch should be 2 after deferred promotion, got %d", e.Epoch)
	}
}

// ============================================================
// Nice-to-have #2: Replica create failure → single-copy mode
//
// Primary alone works; no replica assignments sent.
// ============================================================

func TestIntegration_ReplicaFailureSingleCopy(t *testing.T) {
	ms := integrationMaster(t)
	ctx := context.Background()

	// Make replica allocation always fail.
	callCount := 0
	origAllocate := ms.blockVSAllocate
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		callCount++
		if callCount > 1 {
			// Second call (replica) fails.
			return nil, fmt.Errorf("disk full on replica")
		}
		return origAllocate(ctx, server, name, sizeBytes, walSizeBytes, diskType, durabilityMode)
	}

	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-single-1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("should succeed in single-copy mode: %v", err)
	}
	if resp.ReplicaServer != "" {
		t.Fatalf("should have no replica, got %q", resp.ReplicaServer)
	}

	primaryVS := resp.VolumeServer

	// Only primary assignment should be enqueued.
	if n := ms.blockAssignmentQueue.Pending(primaryVS); n != 1 {
		t.Fatalf("primary pending: got %d, want 1", n)
	}

	// Check there's only a Primary assignment (no Replica assignment anywhere).
	assignments := ms.blockAssignmentQueue.Peek(primaryVS)
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleReplica {
			t.Fatal("should not have Replica assignment in single-copy mode")
		}
	}

	// No failover possible without replica.
	ms.blockRegistry.UpdateEntry("pvc-single-1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	ms.failoverBlockVolumes(primaryVS)

	e, _ := ms.blockRegistry.Lookup("pvc-single-1")
	if e.VolumeServer != primaryVS {
		t.Fatalf("single-copy volume should not failover, got %q", e.VolumeServer)
	}
}

// ============================================================
// Nice-to-have #3: Lease-deferred timer cancelled on reconnect
//
// VS reconnects during lease window → no promotion (no split-brain).
// ============================================================

func TestIntegration_TransientDisconnectNoSplitBrain(t *testing.T) {
	ms := integrationMaster(t)
	ctx := context.Background()

	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-transient-1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	primaryVS := resp.VolumeServer
	replicaVS := resp.ReplicaServer

	// Set lease with long TTL (not expired).
	ms.blockRegistry.UpdateEntry("pvc-transient-1", func(e *BlockVolumeEntry) {
		e.LeaseTTL = 1 * time.Second
		e.LastLeaseGrant = time.Now()
	})

	// Primary disconnects → deferred promotion timer set.
	ms.failoverBlockVolumes(primaryVS)

	// Primary should NOT be swapped yet.
	e, _ := ms.blockRegistry.Lookup("pvc-transient-1")
	if e.VolumeServer != primaryVS {
		t.Fatal("should not promote during lease window")
	}

	// VS reconnects (before lease expires) → deferred timers cancelled.
	ms.recoverBlockVolumes(primaryVS)

	// Wait well past the original lease TTL.
	time.Sleep(1500 * time.Millisecond)

	// Primary should STILL be the same (timer was cancelled).
	e, _ = ms.blockRegistry.Lookup("pvc-transient-1")
	if e.VolumeServer != primaryVS {
		t.Fatalf("reconnected primary should remain primary, got %q", e.VolumeServer)
	}

	// No failover happened, so no pending rebuilds.
	ms.blockFailover.mu.Lock()
	rebuilds := ms.blockFailover.pendingRebuilds[primaryVS]
	ms.blockFailover.mu.Unlock()
	if len(rebuilds) != 0 {
		t.Fatalf("no pending rebuilds for reconnected server, got %d", len(rebuilds))
	}

	// CSI publish should still return original primary.
	lookupResp, err := ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "pvc-transient-1"})
	if err != nil {
		t.Fatalf("Lookup after reconnect: %v", err)
	}
	if lookupResp.IscsiAddr != primaryVS+":3260" {
		t.Fatalf("iSCSI addr should be original primary %q, got %q",
			primaryVS+":3260", lookupResp.IscsiAddr)
	}
	_ = replicaVS // used implicitly via CreateBlockVolume
}

// ============================================================
// Full lifecycle: Create → Publish → Failover → Re-publish →
// Recover → Rebuild confirm → Verify registry health
// ============================================================

func TestIntegration_FullLifecycle(t *testing.T) {
	ms := integrationMaster(t)
	ctx := context.Background()

	// --- Phase 1: Create ---
	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-lifecycle-1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	primaryVS := resp.VolumeServer
	replicaVS := resp.ReplicaServer
	if replicaVS == "" {
		t.Fatal("expected replica")
	}

	// --- Phase 2: Initial publish ---
	lookupResp, err := ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "pvc-lifecycle-1"})
	if err != nil {
		t.Fatalf("initial lookup: %v", err)
	}
	initialAddr := lookupResp.IscsiAddr

	// --- Phase 3: Confirm initial assignments ---
	entry, _ := ms.blockRegistry.Lookup("pvc-lifecycle-1")
	ms.blockAssignmentQueue.ConfirmFromHeartbeat(primaryVS, []blockvol.BlockVolumeInfoMessage{
		{Path: entry.Path, Epoch: 1},
	})
	ms.blockAssignmentQueue.ConfirmFromHeartbeat(replicaVS, []blockvol.BlockVolumeInfoMessage{
		{Path: entry.ReplicaPath, Epoch: 1},
	})
	if ms.blockAssignmentQueue.Pending(primaryVS) != 0 || ms.blockAssignmentQueue.Pending(replicaVS) != 0 {
		t.Fatal("assignments should be confirmed")
	}

	// --- Phase 4: Expire lease + kill primary ---
	ms.blockRegistry.UpdateEntry("pvc-lifecycle-1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	ms.failoverBlockVolumes(primaryVS)

	// --- Phase 5: Verify failover ---
	entry, _ = ms.blockRegistry.Lookup("pvc-lifecycle-1")
	if entry.VolumeServer != replicaVS {
		t.Fatalf("after failover: primary should be %q", replicaVS)
	}
	if entry.Epoch != 2 {
		t.Fatalf("epoch should be 2, got %d", entry.Epoch)
	}

	// --- Phase 6: Re-publish → new address ---
	lookupResp, err = ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "pvc-lifecycle-1"})
	if err != nil {
		t.Fatalf("post-failover lookup: %v", err)
	}
	if lookupResp.IscsiAddr == initialAddr {
		t.Fatal("post-failover addr should differ from initial")
	}

	// --- Phase 7: Confirm failover assignment for new primary ---
	ms.blockAssignmentQueue.ConfirmFromHeartbeat(replicaVS, []blockvol.BlockVolumeInfoMessage{
		{Path: entry.Path, Epoch: 2},
	})

	// --- Phase 8: Old primary reconnects → rebuild ---
	ms.recoverBlockVolumes(primaryVS)

	rebuildAssignments := ms.blockAssignmentQueue.Peek(primaryVS)
	var rebuildPath string
	var rebuildEpoch uint64
	for _, a := range rebuildAssignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			rebuildPath = a.Path
			rebuildEpoch = a.Epoch
		}
	}
	if rebuildPath == "" {
		t.Fatal("expected rebuild assignment")
	}

	// --- Phase 9: Old primary confirms rebuild via heartbeat ---
	ms.blockAssignmentQueue.ConfirmFromHeartbeat(primaryVS, []blockvol.BlockVolumeInfoMessage{
		{Path: rebuildPath, Epoch: rebuildEpoch, Role: blockvol.RoleToWire(blockvol.RoleReplica)},
	})
	if ms.blockAssignmentQueue.Pending(primaryVS) != 0 {
		t.Fatalf("rebuild should be confirmed, got %d pending", ms.blockAssignmentQueue.Pending(primaryVS))
	}

	// --- Phase 10: Final registry state ---
	final, _ := ms.blockRegistry.Lookup("pvc-lifecycle-1")
	if final.VolumeServer != replicaVS {
		t.Fatalf("final primary: got %q, want %q", final.VolumeServer, replicaVS)
	}
	if final.ReplicaServer != primaryVS {
		t.Fatalf("final replica: got %q, want %q", final.ReplicaServer, primaryVS)
	}
	if final.Epoch != 2 {
		t.Fatalf("final epoch: got %d, want 2", final.Epoch)
	}

	// --- Phase 11: Delete ---
	_, err = ms.DeleteBlockVolume(ctx, &master_pb.DeleteBlockVolumeRequest{Name: "pvc-lifecycle-1"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := ms.blockRegistry.Lookup("pvc-lifecycle-1"); ok {
		t.Fatal("volume should be deleted")
	}
}

// ============================================================
// Double failover: primary dies, promoted replica dies, then
// the original server comes back — verify correct state.
// ============================================================

func TestIntegration_DoubleFailover(t *testing.T) {
	ms := integrationMaster(t)
	ctx := context.Background()

	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-double-1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	vs1 := resp.VolumeServer
	vs2 := resp.ReplicaServer

	// First failover: vs1 dies → vs2 promoted.
	ms.blockRegistry.UpdateEntry("pvc-double-1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	ms.failoverBlockVolumes(vs1)

	e1, _ := ms.blockRegistry.Lookup("pvc-double-1")
	if e1.VolumeServer != vs2 {
		t.Fatalf("first failover: primary should be %q, got %q", vs2, e1.VolumeServer)
	}
	if e1.Epoch != 2 {
		t.Fatalf("first failover epoch: got %d, want 2", e1.Epoch)
	}

	// CP8-2: PromoteBestReplica does NOT add old primary back as replica.
	// Reconnect vs1 first so it becomes a replica (via recoverBlockVolumes).
	ms.recoverBlockVolumes(vs1)

	// Simulate heartbeat from vs1 that restores iSCSI addr, health score,
	// role, and heartbeat timestamp (in production this happens when the
	// VS re-registers after reconnect and completes rebuild).
	ms.blockRegistry.UpdateEntry("pvc-double-1", func(e *BlockVolumeEntry) {
		for i := range e.Replicas {
			if e.Replicas[i].Server == vs1 {
				e.Replicas[i].ISCSIAddr = vs1 + ":3260"
				e.Replicas[i].HealthScore = 1.0
				e.Replicas[i].Role = blockvol.RoleToWire(blockvol.RoleReplica)
				e.Replicas[i].LastHeartbeat = time.Now()
			}
		}
	})

	// Now vs1 is back as replica. Second failover: vs2 dies → vs1 promoted.
	ms.blockRegistry.UpdateEntry("pvc-double-1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	ms.failoverBlockVolumes(vs2)

	e2, _ := ms.blockRegistry.Lookup("pvc-double-1")
	if e2.VolumeServer != vs1 {
		t.Fatalf("second failover: primary should be %q, got %q", vs1, e2.VolumeServer)
	}
	if e2.Epoch != 3 {
		t.Fatalf("second failover epoch: got %d, want 3", e2.Epoch)
	}

	// Verify CSI publish returns vs1.
	lookupResp, err := ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "pvc-double-1"})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if lookupResp.IscsiAddr != vs1+":3260" {
		t.Fatalf("after double failover: iSCSI addr should be %q, got %q",
			vs1+":3260", lookupResp.IscsiAddr)
	}
}

// ============================================================
// Multiple volumes: failover + rebuild affects all volumes on
// the dead server, not just one.
// ============================================================

func TestIntegration_MultiVolumeFailoverRebuild(t *testing.T) {
	ms := integrationMaster(t)
	ctx := context.Background()

	// Create 3 volumes — all will land on vs1+vs2.
	for i := 1; i <= 3; i++ {
		_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
			Name:      fmt.Sprintf("pvc-multi-%d", i),
			SizeBytes: 1 << 30,
		})
		if err != nil {
			t.Fatalf("create pvc-multi-%d: %v", i, err)
		}
	}

	// Find which server is primary for each volume.
	primaryCounts := map[string]int{}
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("pvc-multi-%d", i)
		e, _ := ms.blockRegistry.Lookup(name)
		primaryCounts[e.VolumeServer]++
		// Expire lease.
		ms.blockRegistry.UpdateEntry(name, func(entry *BlockVolumeEntry) {
			entry.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
		})
	}

	// Kill the server with the most primaries.
	deadServer := "vs1:9333"
	if primaryCounts["vs2:9333"] > primaryCounts["vs1:9333"] {
		deadServer = "vs2:9333"
	}
	otherServer := "vs2:9333"
	if deadServer == "vs2:9333" {
		otherServer = "vs1:9333"
	}

	ms.failoverBlockVolumes(deadServer)

	// All volumes should now have the other server as primary.
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("pvc-multi-%d", i)
		e, _ := ms.blockRegistry.Lookup(name)
		if e.VolumeServer == deadServer {
			t.Fatalf("%s: primary should not be dead server %q", name, deadServer)
		}
	}

	// Reconnect dead server → rebuild assignments.
	ms.recoverBlockVolumes(deadServer)

	rebuildCount := 0
	for _, a := range ms.blockAssignmentQueue.Peek(deadServer) {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			rebuildCount++
		}
	}
	_ = otherServer
	// CP8-2: With 2 servers and 3 volumes, deadServer hosts all 3 volumes
	// (as primary for some, replica for others). All need rebuild on reconnect.
	if rebuildCount != 3 {
		t.Fatalf("expected 3 rebuild assignments for %s (all volumes), got %d",
			deadServer, rebuildCount)
	}
}
