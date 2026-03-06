package weed_server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// QA helpers
// ============================================================

// testMSForQA creates a MasterServer with full failover support for adversarial tests.
func testMSForQA(t *testing.T) *MasterServer {
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
			ISCSIAddr: server + ":3260",
		}, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		return nil
	}
	return ms
}

// registerQAVolume creates a volume entry with optional replica, configurable lease state.
func registerQAVolume(t *testing.T, ms *MasterServer, name, primary, replica string, epoch uint64, leaseTTL time.Duration, leaseExpired bool) {
	t.Helper()
	entry := &BlockVolumeEntry{
		Name:         name,
		VolumeServer: primary,
		Path:         fmt.Sprintf("/data/%s.blk", name),
		IQN:          fmt.Sprintf("iqn.2024.test:%s", name),
		ISCSIAddr:    primary + ":3260",
		SizeBytes:    1 << 30,
		Epoch:        epoch,
		Role:         blockvol.RoleToWire(blockvol.RolePrimary),
		Status:       StatusActive,
		LeaseTTL:     leaseTTL,
	}
	if leaseExpired {
		entry.LastLeaseGrant = time.Now().Add(-2 * leaseTTL)
	} else {
		entry.LastLeaseGrant = time.Now()
	}
	if replica != "" {
		entry.ReplicaServer = replica
		entry.ReplicaPath = fmt.Sprintf("/data/%s.blk", name)
		entry.ReplicaIQN = fmt.Sprintf("iqn.2024.test:%s-r", name)
		entry.ReplicaISCSIAddr = replica + ":3260"
		// CP8-2: also populate Replicas[].
		entry.Replicas = []ReplicaInfo{
			{
				Server:      replica,
				Path:        fmt.Sprintf("/data/%s.blk", name),
				IQN:         fmt.Sprintf("iqn.2024.test:%s-r", name),
				ISCSIAddr:   replica + ":3260",
				HealthScore: 1.0,
			},
		}
	}
	if err := ms.blockRegistry.Register(entry); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
}

// ============================================================
// A. Assignment Queue Adversarial
// ============================================================

func TestQA_Queue_ConfirmWrongEpoch(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 5, 1))

	// Confirm with wrong epoch should NOT remove.
	q.Confirm("s1", "/a.blk", 4)
	if q.Pending("s1") != 1 {
		t.Fatal("wrong-epoch confirm should not remove")
	}
	q.Confirm("s1", "/a.blk", 6)
	if q.Pending("s1") != 1 {
		t.Fatal("higher-epoch confirm should not remove")
	}
	// Correct epoch should remove.
	q.Confirm("s1", "/a.blk", 5)
	if q.Pending("s1") != 0 {
		t.Fatal("exact-epoch confirm should remove")
	}
}

func TestQA_Queue_HeartbeatPartialConfirm(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 5, 1))
	q.Enqueue("s1", mkAssign("/b.blk", 3, 2))

	// Heartbeat confirms only /a.blk@5, not /b.blk.
	q.ConfirmFromHeartbeat("s1", []blockvol.BlockVolumeInfoMessage{
		{Path: "/a.blk", Epoch: 5},
		{Path: "/c.blk", Epoch: 99}, // unknown path, no effect
	})
	if q.Pending("s1") != 1 {
		t.Fatalf("expected 1 remaining, got %d", q.Pending("s1"))
	}
	got := q.Peek("s1")
	if got[0].Path != "/b.blk" {
		t.Fatalf("wrong remaining: %v", got)
	}
}

func TestQA_Queue_HeartbeatWrongEpochNoConfirm(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 5, 1))

	// Heartbeat with same path but different epoch: should NOT confirm.
	q.ConfirmFromHeartbeat("s1", []blockvol.BlockVolumeInfoMessage{
		{Path: "/a.blk", Epoch: 4},
	})
	if q.Pending("s1") != 1 {
		t.Fatal("wrong-epoch heartbeat should not confirm")
	}
}

func TestQA_Queue_SamePathSameEpochDifferentRoles(t *testing.T) {
	q := NewBlockAssignmentQueue()
	// Edge case: same path+epoch but different roles (shouldn't happen in practice).
	q.Enqueue("s1", blockvol.BlockVolumeAssignment{Path: "/a.blk", Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary)})
	q.Enqueue("s1", blockvol.BlockVolumeAssignment{Path: "/a.blk", Epoch: 1, Role: blockvol.RoleToWire(blockvol.RoleReplica)})

	// Peek should NOT prune either (same epoch).
	got := q.Peek("s1")
	if len(got) != 2 {
		t.Fatalf("expected 2 (same epoch, different roles), got %d", len(got))
	}
}

func TestQA_Queue_ConfirmOnUnknownServer(t *testing.T) {
	q := NewBlockAssignmentQueue()
	// Confirm on a server with no queue should not panic.
	q.Confirm("unknown", "/a.blk", 1)
	q.ConfirmFromHeartbeat("unknown", []blockvol.BlockVolumeInfoMessage{{Path: "/a.blk", Epoch: 1}})
}

func TestQA_Queue_PeekReturnsCopy(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.Enqueue("s1", mkAssign("/a.blk", 1, 1))

	got := q.Peek("s1")
	// Mutate the returned copy.
	got[0].Path = "/MUTATED"

	// Original should be unchanged.
	got2 := q.Peek("s1")
	if got2[0].Path == "/MUTATED" {
		t.Fatal("Peek should return a copy, not a reference to internal state")
	}
}

func TestQA_Queue_ConcurrentEnqueueConfirmPeek(t *testing.T) {
	q := NewBlockAssignmentQueue()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			q.Enqueue("s1", mkAssign(fmt.Sprintf("/v%d.blk", i), uint64(i+1), 1))
		}(i)
		go func(i int) {
			defer wg.Done()
			q.Confirm("s1", fmt.Sprintf("/v%d.blk", i), uint64(i+1))
		}(i)
		go func() {
			defer wg.Done()
			q.Peek("s1")
		}()
	}
	wg.Wait()
	// No panics, no races.
}

// ============================================================
// B. Registry Adversarial
// ============================================================

func TestQA_Reg_DoubleSwap(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		IQN: "iqn:vol1", ISCSIAddr: "vs1:3260", SizeBytes: 1 << 30,
		Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaServer: "vs2", ReplicaPath: "/data/vol1.blk",
		ReplicaIQN: "iqn:vol1-r", ReplicaISCSIAddr: "vs2:3260",
	})

	// First swap: vs1->vs2, epoch 2.
	ep1, err := r.SwapPrimaryReplica("vol1")
	if err != nil {
		t.Fatal(err)
	}
	if ep1 != 2 {
		t.Fatalf("first swap epoch: got %d, want 2", ep1)
	}

	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "vs2" || e.ReplicaServer != "vs1" {
		t.Fatalf("after first swap: primary=%s replica=%s", e.VolumeServer, e.ReplicaServer)
	}

	// Second swap: vs2->vs1, epoch 3.
	ep2, err := r.SwapPrimaryReplica("vol1")
	if err != nil {
		t.Fatal(err)
	}
	if ep2 != 3 {
		t.Fatalf("second swap epoch: got %d, want 3", ep2)
	}

	e, _ = r.Lookup("vol1")
	if e.VolumeServer != "vs1" || e.ReplicaServer != "vs2" {
		t.Fatalf("after double swap: primary=%s replica=%s (should be back to original)", e.VolumeServer, e.ReplicaServer)
	}
}

func TestQA_Reg_SwapNoReplica(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
	})

	_, err := r.SwapPrimaryReplica("vol1")
	if err == nil {
		t.Fatal("swap with no replica should error")
	}
}

func TestQA_Reg_SwapNotFound(t *testing.T) {
	r := NewBlockVolumeRegistry()
	_, err := r.SwapPrimaryReplica("nonexistent")
	if err == nil {
		t.Fatal("swap nonexistent should error")
	}
}

func TestQA_Reg_ConcurrentSwapAndLookup(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		IQN: "iqn:vol1", ISCSIAddr: "vs1:3260", Epoch: 1,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaServer: "vs2", ReplicaPath: "/data/vol1.blk",
		ReplicaIQN: "iqn:vol1-r", ReplicaISCSIAddr: "vs2:3260",
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.SwapPrimaryReplica("vol1")
		}()
		go func() {
			defer wg.Done()
			r.Lookup("vol1")
		}()
	}
	wg.Wait()
	// No panics or races.
}

func TestQA_Reg_SetReplicaTwice_ReplacesOld(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
	})

	// Set replica to vs2.
	r.SetReplica("vol1", "vs2", "/data/vol1.blk", "vs2:3260", "iqn:vol1-r")
	// Replace with vs3.
	r.SetReplica("vol1", "vs3", "/data/vol1.blk", "vs3:3260", "iqn:vol1-r2")

	e, _ := r.Lookup("vol1")
	if e.ReplicaServer != "vs3" {
		t.Fatalf("replica should be vs3, got %s", e.ReplicaServer)
	}

	// vs3 should be in byServer index.
	entries := r.ListByServer("vs3")
	if len(entries) != 1 {
		t.Fatalf("vs3 should have 1 entry, got %d", len(entries))
	}

	// BUG CHECK: vs2 should be removed from byServer when replaced.
	// SetReplica doesn't remove the old replica server from byServer.
	entries2 := r.ListByServer("vs2")
	if len(entries2) != 0 {
		t.Fatalf("BUG: vs2 still in byServer after replica replaced (got %d entries)", len(entries2))
	}
}

func TestQA_Reg_FullHeartbeatDoesNotClobberReplicaServer(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status:        StatusPending,
		ReplicaServer: "vs2", ReplicaPath: "/data/vol1.blk",
	})

	// Full heartbeat from vs1 — should NOT clear replica info.
	r.UpdateFullHeartbeat("vs1", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/data/vol1.blk", Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary), VolumeSize: 1 << 30},
	})

	e, _ := r.Lookup("vol1")
	if e.ReplicaServer != "vs2" {
		t.Fatalf("full heartbeat clobbered ReplicaServer: got %q, want vs2", e.ReplicaServer)
	}
}

func TestQA_Reg_ListByServerIncludesBothPrimaryAndReplica(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
	})
	r.SetReplica("vol1", "vs2", "/data/vol1.blk", "", "")

	// ListByServer should return vol1 for BOTH vs1 and vs2.
	for _, server := range []string{"vs1", "vs2"} {
		entries := r.ListByServer(server)
		if len(entries) != 1 || entries[0].Name != "vol1" {
			t.Fatalf("ListByServer(%q) should return vol1, got %d entries", server, len(entries))
		}
	}
}

// ============================================================
// C. Failover Adversarial
// ============================================================

func TestQA_Failover_DeferredCancelledOnReconnect(t *testing.T) {
	ms := testMSForQA(t)
	registerQAVolume(t, ms, "vol1", "vs1", "vs2", 1, 500*time.Millisecond, false) // lease NOT expired

	// Disconnect vs1 — deferred promotion scheduled.
	ms.failoverBlockVolumes("vs1")

	// vs1 should still be primary (lease not expired).
	e, _ := ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs1" {
		t.Fatalf("premature promotion: primary=%s", e.VolumeServer)
	}

	// vs1 reconnects before timer fires.
	ms.recoverBlockVolumes("vs1")

	// Wait well past the original lease expiry.
	time.Sleep(800 * time.Millisecond)

	// Promotion should NOT have happened (timer was cancelled).
	e, _ = ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs1" {
		t.Fatalf("BUG: promotion happened after reconnect (primary=%s, want vs1)", e.VolumeServer)
	}
}

func TestQA_Failover_DoubleDisconnect_NoPanic(t *testing.T) {
	ms := testMSForQA(t)
	registerQAVolume(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second, true)

	ms.failoverBlockVolumes("vs1")
	// Second failover for same server after promotion — should not panic.
	ms.failoverBlockVolumes("vs1")
}

func TestQA_Failover_PromoteIdempotent_NoReplicaAfterFirstSwap(t *testing.T) {
	ms := testMSForQA(t)
	registerQAVolume(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second, true)

	ms.failoverBlockVolumes("vs1") // promotes vs2

	// CP8-2: PromoteBestReplica does NOT add old primary back as replica.
	// Reconnect vs1 first so it becomes a replica.
	ms.recoverBlockVolumes("vs1")

	e, _ := ms.blockRegistry.Lookup("vol1")
	e.LastLeaseGrant = time.Now().Add(-1 * time.Minute) // expire the new lease
	ms.failoverBlockVolumes("vs2")

	e, _ = ms.blockRegistry.Lookup("vol1")
	// After double failover: should swap back to vs1 as primary.
	if e.VolumeServer != "vs1" {
		t.Fatalf("double failover: primary=%s, want vs1", e.VolumeServer)
	}
	if e.Epoch != 3 {
		t.Fatalf("double failover: epoch=%d, want 3", e.Epoch)
	}
}

func TestQA_Failover_MixedLeaseStates(t *testing.T) {
	ms := testMSForQA(t)
	// vol1: lease expired (immediate promotion).
	registerQAVolume(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second, true)
	// vol2: lease NOT expired (deferred).
	registerQAVolume(t, ms, "vol2", "vs1", "vs3", 2, 500*time.Millisecond, false)

	ms.failoverBlockVolumes("vs1")

	// vol1: immediately promoted.
	e1, _ := ms.blockRegistry.Lookup("vol1")
	if e1.VolumeServer != "vs2" {
		t.Fatalf("vol1: expected immediate promotion, got primary=%s", e1.VolumeServer)
	}

	// vol2: NOT yet promoted.
	e2, _ := ms.blockRegistry.Lookup("vol2")
	if e2.VolumeServer != "vs1" {
		t.Fatalf("vol2: premature promotion, got primary=%s", e2.VolumeServer)
	}

	// Wait for vol2's deferred timer.
	time.Sleep(700 * time.Millisecond)
	e2, _ = ms.blockRegistry.Lookup("vol2")
	if e2.VolumeServer != "vs3" {
		t.Fatalf("vol2: deferred promotion failed, got primary=%s", e2.VolumeServer)
	}
}

func TestQA_Failover_NoRegistryNoPanic(t *testing.T) {
	ms := &MasterServer{} // no registry
	ms.failoverBlockVolumes("vs1")
	// Should not panic.
}

func TestQA_Failover_VolumeDeletedDuringDeferredTimer(t *testing.T) {
	ms := testMSForQA(t)
	registerQAVolume(t, ms, "vol1", "vs1", "vs2", 1, 200*time.Millisecond, false)

	ms.failoverBlockVolumes("vs1")

	// Delete the volume while timer is pending.
	ms.blockRegistry.Unregister("vol1")

	// Wait for timer to fire.
	time.Sleep(400 * time.Millisecond)

	// promoteReplica should gracefully handle missing volume (no panic).
	_, ok := ms.blockRegistry.Lookup("vol1")
	if ok {
		t.Fatal("volume should have been deleted")
	}
}

func TestQA_Failover_ConcurrentFailoverDifferentServers(t *testing.T) {
	ms := testMSForQA(t)
	// vol1: primary=vs1, replica=vs2
	registerQAVolume(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second, true)
	// vol2: primary=vs3, replica=vs4
	registerQAVolume(t, ms, "vol2", "vs3", "vs4", 1, 5*time.Second, true)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); ms.failoverBlockVolumes("vs1") }()
	go func() { defer wg.Done(); ms.failoverBlockVolumes("vs3") }()
	wg.Wait()

	e1, _ := ms.blockRegistry.Lookup("vol1")
	if e1.VolumeServer != "vs2" {
		t.Fatalf("vol1: primary=%s, want vs2", e1.VolumeServer)
	}
	e2, _ := ms.blockRegistry.Lookup("vol2")
	if e2.VolumeServer != "vs4" {
		t.Fatalf("vol2: primary=%s, want vs4", e2.VolumeServer)
	}
}

// ============================================================
// D. CreateBlockVolume + Failover Adversarial
// ============================================================

func TestQA_Create_LeaseNonZero_ImmediateFailoverSafe(t *testing.T) {
	ms := testMSForQA(t)
	ms.blockFailover = newBlockFailoverState()
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")

	// Create volume.
	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "vol1", SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Immediately failover the primary.
	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.LastLeaseGrant.IsZero() {
		t.Fatal("BUG: LastLeaseGrant is zero after Create (F1 regression)")
	}

	// Verify that lease is recent (within last second).
	if time.Since(entry.LastLeaseGrant) > 1*time.Second {
		t.Fatalf("LastLeaseGrant too old: %v", entry.LastLeaseGrant)
	}

	_ = resp
}

func TestQA_Create_ReplicaDeleteOnVolDelete(t *testing.T) {
	ms := testMSForQA(t)
	ms.blockFailover = newBlockFailoverState()
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")

	var deleteCalls sync.Map // server -> count

	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		v, _ := deleteCalls.LoadOrStore(server, new(atomic.Int32))
		v.(*atomic.Int32).Add(1)
		return nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "vol1", SizeBytes: 1 << 30,
	})

	entry, _ := ms.blockRegistry.Lookup("vol1")
	hasReplica := entry.ReplicaServer != ""

	// Delete volume.
	ms.DeleteBlockVolume(context.Background(), &master_pb.DeleteBlockVolumeRequest{Name: "vol1"})

	// Verify primary delete was called.
	v, ok := deleteCalls.Load(entry.VolumeServer)
	if !ok || v.(*atomic.Int32).Load() != 1 {
		t.Fatal("primary delete not called")
	}

	// If replica existed, verify replica delete was also called (F4 regression).
	if hasReplica {
		v, ok := deleteCalls.Load(entry.ReplicaServer)
		if !ok || v.(*atomic.Int32).Load() != 1 {
			t.Fatal("BUG: replica delete not called (F4 regression)")
		}
	}
}

func TestQA_Create_ReplicaDeleteFailure_PrimaryStillDeleted(t *testing.T) {
	ms := testMSForQA(t)
	ms.blockFailover = newBlockFailoverState()
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "vol1", SizeBytes: 1 << 30,
	})

	// Find the replica server and make its delete fail.
	entry, _ := ms.blockRegistry.Lookup("vol1")
	replicaServer := entry.ReplicaServer
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		if server == replicaServer {
			return fmt.Errorf("replica down")
		}
		return nil
	}

	// Delete should succeed even if replica delete fails (best-effort).
	_, err := ms.DeleteBlockVolume(context.Background(), &master_pb.DeleteBlockVolumeRequest{Name: "vol1"})
	if err != nil {
		t.Fatalf("delete should succeed despite replica failure: %v", err)
	}

	// Volume should be unregistered.
	_, ok := ms.blockRegistry.Lookup("vol1")
	if ok {
		t.Fatal("volume should be unregistered after delete")
	}
}

// ============================================================
// E. Rebuild Adversarial
// ============================================================

func TestQA_Rebuild_DoubleReconnect_NoDuplicateAssignments(t *testing.T) {
	ms := testMSForQA(t)
	registerQAVolume(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second, true)

	ms.failoverBlockVolumes("vs1")

	// First reconnect.
	ms.recoverBlockVolumes("vs1")
	pending1 := ms.blockAssignmentQueue.Pending("vs1")

	// Second reconnect — should NOT add duplicate rebuild assignments.
	ms.recoverBlockVolumes("vs1")
	pending2 := ms.blockAssignmentQueue.Pending("vs1")

	if pending2 != pending1 {
		t.Fatalf("double reconnect added duplicate assignments: %d -> %d", pending1, pending2)
	}
}

func TestQA_Rebuild_RecoverNilFailoverState(t *testing.T) {
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        nil, // nil
	}
	// Should not panic.
	ms.recoverBlockVolumes("vs1")
	ms.drainPendingRebuilds("vs1")
	ms.recordPendingRebuild("vs1", pendingRebuild{})
}

func TestQA_Rebuild_FullCycle_CreateFailoverRecoverRebuild(t *testing.T) {
	ms := testMSForQA(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")

	// Create volume.
	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "vol1", SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	primary := resp.VolumeServer
	replica := resp.ReplicaServer
	if replica == "" {
		t.Skip("no replica created (single server)")
	}

	// Expire lease.
	entry, _ := ms.blockRegistry.Lookup("vol1")
	entry.LastLeaseGrant = time.Now().Add(-1 * time.Minute)

	// Primary disconnects.
	ms.failoverBlockVolumes(primary)

	// Verify promotion.
	entry, _ = ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != replica {
		t.Fatalf("expected promotion to %s, got %s", replica, entry.VolumeServer)
	}
	if entry.Epoch != 2 {
		t.Fatalf("expected epoch 2, got %d", entry.Epoch)
	}

	// Old primary reconnects.
	ms.recoverBlockVolumes(primary)

	// Verify rebuild assignment for old primary.
	assignments := ms.blockAssignmentQueue.Peek(primary)
	foundRebuild := false
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			foundRebuild = true
			if a.Epoch != entry.Epoch {
				t.Fatalf("rebuild epoch: got %d, want %d", a.Epoch, entry.Epoch)
			}
		}
	}
	if !foundRebuild {
		t.Fatal("no rebuild assignment found for reconnected server")
	}

	// Verify registry: old primary is now the replica.
	entry, _ = ms.blockRegistry.Lookup("vol1")
	if entry.ReplicaServer != primary {
		t.Fatalf("old primary should be replica, got %s", entry.ReplicaServer)
	}
}

// ============================================================
// F. Queue + Failover Integration
// ============================================================

func TestQA_FailoverEnqueuesNewPrimaryAssignment(t *testing.T) {
	ms := testMSForQA(t)
	registerQAVolume(t, ms, "vol1", "vs1", "vs2", 5, 5*time.Second, true)

	ms.failoverBlockVolumes("vs1")

	// vs2 (new primary) should have an assignment with epoch=6, role=Primary.
	assignments := ms.blockAssignmentQueue.Peek("vs2")
	found := false
	for _, a := range assignments {
		if a.Epoch == 6 && blockvol.RoleFromWire(a.Role) == blockvol.RolePrimary {
			found = true
			if a.LeaseTtlMs == 0 {
				t.Fatal("assignment should have non-zero LeaseTtlMs")
			}
		}
	}
	if !found {
		t.Fatalf("expected Primary assignment with epoch=6 for vs2, got: %+v", assignments)
	}
}

func TestQA_HeartbeatConfirmsFailoverAssignment(t *testing.T) {
	ms := testMSForQA(t)
	registerQAVolume(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second, true)

	ms.failoverBlockVolumes("vs1")

	// Simulate vs2 heartbeat confirming the promotion.
	entry, _ := ms.blockRegistry.Lookup("vol1")
	ms.blockAssignmentQueue.ConfirmFromHeartbeat("vs2", []blockvol.BlockVolumeInfoMessage{
		{Path: entry.Path, Epoch: entry.Epoch},
	})

	if ms.blockAssignmentQueue.Pending("vs2") != 0 {
		t.Fatal("heartbeat should have confirmed the failover assignment")
	}
}

// ============================================================
// G. Edge Cases
// ============================================================

func TestQA_SwapEpochMonotonicallyIncreasing(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/p1", IQN: "iqn1", ISCSIAddr: "vs1:3260",
		Epoch: 100, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaServer: "vs2", ReplicaPath: "/p2", ReplicaIQN: "iqn2", ReplicaISCSIAddr: "vs2:3260",
	})

	var prevEpoch uint64 = 100
	for i := 0; i < 10; i++ {
		ep, err := r.SwapPrimaryReplica("vol1")
		if err != nil {
			t.Fatal(err)
		}
		if ep <= prevEpoch {
			t.Fatalf("swap %d: epoch %d not > previous %d", i, ep, prevEpoch)
		}
		prevEpoch = ep
	}
}

func TestQA_CancelDeferredTimers_NoPendingRebuilds(t *testing.T) {
	ms := testMSForQA(t)
	// Cancel with no timers — should not panic.
	ms.cancelDeferredTimers("vs1")
}

func TestQA_Failover_ReplicaServerDies_PrimaryUntouched(t *testing.T) {
	ms := testMSForQA(t)
	registerQAVolume(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second, true)

	// vs2 is the REPLICA, not primary. Failover should not promote.
	ms.failoverBlockVolumes("vs2")

	e, _ := ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs1" {
		t.Fatalf("primary should remain vs1, got %s", e.VolumeServer)
	}
	if e.Epoch != 1 {
		t.Fatalf("epoch should remain 1, got %d", e.Epoch)
	}
}

func TestQA_Queue_EnqueueBatchEmpty(t *testing.T) {
	q := NewBlockAssignmentQueue()
	q.EnqueueBatch("s1", nil)
	q.EnqueueBatch("s1", []blockvol.BlockVolumeAssignment{})
	if q.Pending("s1") != 0 {
		t.Fatal("empty batch should not add anything")
	}
}
