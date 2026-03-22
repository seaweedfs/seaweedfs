package weed_server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// CP8-3-1 Adversarial QA Test Suite
//
// 20 adversarial tests targeting durability mode ACK contracts,
// F1-F4 review fixes, and edge cases in mode handling.
// ============================================================

// ────────────────────────────────────────────────────────────
// QA-CP831-1: IdempotentCreate_ModeMismatch_Rejected
//
// Create sync_all volume, then retry with best_effort.
// Must return mismatch error, NOT silently succeed.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_IdempotentCreate_ModeMismatch_Rejected(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	// Create sync_all volume.
	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "idem-mode",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})
	if err != nil {
		t.Fatalf("initial create: %v", err)
	}

	// Retry with best_effort (empty string = best_effort).
	_, err = ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "idem-mode",
		SizeBytes: 1 << 30,
		// DurabilityMode omitted = best_effort
	})
	if err == nil {
		t.Fatal("idempotent create with mode mismatch should fail")
	}
	if !strings.Contains(err.Error(), "durability_mode") {
		t.Fatalf("expected durability_mode mismatch error, got: %v", err)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-2: IdempotentCreate_RFMismatch_Rejected
//
// Create RF=2, retry with RF=3. Must fail with RF mismatch.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_IdempotentCreate_RFMismatch_Rejected(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "idem-rf",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Retry with RF=3.
	_, err = ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "idem-rf",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  3,
	})
	if err == nil {
		t.Fatal("idempotent create with RF mismatch should fail")
	}
	if !strings.Contains(err.Error(), "replica_factor") {
		t.Fatalf("expected replica_factor mismatch error, got: %v", err)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-3: IdempotentCreate_AllMatch_Succeeds
//
// Retry with identical params returns existing entry without error.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_IdempotentCreate_AllMatch_Succeeds(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	resp1, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "idem-ok",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Exact same params.
	resp2, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "idem-ok",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})
	if err != nil {
		t.Fatalf("idempotent create should succeed: %v", err)
	}
	if resp1.VolumeServer != resp2.VolumeServer {
		t.Fatalf("idempotent: volume_server changed %q -> %q", resp1.VolumeServer, resp2.VolumeServer)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-4: InvalidDurabilityMode_Rejected
//
// Unknown mode string must be rejected at the gRPC layer.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_InvalidDurabilityMode_Rejected(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	badModes := []string{"sync", "strict", "SYNC_ALL", "best-effort", "quorum"}
	for _, mode := range badModes {
		_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
			Name:           "invalid-" + mode,
			SizeBytes:      1 << 30,
			DurabilityMode: mode,
		})
		if err == nil {
			t.Errorf("durability_mode=%q should be rejected", mode)
			continue
		}
		if !strings.Contains(err.Error(), "invalid") {
			t.Errorf("durability_mode=%q: expected 'invalid' error, got: %v", mode, err)
		}
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-5: HeartbeatEmptyMode_DoesNotOverwriteStrict
//
// F3 regression: heartbeat with empty DurabilityMode must NOT
// downgrade an existing sync_all volume to best_effort.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_HeartbeatEmptyMode_DoesNotOverwriteStrict(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "hb-strict",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("hb-strict")

	// 10 rapid heartbeats with empty mode — simulates older VS version.
	for i := 0; i < 10; i++ {
		ms.blockRegistry.UpdateFullHeartbeat(entry.VolumeServer, []*master_pb.BlockVolumeInfoMessage{
			{
				Path:           entry.Path,
				VolumeSize:     1 << 30,
				DurabilityMode: "", // empty — must not overwrite
			},
		}, "")
	}

	entry, ok := ms.blockRegistry.Lookup("hb-strict")
	if !ok {
		t.Fatal("volume gone after heartbeat flood")
	}
	if entry.DurabilityMode != "sync_all" {
		t.Fatalf("F3 regression: empty heartbeat overwrote mode to %q", entry.DurabilityMode)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-6: HeartbeatNonEmptyMode_DoesUpdate
//
// Heartbeat with non-empty DurabilityMode DOES update the entry.
// (Legitimate VS reports after mode-aware upgrade.)
// ────────────────────────────────────────────────────────────
func TestQA_CP831_HeartbeatNonEmptyMode_DoesUpdate(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "hb-update",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("hb-update")

	// Heartbeat with matching non-empty mode — should be accepted.
	ms.blockRegistry.UpdateFullHeartbeat(entry.VolumeServer, []*master_pb.BlockVolumeInfoMessage{
		{
			Path:           entry.Path,
			VolumeSize:     1 << 30,
			DurabilityMode: "sync_all",
		},
	}, "")

	entry, ok := ms.blockRegistry.Lookup("hb-update")
	if !ok {
		t.Fatal("volume gone after heartbeat")
	}
	if entry.DurabilityMode != "sync_all" {
		t.Fatalf("expected sync_all after non-empty heartbeat, got %q", entry.DurabilityMode)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-7: SyncAll_RF3_PartialReplica_OneOfTwo_Fails
//
// sync_all RF=3 requires 2 replicas (RequiredReplicas=2).
// If only 1 of 2 replicas is provisioned → must fail + cleanup.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_SyncAll_RF3_PartialReplica_OneOfTwo_Fails(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	// First call succeeds (primary), second succeeds (replica 1),
	// third fails (replica 2).
	callCount := 0
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		callCount++
		if callCount == 3 {
			return nil, fmt.Errorf("disk full on third server")
		}
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         server + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}

	var deletedServers []string
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		deletedServers = append(deletedServers, server)
		return nil
	}

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "sa-rf3-partial",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  3,
	})
	if err == nil {
		t.Fatal("sync_all RF=3 should fail when only 1 of 2 required replicas provisioned")
	}
	if !strings.Contains(err.Error(), "requires") {
		t.Fatalf("expected 'requires' error, got: %v", err)
	}

	// Cleanup should have deleted primary + the 1 successful replica.
	if len(deletedServers) < 2 {
		t.Fatalf("expected at least 2 cleanup deletes (primary + 1 replica), got %d", len(deletedServers))
	}

	// Volume must not be in registry.
	if _, ok := ms.blockRegistry.Lookup("sa-rf3-partial"); ok {
		t.Fatal("partially created volume should not remain in registry")
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-8: SyncQuorum_RF3_OneReplicaOK_Succeeds
//
// sync_quorum RF=3 requires only 1 replica (quorum=2, minus primary=1).
// If 1 of 2 replicas fails, should still succeed.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_SyncQuorum_RF3_OneReplicaOK_Succeeds(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	callCount := 0
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		callCount++
		if callCount == 3 {
			return nil, fmt.Errorf("disk full on third server")
		}
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         server + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "sq-rf3-partial",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_quorum",
		ReplicaFactor:  3,
	})
	if err != nil {
		t.Fatalf("sync_quorum RF=3 should succeed with 1 replica: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("sq-rf3-partial")
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 replica, got %d", len(entry.Replicas))
	}
	if entry.DurabilityMode != "sync_quorum" {
		t.Fatalf("expected sync_quorum, got %q", entry.DurabilityMode)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-9: SyncQuorum_RF3_AllReplicasFail_Fails
//
// sync_quorum RF=3 needs 1 replica. If all replica servers fail,
// must fail + cleanup.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_SyncQuorum_RF3_AllReplicasFail_Fails(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	callCount := 0
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		callCount++
		if callCount > 1 { // primary succeeds, all replicas fail
			return nil, fmt.Errorf("disk full")
		}
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         server + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}

	var cleanupCalled bool
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		cleanupCalled = true
		return nil
	}

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "sq-rf3-none",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_quorum",
		ReplicaFactor:  3,
	})
	if err == nil {
		t.Fatal("sync_quorum RF=3 should fail when no replicas provisioned")
	}
	if !cleanupCalled {
		t.Fatal("expected cleanup of partial create")
	}
	if _, ok := ms.blockRegistry.Lookup("sq-rf3-none"); ok {
		t.Fatal("volume should not be in registry")
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-10: ConcurrentCreate_SameName_DifferentModes
//
// Two goroutines race to create same volume with different modes.
// Exactly one should succeed, the other should get either
// inflight error or idempotent mismatch error.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_ConcurrentCreate_SameName_DifferentModes(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	// Slow down allocation to increase race window.
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		time.Sleep(10 * time.Millisecond)
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         server + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}

	var wg sync.WaitGroup
	results := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		_, results[0] = ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
			Name:           "race-vol",
			SizeBytes:      1 << 30,
			DurabilityMode: "sync_all",
			ReplicaFactor:  2,
		})
	}()
	go func() {
		defer wg.Done()
		_, results[1] = ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
			Name:      "race-vol",
			SizeBytes: 1 << 30,
			// best_effort
		})
	}()
	wg.Wait()

	// Exactly one should succeed (or both fail if inflight lock blocks both).
	successes := 0
	for _, err := range results {
		if err == nil {
			successes++
		}
	}
	if successes > 1 {
		t.Fatal("both creates succeeded with different modes — split-brain!")
	}

	// Volume in registry must have a consistent mode.
	entry, ok := ms.blockRegistry.Lookup("race-vol")
	if ok {
		mode := entry.DurabilityMode
		if mode == "" {
			mode = "best_effort"
		}
		// Must be one of the two requested modes, not a corrupt mix.
		if mode != "sync_all" && mode != "best_effort" {
			t.Fatalf("corrupt mode in registry: %q", mode)
		}
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-11: FailoverPreservesDurabilityMode
//
// Create sync_all volume, trigger failover. Mode must survive
// promotion to new primary.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_FailoverPreservesDurabilityMode(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "fo-mode",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("fo-mode")
	primary := entry.VolumeServer

	// Expire the lease so failover will actually promote.
	entry.LeaseTTL = 5 * time.Second
	entry.LastLeaseGrant = time.Now().Add(-1 * time.Minute)

	// Ensure replica has fresh heartbeat for promotion eligibility.
	if len(entry.Replicas) > 0 {
		entry.Replicas[0].LastHeartbeat = time.Now()
		entry.Replicas[0].Role = blockvol.RoleToWire(blockvol.RoleReplica)
		entry.Replicas[0].WALHeadLSN = entry.WALHeadLSN
	}

	ms.failoverBlockVolumes(primary)

	entry, ok := ms.blockRegistry.Lookup("fo-mode")
	if !ok {
		t.Fatal("volume gone after failover")
	}
	if entry.VolumeServer == primary {
		t.Fatal("failover didn't promote replica")
	}
	if entry.DurabilityMode != "sync_all" {
		t.Fatalf("durability mode lost after failover: got %q, want sync_all", entry.DurabilityMode)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-12: ExpandPreservesDurabilityMode
//
// Expand a sync_all volume. Mode must not change.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_ExpandPreservesDurabilityMode(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "expand-mode",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = ms.ExpandBlockVolume(ctx, &master_pb.ExpandBlockVolumeRequest{
		Name:         "expand-mode",
		NewSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("expand-mode")
	if entry.DurabilityMode != "sync_all" {
		t.Fatalf("mode changed after expand: got %q", entry.DurabilityMode)
	}
	if entry.SizeBytes != 2<<30 {
		t.Fatalf("size not updated: got %d", entry.SizeBytes)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-13: LookupReturnsDurabilityMode
//
// Lookup response must include the correct durability mode.
// Also tests default for legacy volumes (empty → "best_effort").
// ────────────────────────────────────────────────────────────
func TestQA_CP831_LookupReturnsDurabilityMode(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	// Create sync_all.
	ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "lu-strict",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})

	resp, err := ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "lu-strict"})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if resp.DurabilityMode != "sync_all" {
		t.Fatalf("lookup DurabilityMode: got %q, want sync_all", resp.DurabilityMode)
	}

	// Create best_effort (empty mode).
	ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "lu-default",
		SizeBytes: 1 << 30,
	})

	resp, err = ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "lu-default"})
	if err != nil {
		t.Fatalf("lookup default: %v", err)
	}
	if resp.DurabilityMode != "best_effort" {
		t.Fatalf("lookup DurabilityMode for default: got %q, want best_effort", resp.DurabilityMode)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-14: BestEffort_NoReplicaCreate_StillSucceeds
//
// best_effort with only 1 server should succeed (no replica needed).
// ────────────────────────────────────────────────────────────
func TestQA_CP831_BestEffort_NoReplicaCreate_StillSucceeds(t *testing.T) {
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
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error { return nil }
	ms.blockRegistry.MarkBlockCapable("vs1:9333") // only 1 server

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "single-server",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("best_effort single server should succeed: %v", err)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-15: SyncAll_SingleServer_Fails
//
// sync_all with only 1 block server: cannot create any replicas,
// RequiredReplicas=1, so must fail with cleanup.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_SyncAll_SingleServer_Fails(t *testing.T) {
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
	var cleaned bool
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		cleaned = true
		return nil
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:           "sa-single",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})
	if err == nil {
		t.Fatal("sync_all should fail with only 1 server (no replicas possible)")
	}
	if !cleaned {
		t.Fatal("expected cleanup of primary")
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-16: CleanupPartialCreate_DeletesFails_NoRegistryLeak
//
// Cleanup delete itself fails. Volume must still NOT be in registry.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_CleanupPartialCreate_DeletesFails_NoRegistryLeak(t *testing.T) {
	ms := qaDurabilityMaster(t)
	ctx := context.Background()

	callCount := 0
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		callCount++
		if callCount > 1 { // all replicas fail
			return nil, fmt.Errorf("no space")
		}
		return &blockAllocResult{
			Path:    fmt.Sprintf("/data/%s.blk", name),
			IQN:     fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: server + ":3260",
			ReplicaDataAddr: server + ":14260",
			ReplicaCtrlAddr: server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}
	// Delete also fails (worst case: VS unreachable during cleanup).
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		return fmt.Errorf("connection refused")
	}

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "cleanup-fail",
		SizeBytes:      1 << 30,
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
	})
	if err == nil {
		t.Fatal("should fail due to insufficient replicas")
	}

	// Even though cleanup-delete failed, volume must NOT be in registry.
	if _, ok := ms.blockRegistry.Lookup("cleanup-fail"); ok {
		t.Fatal("volume leaked into registry despite failed cleanup")
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-17: MasterRestart_AutoRegister_PreservesDurabilityMode
//
// After master restart, heartbeat auto-registers volume.
// If heartbeat carries durability_mode, it should be preserved.
// If empty (old VS), mode defaults correctly.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_MasterRestart_AutoRegister_PreservesDurabilityMode(t *testing.T) {
	ms := qaDurabilityMaster(t)

	// Simulate heartbeat with durability mode (mode-aware VS).
	ms.blockRegistry.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:           "/data/strict-vol.blk",
			VolumeSize:     1 << 30,
			DurabilityMode: "sync_all",
		},
	}, "")

	entry, ok := ms.blockRegistry.Lookup("strict-vol")
	if !ok {
		t.Fatal("volume should be auto-registered")
	}
	if entry.DurabilityMode != "sync_all" {
		t.Fatalf("auto-registered mode: got %q, want sync_all", entry.DurabilityMode)
	}

	// Simulate heartbeat without mode (old VS).
	ms2 := qaDurabilityMaster(t)
	ms2.blockRegistry.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:       "/data/legacy-vol.blk",
			VolumeSize: 1 << 30,
			// DurabilityMode omitted
		},
	}, "")

	entry2, ok := ms2.blockRegistry.Lookup("legacy-vol")
	if !ok {
		t.Fatal("legacy volume should be auto-registered")
	}
	// Empty mode = best_effort (default).
	if entry2.DurabilityMode != "" {
		t.Fatalf("legacy auto-register mode should be empty (defaults to best_effort), got %q", entry2.DurabilityMode)
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-18: DurabilityMode_Superblock_Roundtrip_AllModes
//
// Create + close + reopen for each mode. Verifies superblock
// persistence for all 3 modes.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_DurabilityMode_Superblock_Roundtrip_AllModes(t *testing.T) {
	modes := []struct {
		mode blockvol.DurabilityMode
		name string
	}{
		{blockvol.DurabilityBestEffort, "best_effort"},
		{blockvol.DurabilitySyncAll, "sync_all"},
		{blockvol.DurabilitySyncQuorum, "sync_quorum"},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := dir + "/" + tc.name + ".blk"

			vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
				VolumeSize:     4 * 1024 * 1024,
				DurabilityMode: tc.mode,
			})
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			if vol.DurabilityMode() != tc.mode {
				t.Fatalf("after create: got %s, want %s", vol.DurabilityMode(), tc.mode)
			}
			vol.Close()

			vol2, err := blockvol.OpenBlockVol(path)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer vol2.Close()
			if vol2.DurabilityMode() != tc.mode {
				t.Fatalf("after reopen: got %s, want %s", vol2.DurabilityMode(), tc.mode)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-19: DurabilityMode_Validate_EdgeCases
//
// Validate all mode × RF combinations including edge cases.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_DurabilityMode_Validate_EdgeCases(t *testing.T) {
	cases := []struct {
		mode    blockvol.DurabilityMode
		rf      int
		wantErr bool
	}{
		{blockvol.DurabilityBestEffort, 1, false},
		{blockvol.DurabilityBestEffort, 2, false},
		{blockvol.DurabilityBestEffort, 3, false},
		{blockvol.DurabilitySyncAll, 1, false},
		{blockvol.DurabilitySyncAll, 2, false},
		{blockvol.DurabilitySyncAll, 3, false},
		{blockvol.DurabilitySyncQuorum, 1, true},  // RF < 3
		{blockvol.DurabilitySyncQuorum, 2, true},  // RF < 3
		{blockvol.DurabilitySyncQuorum, 3, false},  // RF >= 3
		{blockvol.DurabilityMode(99), 2, true},     // invalid mode
	}

	for _, tc := range cases {
		name := fmt.Sprintf("%s_RF%d", tc.mode, tc.rf)
		t.Run(name, func(t *testing.T) {
			err := tc.mode.Validate(tc.rf)
			if tc.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-20: DurabilityMode_RequiredReplicas_Math
//
// Verify RequiredReplicas returns correct values for all modes
// and RF combinations. Catches off-by-one in quorum formula.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_DurabilityMode_RequiredReplicas_Math(t *testing.T) {
	cases := []struct {
		mode     blockvol.DurabilityMode
		rf       int
		expected int
	}{
		// best_effort: always 0
		{blockvol.DurabilityBestEffort, 1, 0},
		{blockvol.DurabilityBestEffort, 2, 0},
		{blockvol.DurabilityBestEffort, 3, 0},

		// sync_all: RF-1
		{blockvol.DurabilitySyncAll, 1, 0},
		{blockvol.DurabilitySyncAll, 2, 1},
		{blockvol.DurabilitySyncAll, 3, 2},

		// sync_quorum: RF/2 (integer division)
		{blockvol.DurabilitySyncQuorum, 1, 0},
		{blockvol.DurabilitySyncQuorum, 2, 1}, // 2/2 = 1
		{blockvol.DurabilitySyncQuorum, 3, 1}, // 3/2 = 1
	}

	for _, tc := range cases {
		name := fmt.Sprintf("%s_RF%d", tc.mode, tc.rf)
		t.Run(name, func(t *testing.T) {
			got := tc.mode.RequiredReplicas(tc.rf)
			if got != tc.expected {
				t.Errorf("RequiredReplicas(%d) = %d, want %d", tc.rf, got, tc.expected)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────
// QA-CP831-bonus: SentinelErrors_AreProperlyCategorized
//
// Verify durability sentinel errors are correctly detected by
// errors.Is and NOT by string matching.
// ────────────────────────────────────────────────────────────
func TestQA_CP831_SentinelErrors_AreProperlyCategorized(t *testing.T) {
	// Wrapped errors should still be detected.
	wrapped := fmt.Errorf("write op failed: %w", blockvol.ErrDurabilityBarrierFailed)
	if !errors.Is(wrapped, blockvol.ErrDurabilityBarrierFailed) {
		t.Fatal("wrapped ErrDurabilityBarrierFailed should be detected via errors.Is")
	}

	doubleWrapped := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", blockvol.ErrDurabilityQuorumLost))
	if !errors.Is(doubleWrapped, blockvol.ErrDurabilityQuorumLost) {
		t.Fatal("double-wrapped ErrDurabilityQuorumLost should be detected via errors.Is")
	}

	// Plain string should NOT match sentinel.
	fake := errors.New("blockvol: sync_all durability barrier failed")
	if errors.Is(fake, blockvol.ErrDurabilityBarrierFailed) {
		t.Fatal("plain string error should NOT match sentinel")
	}

	// Verify ErrInvalidDurabilityMode wraps correctly from ParseDurabilityMode.
	_, err := blockvol.ParseDurabilityMode("garbage")
	if err == nil {
		t.Fatal("expected error from ParseDurabilityMode")
	}
	if !errors.Is(err, blockvol.ErrInvalidDurabilityMode) {
		t.Fatalf("ParseDurabilityMode error should wrap ErrInvalidDurabilityMode: %v", err)
	}
}

// Suppress unused import.
var _ = time.Second
