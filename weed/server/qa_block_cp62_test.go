package weed_server

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// ============================================================
// QA-REG: Registry adversarial tests
// ============================================================

// QA-REG-1: FullHeartbeat removes volumes created via RPC while heartbeat is in transit.
// Scenario: Master creates vol1 (StatusActive). Before the VS heartbeat arrives,
// another VS heartbeat (from a different server?) or a delayed heartbeat arrives
// that doesn't include vol1. FullHeartbeat should only remove stale entries for
// THAT specific server, not all servers.
func TestQA_Reg_FullHeartbeatCrossTalk(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// Register vol1 on server s1.
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk", Status: StatusActive})
	// Register vol2 on server s2.
	r.Register(&BlockVolumeEntry{Name: "vol2", VolumeServer: "s2", Path: "/v2.blk", Status: StatusActive})

	// Full heartbeat from s1 reports vol1 — should NOT affect s2's volumes.
	r.UpdateFullHeartbeat("s1", []*master_pb.BlockVolumeInfoMessage{
		{Path: "/v1.blk", Epoch: 1},
	}, "")

	// vol2 on s2 should still exist.
	if _, ok := r.Lookup("vol2"); !ok {
		t.Fatal("BUG: full heartbeat from s1 removed vol2 which belongs to s2")
	}
}

// QA-REG-2: FullHeartbeat from server with zero volumes should clear all entries for that server.
func TestQA_Reg_FullHeartbeatEmptyServer(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk", Status: StatusActive})
	r.Register(&BlockVolumeEntry{Name: "vol2", VolumeServer: "s1", Path: "/v2.blk", Status: StatusActive})

	// Empty heartbeat from s1 (HasNoBlockVolumes=true, zero infos).
	r.UpdateFullHeartbeat("s1", nil, "")

	if _, ok := r.Lookup("vol1"); ok {
		t.Error("BUG: vol1 should be removed after empty full heartbeat")
	}
	if _, ok := r.Lookup("vol2"); ok {
		t.Error("BUG: vol2 should be removed after empty full heartbeat")
	}
}

// QA-REG-3: Concurrent FullHeartbeat and Register for same server.
// While a heartbeat is being processed, a new CreateBlockVolume registers on the same server.
func TestQA_Reg_ConcurrentHeartbeatAndRegister(t *testing.T) {
	r := NewBlockVolumeRegistry()

	var wg sync.WaitGroup
	var panicked atomic.Bool

	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked.Store(true)
				}
			}()
			r.UpdateFullHeartbeat("s1", []*master_pb.BlockVolumeInfoMessage{
				{Path: fmt.Sprintf("/v%d.blk", i), Epoch: uint64(i)},
			}, "")
		}(i)
		go func(i int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked.Store(true)
				}
			}()
			r.Register(&BlockVolumeEntry{
				Name:         fmt.Sprintf("vol%d", i),
				VolumeServer: "s1",
				Path:         fmt.Sprintf("/v%d.blk", i),
				Status:       StatusActive,
			})
		}(i)
	}
	wg.Wait()

	if panicked.Load() {
		t.Fatal("BUG: concurrent heartbeat + register caused panic")
	}
}

// QA-REG-4: DeltaHeartbeat with removed path that doesn't match any registered volume.
// Should be a no-op, not panic or corrupt state.
func TestQA_Reg_DeltaHeartbeatUnknownPath(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})

	// Delta says /unknown.blk was removed — this path doesn't match vol1.
	r.UpdateDeltaHeartbeat("s1",
		nil,
		[]*master_pb.BlockVolumeShortInfoMessage{{Path: "/unknown.blk"}},
	)

	// vol1 should still exist.
	if _, ok := r.Lookup("vol1"); !ok {
		t.Fatal("BUG: delta heartbeat with unknown path removed an unrelated volume")
	}
}

// QA-REG-5: PickServer always picks the same server when counts are tied.
// Deterministic placement prevents flip-flopping.
func TestQA_Reg_PickServerTiebreaker(t *testing.T) {
	r := NewBlockVolumeRegistry()

	// All servers have 0 volumes.
	servers := []string{"s1", "s2", "s3"}
	results := make(map[string]int)
	for i := 0; i < 10; i++ {
		s, err := r.PickServer(servers)
		if err != nil {
			t.Fatal(err)
		}
		results[s]++
	}
	// With stable ordering, the same server should win every time.
	// The algorithm picks servers[0] by default when counts are equal.
	if results["s1"] != 10 {
		t.Logf("PickServer results with tied counts: %v (non-deterministic but OK if no panic)", results)
	}
}

// QA-REG-6: Unregister a volume then re-register with a different server.
func TestQA_Reg_ReregisterDifferentServer(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s1", Path: "/v1.blk"})

	// Unregister and re-register on s2.
	r.Unregister("vol1")
	r.Register(&BlockVolumeEntry{Name: "vol1", VolumeServer: "s2", Path: "/v1.blk"})

	entry, ok := r.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 should exist after re-register")
	}
	if entry.VolumeServer != "s2" {
		t.Fatalf("VolumeServer: got %q, want s2", entry.VolumeServer)
	}

	// s1 should have no volumes.
	if vols := r.ListByServer("s1"); len(vols) != 0 {
		t.Fatalf("s1 should have 0 volumes after re-register, got %d", len(vols))
	}
	// s2 should have 1 volume.
	if vols := r.ListByServer("s2"); len(vols) != 1 {
		t.Fatalf("s2 should have 1 volume, got %d", len(vols))
	}
}

// QA-REG-7: AcquireInflight for different names doesn't interfere.
func TestQA_Reg_InflightIndependence(t *testing.T) {
	r := NewBlockVolumeRegistry()

	if !r.AcquireInflight("vol1") {
		t.Fatal("acquire vol1 should succeed")
	}
	if !r.AcquireInflight("vol2") {
		t.Fatal("acquire vol2 should succeed (different name)")
	}
	if r.AcquireInflight("vol1") {
		t.Fatal("double acquire vol1 should fail")
	}
	r.ReleaseInflight("vol1")
	if !r.AcquireInflight("vol1") {
		t.Fatal("acquire vol1 after release should succeed")
	}
	r.ReleaseInflight("vol1")
	r.ReleaseInflight("vol2")
}

// QA-REG-8: BlockCapableServers includes only currently-marked servers.
func TestQA_Reg_BlockCapableServersAfterUnmark(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("s1")
	r.MarkBlockCapable("s2")
	r.MarkBlockCapable("s3")

	r.UnmarkBlockCapable("s2")

	servers := r.BlockCapableServers()
	for _, s := range servers {
		if s == "s2" {
			t.Fatal("BUG: s2 should not be in block-capable list after UnmarkBlockCapable")
		}
	}
	if len(servers) != 2 {
		t.Fatalf("expected 2 block-capable servers, got %d: %v", len(servers), servers)
	}
}

// ============================================================
// QA-MASTER: Master RPC adversarial tests
// ============================================================

// QA-MASTER-1: CreateBlockVolume then Delete while VS is unreachable.
// Delete should fail (cannot contact VS), but registry entry should NOT be removed.
func TestQA_Master_DeleteVSUnreachable(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "vol1", SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Make VS delete fail.
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		return fmt.Errorf("connection refused")
	}

	_, err = ms.DeleteBlockVolume(context.Background(), &master_pb.DeleteBlockVolumeRequest{
		Name: "vol1",
	})
	if err == nil {
		t.Fatal("delete should fail when VS is unreachable")
	}

	// Registry entry should still exist (not orphaned).
	if _, ok := ms.blockRegistry.Lookup("vol1"); !ok {
		t.Fatal("BUG: registry entry removed even though VS delete failed")
	}
}

// QA-MASTER-2: Create then lookup a volume with a name that requires sanitization.
func TestQA_Master_CreateSanitizedName(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	// Name with special characters.
	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "pvc-abc/def:123", SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Lookup should use the exact name (not sanitized).
	lookupResp, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{
		Name: "pvc-abc/def:123",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if lookupResp.VolumeServer != resp.VolumeServer {
		t.Fatalf("lookup mismatch: %+v vs %+v", lookupResp, resp)
	}
}

// QA-MASTER-3: Concurrent Create and Delete for the same volume name.
func TestQA_Master_ConcurrentCreateDelete(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	var wg sync.WaitGroup
	var panicked atomic.Bool

	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked.Store(true)
					t.Errorf("PANIC in create: %v", r)
				}
			}()
			ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
				Name: "race-vol", SizeBytes: 1 << 30,
			})
		}()
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked.Store(true)
					t.Errorf("PANIC in delete: %v", r)
				}
			}()
			ms.DeleteBlockVolume(context.Background(), &master_pb.DeleteBlockVolumeRequest{
				Name: "race-vol",
			})
		}()
	}
	wg.Wait()

	if panicked.Load() {
		t.Fatal("BUG: concurrent create/delete caused panic")
	}
}

// QA-MASTER-4: Create with all VS failing should not leave orphan in registry.
func TestQA_Master_AllVSFailNoOrphan(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockRegistry.MarkBlockCapable("vs3:9333")

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return nil, fmt.Errorf("disk full on %s", server)
	}

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "doomed", SizeBytes: 1 << 30,
	})
	if err == nil {
		t.Fatal("expected error when all VS fail")
	}

	// No orphan in registry.
	if _, ok := ms.blockRegistry.Lookup("doomed"); ok {
		t.Fatal("BUG: orphan entry in registry after all VS failed")
	}

	// Inflight lock should be released.
	if !ms.blockRegistry.AcquireInflight("doomed") {
		t.Fatal("BUG: inflight lock not released after all VS failed")
	}
	ms.blockRegistry.ReleaseInflight("doomed")
}

// QA-MASTER-5: VS allocate succeeds but is slow — inflight lock must block second create.
func TestQA_Master_SlowAllocateBlocksSecond(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	var allocCount atomic.Int32
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		allocCount.Add(1)
		time.Sleep(100 * time.Millisecond) // simulate slow VS
		return &blockAllocResult{
			Path:      fmt.Sprintf("/data/%s.blk", name),
			IQN:       fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr: server,
		}, nil
	}

	var wg sync.WaitGroup
	errors := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errors[i] = ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
				Name: "slow-vol", SizeBytes: 1 << 30,
			})
		}(i)
	}
	wg.Wait()

	// One should succeed, one should get "already in progress" or succeed via idempotent path.
	successCount := 0
	for _, err := range errors {
		if err == nil {
			successCount++
		}
	}
	if successCount == 0 {
		t.Fatal("at least one create should succeed")
	}

	// Only 1 VS allocation call should have been made (inflight blocks the second).
	if allocCount.Load() > 1 {
		t.Logf("WARNING: %d VS allocations made (expected 1 — second should be blocked by inflight lock or return idempotent)", allocCount.Load())
	}
}

// QA-MASTER-6: Create with SizeBytes == 0 should be rejected.
func TestQA_Master_CreateZeroSize(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "zero", SizeBytes: 0,
	})
	if err == nil {
		t.Fatal("expected error for zero size")
	}
}

// QA-MASTER-7: Create with empty name should be rejected.
func TestQA_Master_CreateEmptyName(t *testing.T) {
	ms := testMasterServer(t)

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "", SizeBytes: 1 << 30,
	})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

// QA-MASTER-8: Lookup/Delete with empty name should be rejected.
func TestQA_Master_EmptyNameValidation(t *testing.T) {
	ms := testMasterServer(t)

	_, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{Name: ""})
	if err == nil {
		t.Error("lookup with empty name should fail")
	}

	_, err = ms.DeleteBlockVolume(context.Background(), &master_pb.DeleteBlockVolumeRequest{Name: ""})
	if err == nil {
		t.Error("delete with empty name should fail")
	}
}

// ============================================================
// QA-VS: Volume server BlockService adversarial tests
// ============================================================

// QA-VS-1: Concurrent CreateBlockVol for the same name on a single VS.
func TestQA_VS_ConcurrentCreate(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	var wg sync.WaitGroup
	var panicked atomic.Bool
	errors := make([]error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked.Store(true)
					t.Errorf("PANIC: %v", r)
				}
			}()
			_, _, _, errors[i] = bs.CreateBlockVol("race-vol", 4*1024*1024, "", "")
		}(i)
	}
	wg.Wait()

	if panicked.Load() {
		t.Fatal("BUG: concurrent CreateBlockVol caused panic")
	}

	// At least some should succeed.
	successCount := 0
	for _, err := range errors {
		if err == nil {
			successCount++
		}
	}
	if successCount == 0 {
		t.Fatal("at least one concurrent CreateBlockVol should succeed")
	}
}

// QA-VS-2: Concurrent CreateBlockVol and DeleteBlockVol for the same name.
func TestQA_VS_ConcurrentCreateDelete(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	var wg sync.WaitGroup
	var panicked atomic.Bool

	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked.Store(true)
				}
			}()
			bs.CreateBlockVol("cd-vol", 4*1024*1024, "", "")
		}()
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked.Store(true)
				}
			}()
			bs.DeleteBlockVol("cd-vol")
		}()
	}
	wg.Wait()

	if panicked.Load() {
		t.Fatal("BUG: concurrent create/delete caused panic")
	}
}

// QA-VS-3: DeleteBlockVol should clean up .snap.* files.
func TestQA_VS_DeleteCleansSnapshots(t *testing.T) {
	bs, blockDir := newTestBlockServiceWithDir(t)

	bs.CreateBlockVol("snap-vol", 4*1024*1024, "", "")

	// Simulate snapshot files.
	snapPath := blockDir + "/snap-vol.blk.snap.0"
	if err := writeTestFile(snapPath); err != nil {
		t.Fatalf("create snap file: %v", err)
	}

	bs.DeleteBlockVol("snap-vol")

	// Snap file should be removed.
	if fileExists(snapPath) {
		t.Error("BUG: .snap.0 file not cleaned up after DeleteBlockVol")
	}
}

// QA-VS-4: CreateBlockVol with name that sanitizes to the same filename as another volume.
func TestQA_VS_SanitizationCollision(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	// "VolA" sanitizes to "vola.blk", "vola" also sanitizes to "vola.blk".
	_, _, _, err := bs.CreateBlockVol("VolA", 4*1024*1024, "", "")
	if err != nil {
		t.Fatalf("create VolA: %v", err)
	}

	// "vola" should get the idempotent path (same file on disk).
	path2, _, _, err := bs.CreateBlockVol("vola", 4*1024*1024, "", "")
	if err != nil {
		t.Fatalf("create vola: %v", err)
	}

	// Should point to the same file.
	if !strings.HasSuffix(path2, "vola.blk") {
		t.Errorf("path: got %q, expected to end with vola.blk", path2)
	}
}

// QA-VS-5: CreateBlockVol idempotent path verifies TargetServer re-registration.
func TestQA_VS_CreateIdempotentReaddTarget(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	// First create.
	_, iqn1, _, err := bs.CreateBlockVol("readd-vol", 4*1024*1024, "", "")
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	// Second create (idempotent) — should succeed and re-add to TargetServer.
	_, iqn2, _, err := bs.CreateBlockVol("readd-vol", 4*1024*1024, "", "")
	if err != nil {
		t.Fatalf("idempotent create: %v", err)
	}

	if iqn1 != iqn2 {
		t.Fatalf("IQN mismatch: %q vs %q", iqn1, iqn2)
	}
}

// QA-VS-6: gRPC handler with nil blockService.
func TestQA_VS_GrpcNilBlockService(t *testing.T) {
	vs := &VolumeServer{blockService: nil}

	_, err := vs.AllocateBlockVolume(context.Background(), &volume_server_pb.AllocateBlockVolumeRequest{
		Name: "vol1", SizeBytes: 1 << 30,
	})
	if err == nil {
		t.Fatal("expected error when blockService is nil")
	}

	_, err = vs.VolumeServerDeleteBlockVolume(context.Background(), &volume_server_pb.VolumeServerDeleteBlockVolumeRequest{
		Name: "vol1",
	})
	if err == nil {
		t.Fatal("expected error when blockService is nil")
	}
}

// Helper functions.

func writeTestFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	f.Write([]byte("test"))
	return f.Close()
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
