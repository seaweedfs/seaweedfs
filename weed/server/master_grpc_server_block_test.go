package weed_server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// testMasterServer creates a minimal MasterServer with mock VS calls for testing.
func testMasterServer(t *testing.T) *MasterServer {
	t.Helper()
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
	}
	// Default mock: succeed with deterministic values.
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

func TestMaster_CreateBlockVolume(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "test-vol",
		SizeBytes: 1 << 30,
		DiskType:  "ssd",
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	if resp.VolumeId != "test-vol" {
		t.Fatalf("VolumeId: got %q, want test-vol", resp.VolumeId)
	}
	if resp.VolumeServer != "vs1:9333" {
		t.Fatalf("VolumeServer: got %q, want vs1:9333", resp.VolumeServer)
	}
	if resp.Iqn == "" || resp.IscsiAddr == "" {
		t.Fatal("IQN or ISCSIAddr is empty")
	}

	// Verify registry entry.
	entry, ok := ms.blockRegistry.Lookup("test-vol")
	if !ok {
		t.Fatal("volume not found in registry")
	}
	if entry.Status != StatusActive {
		t.Fatalf("status: got %d, want StatusActive", entry.Status)
	}
}

func TestMaster_CreateIdempotent(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	resp1, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	resp2, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("idempotent create: %v", err)
	}

	if resp1.VolumeId != resp2.VolumeId || resp1.VolumeServer != resp2.VolumeServer {
		t.Fatalf("idempotent mismatch: %+v vs %+v", resp1, resp2)
	}
}

func TestMaster_CreateIdempotentSizeMismatch(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	// Larger size should fail.
	_, err = ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 2 << 30,
	})
	if err == nil {
		t.Fatal("expected error for size mismatch")
	}

	// Same or smaller size should succeed (idempotent).
	_, err = ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 29,
	})
	if err != nil {
		t.Fatalf("smaller size should succeed: %v", err)
	}
}

func TestMaster_CreateNoServers(t *testing.T) {
	ms := testMasterServer(t)
	// No block-capable servers registered.

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err == nil {
		t.Fatal("expected error when no servers available")
	}
}

func TestMaster_CreateVSFailure_Retry(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	var callCount atomic.Int32
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		n := callCount.Add(1)
		if n == 1 {
			return nil, fmt.Errorf("disk full")
		}
		return &blockAllocResult{
			Path:      fmt.Sprintf("/data/%s.blk", name),
			IQN:       fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: server,
		}, nil
	}

	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("expected retry to succeed: %v", err)
	}
	if resp.VolumeId != "vol1" {
		t.Fatalf("VolumeId: got %q, want vol1", resp.VolumeId)
	}
	if callCount.Load() < 2 {
		t.Fatalf("expected at least 2 VS calls, got %d", callCount.Load())
	}
}

func TestMaster_CreateVSFailure_Cleanup(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		return nil, fmt.Errorf("all servers broken")
	}

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err == nil {
		t.Fatal("expected error when all VS fail")
	}

	// No stale registry entry.
	if _, ok := ms.blockRegistry.Lookup("vol1"); ok {
		t.Fatal("stale registry entry should not exist")
	}
}

func TestMaster_CreateConcurrentSameName(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	var callCount atomic.Int32
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		callCount.Add(1)
		return &blockAllocResult{
			Path:      fmt.Sprintf("/data/%s.blk", name),
			IQN:       fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: server,
		}, nil
	}

	var wg sync.WaitGroup
	results := make([]*master_pb.CreateBlockVolumeResponse, 10)
	errors := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errors[i] = ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
				Name:      "same-vol",
				SizeBytes: 1 << 30,
			})
		}(i)
	}
	wg.Wait()

	// Some may get "already in progress" error, but at least one must succeed.
	successCount := 0
	for i := 0; i < 10; i++ {
		if errors[i] == nil {
			successCount++
		}
	}
	if successCount == 0 {
		t.Fatal("at least one concurrent create should succeed")
	}

	// Only one VS allocation call should have been made.
	if callCount.Load() != 1 {
		t.Fatalf("expected exactly 1 VS call, got %d", callCount.Load())
	}
}

func TestMaster_DeleteBlockVolume(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = ms.DeleteBlockVolume(context.Background(), &master_pb.DeleteBlockVolumeRequest{
		Name: "vol1",
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	if _, ok := ms.blockRegistry.Lookup("vol1"); ok {
		t.Fatal("volume should be removed from registry")
	}
}

func TestMaster_DeleteNotFound(t *testing.T) {
	ms := testMasterServer(t)

	_, err := ms.DeleteBlockVolume(context.Background(), &master_pb.DeleteBlockVolumeRequest{
		Name: "nonexistent",
	})
	if err != nil {
		t.Fatalf("delete nonexistent should succeed (idempotent): %v", err)
	}
}

func TestMaster_CreateWithReplica(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	var allocServers []string
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		allocServers = append(allocServers, server)
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":14260",
			ReplicaCtrlAddr: server + ":14261",
		}, nil
	}

	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}

	// Should have called allocate twice (primary + replica).
	if len(allocServers) != 2 {
		t.Fatalf("expected 2 alloc calls, got %d", len(allocServers))
	}
	if allocServers[0] == allocServers[1] {
		t.Fatalf("primary and replica should be on different servers, both on %s", allocServers[0])
	}

	// Response should include replica server.
	if resp.ReplicaServer == "" {
		t.Fatal("ReplicaServer should be set")
	}
	if resp.ReplicaServer == resp.VolumeServer {
		t.Fatalf("replica should differ from primary: both %q", resp.VolumeServer)
	}

	// Registry entry should have replica info.
	entry, ok := ms.blockRegistry.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 not in registry")
	}
	if entry.ReplicaServer == "" {
		t.Fatal("registry ReplicaServer should be set")
	}
	if entry.ReplicaPath == "" {
		t.Fatal("registry ReplicaPath should be set")
	}
}

func TestMaster_CreateSingleServer_NoReplica(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	var allocCount atomic.Int32
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		allocCount.Add(1)
		return &blockAllocResult{
			Path:      fmt.Sprintf("/data/%s.blk", name),
			IQN:       fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: server,
		}, nil
	}

	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}

	// Only 1 server → single-copy mode, only 1 alloc call.
	if allocCount.Load() != 1 {
		t.Fatalf("expected 1 alloc call, got %d", allocCount.Load())
	}
	if resp.ReplicaServer != "" {
		t.Fatalf("ReplicaServer should be empty in single-copy mode, got %q", resp.ReplicaServer)
	}

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.ReplicaServer != "" {
		t.Fatalf("registry ReplicaServer should be empty, got %q", entry.ReplicaServer)
	}
}

func TestMaster_CreateReplica_SecondFails_SingleCopy(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	var callCount atomic.Int32
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		n := callCount.Add(1)
		if n == 2 {
			// Replica allocation fails.
			return nil, fmt.Errorf("replica disk full")
		}
		return &blockAllocResult{
			Path:      fmt.Sprintf("/data/%s.blk", name),
			IQN:       fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: server,
		}, nil
	}

	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume should succeed in single-copy mode: %v", err)
	}

	// Volume created, but without replica (F4).
	if resp.ReplicaServer != "" {
		t.Fatalf("ReplicaServer should be empty when replica fails, got %q", resp.ReplicaServer)
	}

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.ReplicaServer != "" {
		t.Fatal("registry should have no replica")
	}
}

func TestMaster_CreateEnqueuesAssignments(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":14260",
			ReplicaCtrlAddr: server + ":14261",
		}, nil
	}

	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}

	// Primary server should have 1 pending assignment.
	primaryPending := ms.blockAssignmentQueue.Pending(resp.VolumeServer)
	if primaryPending != 1 {
		t.Fatalf("primary pending assignments: got %d, want 1", primaryPending)
	}

	// Replica server should have 1 pending assignment.
	if resp.ReplicaServer == "" {
		t.Fatal("expected replica server")
	}
	replicaPending := ms.blockAssignmentQueue.Pending(resp.ReplicaServer)
	if replicaPending != 1 {
		t.Fatalf("replica pending assignments: got %d, want 1", replicaPending)
	}
}

func TestMaster_CreateSingleCopy_NoReplicaAssignment(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}

	// Only primary assignment, no replica.
	primaryPending := ms.blockAssignmentQueue.Pending("vs1:9333")
	if primaryPending != 1 {
		t.Fatalf("primary pending: got %d, want 1", primaryPending)
	}

	// No other server should have pending assignments.
	// (No way to enumerate all servers, but we know there's only 1 server.)
}

func TestMaster_LookupReturnsReplicaServer(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:      fmt.Sprintf("/data/%s.blk", name),
			IQN:       fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: server,
		}, nil
	}

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	resp, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{
		Name: "vol1",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if resp.ReplicaServer == "" {
		t.Fatal("LookupBlockVolume should return ReplicaServer")
	}
	if resp.ReplicaServer == resp.VolumeServer {
		t.Fatalf("replica should differ from primary")
	}
}

func TestMaster_CreateBlockSnapshot(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockVSSnapshot = func(ctx context.Context, server string, name string, snapID uint32) (int64, uint64, error) {
		return 1709654400, 1 << 30, nil
	}

	// Create volume first.
	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "snap-vol", SizeBytes: 1 << 30,
	})

	resp, err := ms.CreateBlockSnapshot(context.Background(), &master_pb.CreateBlockSnapshotRequest{
		VolumeName: "snap-vol", SnapshotId: 42,
	})
	if err != nil {
		t.Fatalf("CreateBlockSnapshot: %v", err)
	}
	if resp.SnapshotId != 42 {
		t.Fatalf("SnapshotId: got %d, want 42", resp.SnapshotId)
	}
	if resp.CreatedAt != 1709654400 {
		t.Fatalf("CreatedAt: got %d, want 1709654400", resp.CreatedAt)
	}
}

func TestMaster_CreateBlockSnapshot_VolumeNotFound(t *testing.T) {
	ms := testMasterServer(t)
	_, err := ms.CreateBlockSnapshot(context.Background(), &master_pb.CreateBlockSnapshotRequest{
		VolumeName: "nonexistent", SnapshotId: 1,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent volume")
	}
}

func TestMaster_DeleteBlockSnapshot(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockVSDeleteSnap = func(ctx context.Context, server string, name string, snapID uint32) error {
		return nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "snap-vol", SizeBytes: 1 << 30,
	})

	_, err := ms.DeleteBlockSnapshot(context.Background(), &master_pb.DeleteBlockSnapshotRequest{
		VolumeName: "snap-vol", SnapshotId: 42,
	})
	if err != nil {
		t.Fatalf("DeleteBlockSnapshot: %v", err)
	}
}

func TestMaster_ListBlockSnapshots(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockVSListSnaps = func(ctx context.Context, server string, name string) ([]*volume_server_pb.BlockSnapshotInfo, error) {
		return []*volume_server_pb.BlockSnapshotInfo{
			{SnapshotId: 1, CreatedAt: 100},
			{SnapshotId: 2, CreatedAt: 200},
		}, nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "snap-vol", SizeBytes: 1 << 30,
	})

	resp, err := ms.ListBlockSnapshots(context.Background(), &master_pb.ListBlockSnapshotsRequest{
		VolumeName: "snap-vol",
	})
	if err != nil {
		t.Fatalf("ListBlockSnapshots: %v", err)
	}
	if len(resp.Snapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(resp.Snapshots))
	}
}

func TestMaster_ExpandBlockVolume(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockVSExpand = func(ctx context.Context, server string, name string, newSize uint64) (uint64, error) {
		return newSize, nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "expand-vol", SizeBytes: 1 << 30,
	})

	resp, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "expand-vol", NewSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("ExpandBlockVolume: %v", err)
	}
	if resp.CapacityBytes != 2<<30 {
		t.Fatalf("CapacityBytes: got %d, want %d", resp.CapacityBytes, 2<<30)
	}

	// Verify registry was updated.
	entry, ok := ms.blockRegistry.Lookup("expand-vol")
	if !ok {
		t.Fatal("volume not found in registry")
	}
	if entry.SizeBytes != 2<<30 {
		t.Fatalf("registry size: got %d, want %d", entry.SizeBytes, 2<<30)
	}
}

func TestMaster_ExpandBlockVolume_VSFailure(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockVSExpand = func(ctx context.Context, server string, name string, newSize uint64) (uint64, error) {
		return 0, fmt.Errorf("disk full")
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "expand-vol", SizeBytes: 1 << 30,
	})

	_, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "expand-vol", NewSizeBytes: 2 << 30,
	})
	if err == nil {
		t.Fatal("expected error when VS expand fails")
	}

	// Registry should NOT have been updated.
	entry, _ := ms.blockRegistry.Lookup("expand-vol")
	if entry.SizeBytes != 1<<30 {
		t.Fatalf("registry size should be unchanged: got %d, want %d", entry.SizeBytes, 1<<30)
	}
}

func TestMaster_LookupBlockVolume(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	resp, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{
		Name: "vol1",
	})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if resp.VolumeServer != "vs1:9333" {
		t.Fatalf("VolumeServer: got %q, want vs1:9333", resp.VolumeServer)
	}
	if resp.CapacityBytes != 1<<30 {
		t.Fatalf("CapacityBytes: got %d, want %d", resp.CapacityBytes, 1<<30)
	}

	// Lookup nonexistent.
	_, err = ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{
		Name: "nonexistent",
	})
	if err == nil {
		t.Fatal("lookup nonexistent should return error")
	}
}

// ============================================================
// CP8-2 T9: Multi-Replica Create/Delete/Assign Tests
// ============================================================

func testMasterServerRF3(t *testing.T) *MasterServer {
	t.Helper()
	ms := testMasterServer(t)
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:       server + ":3260",
			ReplicaDataAddr: server + ":14260",
			ReplicaCtrlAddr: server + ":14261",
		}, nil
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockRegistry.MarkBlockCapable("vs3:9333")
	return ms
}

// RF=3 with 3 servers: should create 2 replicas.
func TestMaster_CreateRF3_ThreeServers(t *testing.T) {
	ms := testMasterServerRF3(t)

	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          "vol1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}

	entry, ok := ms.blockRegistry.Lookup("vol1")
	if !ok {
		t.Fatal("vol1 not in registry")
	}
	if entry.ReplicaFactor != 3 {
		t.Fatalf("ReplicaFactor: got %d, want 3", entry.ReplicaFactor)
	}
	if len(entry.Replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(entry.Replicas))
	}

	// All 3 servers should be different.
	servers := map[string]bool{resp.VolumeServer: true}
	for _, ri := range entry.Replicas {
		if servers[ri.Server] {
			t.Fatalf("duplicate server: %q", ri.Server)
		}
		servers[ri.Server] = true
	}
	if len(servers) != 3 {
		t.Fatalf("expected 3 distinct servers, got %d", len(servers))
	}
}

// RF=3 with only 2 servers: should create 1 replica (partial).
func TestMaster_CreateRF3_TwoServers(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:       server + ":3260",
			ReplicaDataAddr: server + ":14260",
			ReplicaCtrlAddr: server + ":14261",
		}, nil
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          "vol1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("vol1")
	// Only 2 servers available, so only 1 replica (not 2).
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 replica (only 2 servers), got %d", len(entry.Replicas))
	}
}

// RF=2 unchanged after CP8-2 changes.
func TestMaster_CreateRF2_Unchanged(t *testing.T) {
	ms := testMasterServerRF3(t)

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "vol1",
		SizeBytes: 1 << 30,
		// No ReplicaFactor → defaults to 2.
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.ReplicaFactor != 2 {
		t.Fatalf("ReplicaFactor: got %d, want 2 (default)", entry.ReplicaFactor)
	}
	if len(entry.Replicas) != 1 {
		t.Fatalf("expected 1 replica for RF=2, got %d", len(entry.Replicas))
	}
	// Backward compat scalar fields should be set.
	if entry.ReplicaServer == "" {
		t.Fatal("ReplicaServer (deprecated) should still be set for backward compat")
	}
}

// DeleteBlockVolume RF=3 deletes all replicas.
func TestMaster_DeleteRF3_DeletesAllReplicas(t *testing.T) {
	ms := testMasterServerRF3(t)

	var deletedServers []string
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		deletedServers = append(deletedServers, server)
		return nil
	}

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          "vol1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = ms.DeleteBlockVolume(context.Background(), &master_pb.DeleteBlockVolumeRequest{
		Name: "vol1",
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Should have deleted on primary + 2 replicas = 3 servers.
	if len(deletedServers) != 3 {
		t.Fatalf("expected 3 delete calls (primary + 2 replicas), got %d: %v",
			len(deletedServers), deletedServers)
	}
}

// ExpandBlockVolume RF=3 expands all replicas.
func TestMaster_ExpandRF3_ExpandsAllReplicas(t *testing.T) {
	ms := testMasterServerRF3(t)

	var expandedServers []string
	ms.blockVSExpand = func(ctx context.Context, server string, name string, newSize uint64) (uint64, error) {
		expandedServers = append(expandedServers, server)
		return newSize, nil
	}

	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          "vol1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "vol1", NewSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}

	// Should have expanded on primary + 2 replicas = 3 servers.
	if len(expandedServers) != 3 {
		t.Fatalf("expected 3 expand calls (primary + 2 replicas), got %d: %v",
			len(expandedServers), expandedServers)
	}
}

// ProcessAssignments with multi-replica addrs.
func TestMaster_CreateRF3_AssignmentsIncludeReplicaAddrs(t *testing.T) {
	ms := testMasterServerRF3(t)

	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          "vol1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Primary assignment should have ReplicaAddrs with 2 entries.
	assignments := ms.blockAssignmentQueue.Peek(resp.VolumeServer)
	if len(assignments) != 1 {
		t.Fatalf("expected 1 primary assignment, got %d", len(assignments))
	}
	pa := assignments[0]
	if len(pa.ReplicaAddrs) != 2 {
		t.Fatalf("primary assignment ReplicaAddrs: got %d, want 2", len(pa.ReplicaAddrs))
	}
	// Each replica addr should have non-empty data/ctrl.
	for i, ra := range pa.ReplicaAddrs {
		if ra.DataAddr == "" || ra.CtrlAddr == "" {
			t.Fatalf("ReplicaAddrs[%d] has empty addr: %+v", i, ra)
		}
	}
}

// Fix #4: CreateBlockVolumeResponse includes ReplicaServers.
func TestMaster_CreateResponse_IncludesReplicaServers(t *testing.T) {
	ms := testMasterServerRF3(t)
	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          "vol1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	if len(resp.ReplicaServers) != 2 {
		t.Fatalf("ReplicaServers: got %d, want 2", len(resp.ReplicaServers))
	}
	// ReplicaServers should match registry entry Replicas.
	entry, _ := ms.blockRegistry.Lookup("vol1")
	for i, ri := range entry.Replicas {
		if resp.ReplicaServers[i] != ri.Server {
			t.Errorf("ReplicaServers[%d]: got %q, want %q", i, resp.ReplicaServers[i], ri.Server)
		}
	}
	// Backward compat: ReplicaServer (scalar) should be first replica.
	if resp.ReplicaServer != entry.ReplicaServer {
		t.Errorf("ReplicaServer: got %q, want %q", resp.ReplicaServer, entry.ReplicaServer)
	}
}

// Fix #4: LookupBlockVolumeResponse includes ReplicaFactor and ReplicaServers.
func TestMaster_LookupResponse_IncludesReplicaFields(t *testing.T) {
	ms := testMasterServerRF3(t)
	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          "vol1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	resp, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{
		Name: "vol1",
	})
	if err != nil {
		t.Fatalf("LookupBlockVolume: %v", err)
	}
	if resp.ReplicaFactor != 3 {
		t.Fatalf("ReplicaFactor: got %d, want 3", resp.ReplicaFactor)
	}
	if len(resp.ReplicaServers) != 2 {
		t.Fatalf("ReplicaServers: got %d, want 2", len(resp.ReplicaServers))
	}
	// Backward compat: ReplicaServer (scalar).
	if resp.ReplicaServer == "" {
		t.Error("ReplicaServer (scalar) should be non-empty for backward compat")
	}
}

// Fix #4: Idempotent create returns ReplicaServers.
func TestMaster_CreateIdempotent_IncludesReplicaServers(t *testing.T) {
	ms := testMasterServerRF3(t)
	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          "vol1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	// Second create (idempotent) should also include ReplicaServers.
	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          "vol1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	if len(resp.ReplicaServers) != 2 {
		t.Fatalf("idempotent ReplicaServers: got %d, want 2", len(resp.ReplicaServers))
	}
}

// Lookup returns ReplicaFactor=2 for pre-CP8-2 entries where ReplicaFactor is 0.
func TestMaster_LookupResponse_ReplicaFactorDefault(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	// Manually register entry with ReplicaFactor=0 (pre-CP8-2 legacy).
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:         "legacy-vol",
		VolumeServer: "vs1:9333",
		Path:         "/data/legacy-vol.blk",
		SizeBytes:    1 << 30,
		Status:       StatusActive,
		// ReplicaFactor intentionally 0 (zero value).
	})

	resp, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{
		Name: "legacy-vol",
	})
	if err != nil {
		t.Fatalf("LookupBlockVolume: %v", err)
	}
	if resp.ReplicaFactor != 2 {
		t.Fatalf("ReplicaFactor: got %d, want 2 (default for pre-CP8-2)", resp.ReplicaFactor)
	}
}

// ReplicaServers[0] matches ReplicaServer (legacy scalar).
func TestMaster_ResponseConsistency_ReplicaServerVsReplicaServers(t *testing.T) {
	ms := testMasterServerRF3(t)
	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          "vol1",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	if len(resp.ReplicaServers) < 1 {
		t.Fatal("expected at least 1 replica server")
	}
	if resp.ReplicaServer != resp.ReplicaServers[0] {
		t.Fatalf("ReplicaServer=%q != ReplicaServers[0]=%q",
			resp.ReplicaServer, resp.ReplicaServers[0])
	}

	// Same for Lookup.
	lresp, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{
		Name: "vol1",
	})
	if err != nil {
		t.Fatalf("LookupBlockVolume: %v", err)
	}
	if lresp.ReplicaServer != lresp.ReplicaServers[0] {
		t.Fatalf("Lookup: ReplicaServer=%q != ReplicaServers[0]=%q",
			lresp.ReplicaServer, lresp.ReplicaServers[0])
	}
}
