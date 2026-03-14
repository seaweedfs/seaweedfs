package weed_server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// testMasterServer creates a minimal MasterServer with mock VS calls for testing.
func testMasterServer(t *testing.T) *MasterServer {
	t.Helper()
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
	}
	// Default mock: succeed with deterministic values.
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
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
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

// ExpandBlockVolume RF=3 uses coordinated prepare/commit on all nodes.
func TestMaster_ExpandRF3_ExpandsAllReplicas(t *testing.T) {
	ms := testMasterServerRF3(t)

	var preparedServers, committedServers []string
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		preparedServers = append(preparedServers, server)
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		committedServers = append(committedServers, server)
		return 2 << 30, nil
	}
	ms.blockVSCancelExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) error {
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

	_, err = ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "vol1", NewSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}

	// Should have prepared on primary + 2 replicas = 3 servers.
	if len(preparedServers) != 3 {
		t.Fatalf("expected 3 prepare calls, got %d: %v", len(preparedServers), preparedServers)
	}
	if len(committedServers) != 3 {
		t.Fatalf("expected 3 commit calls, got %d: %v", len(committedServers), committedServers)
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

func TestMaster_NvmeFieldsFlowThroughCreateAndLookup(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	// Mock: VS returns NVMe fields.
	ms.blockVSAllocate = func(ctx context.Context, server, name string, sizeBytes uint64, diskType, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:      fmt.Sprintf("/data/%s.blk", name),
			IQN:       fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: server,
			NvmeAddr:  "10.0.0.1:4420",
			NQN:       fmt.Sprintf("nqn.2024-01.com.seaweedfs:vol.%s", name),
		}, nil
	}

	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "nvme-vol",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	if resp.NvmeAddr != "10.0.0.1:4420" {
		t.Fatalf("CreateResponse.NvmeAddr: got %q, want 10.0.0.1:4420", resp.NvmeAddr)
	}
	if resp.Nqn != "nqn.2024-01.com.seaweedfs:vol.nvme-vol" {
		t.Fatalf("CreateResponse.Nqn: got %q", resp.Nqn)
	}

	// Verify registry entry.
	entry, ok := ms.blockRegistry.Lookup("nvme-vol")
	if !ok {
		t.Fatal("volume not found in registry")
	}
	if entry.NvmeAddr != "10.0.0.1:4420" {
		t.Fatalf("entry.NvmeAddr: got %q", entry.NvmeAddr)
	}
	if entry.NQN != "nqn.2024-01.com.seaweedfs:vol.nvme-vol" {
		t.Fatalf("entry.NQN: got %q", entry.NQN)
	}

	// Lookup should also return NVMe fields.
	lresp, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{
		Name: "nvme-vol",
	})
	if err != nil {
		t.Fatalf("LookupBlockVolume: %v", err)
	}
	if lresp.NvmeAddr != "10.0.0.1:4420" {
		t.Fatalf("LookupResponse.NvmeAddr: got %q", lresp.NvmeAddr)
	}
	if lresp.Nqn != "nqn.2024-01.com.seaweedfs:vol.nvme-vol" {
		t.Fatalf("LookupResponse.Nqn: got %q", lresp.Nqn)
	}
}

func TestMaster_NoNvmeFieldsWhenDisabled(t *testing.T) {
	ms := testMasterServer(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	// Default mock returns no NVMe fields (NVMe disabled).
	resp, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:      "iscsi-only-vol",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	if resp.NvmeAddr != "" {
		t.Fatalf("NvmeAddr should be empty when NVMe disabled, got %q", resp.NvmeAddr)
	}
	if resp.Nqn != "" {
		t.Fatalf("Nqn should be empty when NVMe disabled, got %q", resp.Nqn)
	}

	lresp, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{
		Name: "iscsi-only-vol",
	})
	if err != nil {
		t.Fatalf("LookupBlockVolume: %v", err)
	}
	if lresp.NvmeAddr != "" || lresp.Nqn != "" {
		t.Fatalf("Lookup NVMe fields should be empty, got addr=%q nqn=%q", lresp.NvmeAddr, lresp.Nqn)
	}
}

func TestMaster_PromotionCopiesNvmeFields(t *testing.T) {
	ms := testMasterServer(t)
	// Mark servers as block-capable so promotion Gate 4 (liveness) passes.
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	// Directly register an entry with primary + replica, both having NVMe fields.
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:         "ha-vol",
		VolumeServer: "vs1:9333",
		Path:         "/data/ha-vol.blk",
		IQN:          "iqn.2024.test:ha-vol",
		ISCSIAddr:    "vs1:3260",
		NvmeAddr:     "vs1:4420",
		NQN:          "nqn.2024-01.com.seaweedfs:vol.ha-vol.vs1",
		SizeBytes:    1 << 30,
		Epoch:        5,
		Role:         1, // RolePrimary
		LeaseTTL:     30 * time.Second,
		Replicas: []ReplicaInfo{
			{
				Server:        "vs2:9333",
				Path:          "/data/ha-vol.blk",
				IQN:           "iqn.2024.test:ha-vol-r",
				ISCSIAddr:     "vs2:3260",
				NvmeAddr:      "vs2:4420",
				NQN:           "nqn.2024-01.com.seaweedfs:vol.ha-vol.vs2",
				DataAddr:      "vs2:14260",
				CtrlAddr:      "vs2:14261",
				HealthScore:   0.95,
				WALHeadLSN:    100,
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				LastHeartbeat: time.Now(),
			},
		},
	})
	// Wire byServer index for replica.
	ms.blockRegistry.mu.Lock()
	ms.blockRegistry.addToServer("vs2:9333", "ha-vol")
	ms.blockRegistry.mu.Unlock()

	// Pre-promotion: verify primary NVMe fields.
	entry, _ := ms.blockRegistry.Lookup("ha-vol")
	if entry.NvmeAddr != "vs1:4420" {
		t.Fatalf("pre-promotion NvmeAddr: got %q, want vs1:4420", entry.NvmeAddr)
	}

	// Promote replica.
	newEpoch, err := ms.blockRegistry.PromoteBestReplica("ha-vol")
	if err != nil {
		t.Fatalf("PromoteBestReplica: %v", err)
	}
	if newEpoch != 6 {
		t.Fatalf("newEpoch: got %d, want 6", newEpoch)
	}

	// After promotion: entry should have replica's NVMe fields.
	entry, _ = ms.blockRegistry.Lookup("ha-vol")
	if entry.VolumeServer != "vs2:9333" {
		t.Fatalf("after promotion, VolumeServer: got %q, want vs2:9333", entry.VolumeServer)
	}
	if entry.NvmeAddr != "vs2:4420" {
		t.Fatalf("after promotion, NvmeAddr: got %q, want vs2:4420", entry.NvmeAddr)
	}
	if entry.NQN != "nqn.2024-01.com.seaweedfs:vol.ha-vol.vs2" {
		t.Fatalf("after promotion, NQN: got %q", entry.NQN)
	}

	// Lookup should return the promoted replica's NVMe fields immediately.
	lresp, err := ms.LookupBlockVolume(context.Background(), &master_pb.LookupBlockVolumeRequest{
		Name: "ha-vol",
	})
	if err != nil {
		t.Fatalf("LookupBlockVolume: %v", err)
	}
	if lresp.NvmeAddr != "vs2:4420" {
		t.Fatalf("Lookup NvmeAddr after promotion: got %q, want vs2:4420", lresp.NvmeAddr)
	}
	if lresp.Nqn != "nqn.2024-01.com.seaweedfs:vol.ha-vol.vs2" {
		t.Fatalf("Lookup Nqn after promotion: got %q", lresp.Nqn)
	}
}

// ============================================================
// CP11A-2: Coordinated Expand Tests
// ============================================================

func testMasterServerWithExpandMocks(t *testing.T) *MasterServer {
	t.Helper()
	ms := testMasterServer(t)
	ms.blockVSExpand = func(ctx context.Context, server string, name string, newSize uint64) (uint64, error) {
		return newSize, nil
	}
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		return 2 << 30, nil
	}
	ms.blockVSCancelExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) error {
		return nil
	}
	return ms
}

func TestMaster_ExpandCoordinated_Success(t *testing.T) {
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":4001",
			ReplicaCtrlAddr: server + ":4002",
		}, nil
	}

	var prepareCount, commitCount int
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		prepareCount++
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		commitCount++
		return 2 << 30, nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "coord-vol", SizeBytes: 1 << 30,
	})

	resp, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "coord-vol", NewSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	if resp.CapacityBytes != 2<<30 {
		t.Fatalf("capacity: got %d, want %d", resp.CapacityBytes, 2<<30)
	}
	if prepareCount != 2 {
		t.Fatalf("expected 2 prepare calls (primary+replica), got %d", prepareCount)
	}
	if commitCount != 2 {
		t.Fatalf("expected 2 commit calls, got %d", commitCount)
	}
	entry, _ := ms.blockRegistry.Lookup("coord-vol")
	if entry.SizeBytes != 2<<30 {
		t.Fatalf("registry size: got %d, want %d", entry.SizeBytes, 2<<30)
	}
}

func TestMaster_ExpandCoordinated_PrepareFailure_Cancels(t *testing.T) {
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":4001",
			ReplicaCtrlAddr: server + ":4002",
		}, nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "cancel-vol", SizeBytes: 1 << 30,
	})

	// Determine which server is primary so we can fail the replica.
	entry, _ := ms.blockRegistry.Lookup("cancel-vol")
	primaryServer := entry.VolumeServer

	var cancelCount int
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		if server != primaryServer {
			return fmt.Errorf("replica prepare failed")
		}
		return nil
	}
	ms.blockVSCancelExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) error {
		cancelCount++
		return nil
	}

	_, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "cancel-vol", NewSizeBytes: 2 << 30,
	})
	if err == nil {
		t.Fatal("expected error when replica prepare fails")
	}
	if cancelCount != 1 {
		t.Fatalf("expected 1 cancel call (primary was prepared), got %d", cancelCount)
	}
	entry, _ = ms.blockRegistry.Lookup("cancel-vol")
	if entry.SizeBytes != 1<<30 {
		t.Fatalf("registry size should be unchanged: got %d", entry.SizeBytes)
	}
}

func TestMaster_ExpandCoordinated_Standalone_DirectCommit(t *testing.T) {
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	var expandCalled bool
	ms.blockVSExpand = func(ctx context.Context, server string, name string, newSize uint64) (uint64, error) {
		expandCalled = true
		return newSize, nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "solo-vol", SizeBytes: 1 << 30,
	})

	resp, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "solo-vol", NewSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	if !expandCalled {
		t.Fatal("standalone should use direct expand, not prepare/commit")
	}
	if resp.CapacityBytes != 2<<30 {
		t.Fatalf("capacity: got %d", resp.CapacityBytes)
	}
}

func TestMaster_ExpandCoordinated_ConcurrentRejected(t *testing.T) {
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":4001",
			ReplicaCtrlAddr: server + ":4002",
		}, nil
	}

	// Make prepare block until we release it.
	blockCh := make(chan struct{})
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		<-blockCh
		return nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "conc-vol", SizeBytes: 1 << 30,
	})

	// First expand acquires inflight.
	errCh := make(chan error, 1)
	go func() {
		_, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
			Name: "conc-vol", NewSizeBytes: 2 << 30,
		})
		errCh <- err
	}()

	// Give goroutine time to acquire lock.
	time.Sleep(20 * time.Millisecond)

	// Second expand should be rejected.
	_, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "conc-vol", NewSizeBytes: 2 << 30,
	})
	if err == nil {
		t.Fatal("concurrent expand should be rejected")
	}

	// Release the first expand.
	close(blockCh)
	<-errCh
}

func TestMaster_ExpandCoordinated_Idempotent(t *testing.T) {
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "idem-vol", SizeBytes: 1 << 30,
	})

	// Same size expand: standalone path, Expand handles no-op internally.
	ms.blockVSExpand = func(ctx context.Context, server string, name string, newSize uint64) (uint64, error) {
		return 1 << 30, nil // return current size
	}

	resp, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "idem-vol", NewSizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("idempotent expand: %v", err)
	}
	if resp.CapacityBytes != 1<<30 {
		t.Fatalf("capacity: got %d", resp.CapacityBytes)
	}
}

func TestMaster_ExpandCoordinated_CommitFailure_MarksInconsistent(t *testing.T) {
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":4001",
			ReplicaCtrlAddr: server + ":4002",
		}, nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "fail-vol", SizeBytes: 1 << 30,
	})

	// Determine which server is primary so we fail only the replica's commit.
	entry, _ := ms.blockRegistry.Lookup("fail-vol")
	primaryServer := entry.VolumeServer

	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		if server != primaryServer {
			return 0, fmt.Errorf("replica commit failed")
		}
		return 2 << 30, nil
	}

	_, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "fail-vol", NewSizeBytes: 2 << 30,
	})
	if err == nil {
		t.Fatal("expected error when replica commit fails")
	}

	// Registry size should NOT be updated (inconsistent state).
	entry, _ = ms.blockRegistry.Lookup("fail-vol")
	if entry.SizeBytes != 1<<30 {
		t.Fatalf("registry size should be unchanged: got %d", entry.SizeBytes)
	}

	// Finding 1: ExpandFailed must be true, ExpandInProgress must stay true
	// so heartbeat cannot overwrite SizeBytes with the primary's new committed size.
	if !entry.ExpandFailed {
		t.Fatal("entry.ExpandFailed should be true after partial commit failure")
	}
	if !entry.ExpandInProgress {
		t.Fatal("entry.ExpandInProgress should stay true to suppress heartbeat size updates")
	}

	// Finding 2: PendingExpandSize and ExpandEpoch should be populated for diagnosis.
	if entry.PendingExpandSize != 2<<30 {
		t.Fatalf("entry.PendingExpandSize: got %d, want %d", entry.PendingExpandSize, 2<<30)
	}
	if entry.ExpandEpoch == 0 {
		t.Fatal("entry.ExpandEpoch should be non-zero")
	}

	// A new expand should be rejected while ExpandFailed is set.
	_, err = ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "fail-vol", NewSizeBytes: 2 << 30,
	})
	if err == nil {
		t.Fatal("expand should be rejected while ExpandFailed is set")
	}

	// ClearExpandFailed unblocks new expands.
	ms.blockRegistry.ClearExpandFailed("fail-vol")
	entry, _ = ms.blockRegistry.Lookup("fail-vol")
	if entry.ExpandFailed || entry.ExpandInProgress {
		t.Fatal("ClearExpandFailed should reset both flags")
	}
}

func TestMaster_ExpandCoordinated_HeartbeatSuppressedAfterPartialCommit(t *testing.T) {
	// Bug 1 regression: after primary commits but replica fails,
	// a heartbeat from the primary reporting the new VolumeSize
	// must NOT update the registry SizeBytes.
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":4001",
			ReplicaCtrlAddr: server + ":4002",
		}, nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "hb-vol", SizeBytes: 1 << 30,
	})

	entry, _ := ms.blockRegistry.Lookup("hb-vol")
	primaryServer := entry.VolumeServer

	// Fail replica commit.
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		if server != primaryServer {
			return 0, fmt.Errorf("replica commit failed")
		}
		return 2 << 30, nil
	}

	ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "hb-vol", NewSizeBytes: 2 << 30,
	})

	// Volume is now in ExpandFailed state, ExpandInProgress=true.
	// Simulate primary heartbeat reporting VolumeSize = 2 GiB (primary already committed).
	ms.blockRegistry.UpdateFullHeartbeat(primaryServer, []*master_pb.BlockVolumeInfoMessage{
		{
			Path:       fmt.Sprintf("/data/%s.blk", "hb-vol"),
			VolumeSize: 2 << 30, // primary's new committed size
			Epoch:      1,
			Role:       1,
		},
	})

	// Registry size must still be the OLD size — heartbeat must not leak the new size.
	entry, _ = ms.blockRegistry.Lookup("hb-vol")
	if entry.SizeBytes != 1<<30 {
		t.Fatalf("heartbeat leaked new size: got %d, want %d", entry.SizeBytes, 1<<30)
	}
	if !entry.ExpandFailed {
		t.Fatal("ExpandFailed should still be true after heartbeat")
	}
}

func TestMaster_ExpandCoordinated_FailoverDuringPrepare(t *testing.T) {
	// Scenario: primary and replica are prepared but commit hasn't happened.
	// On recovery (OpenBlockVol), prepared state is cleared → VolumeSize stays at old.
	// This test validates at the registry/coordinator level.
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":4001",
			ReplicaCtrlAddr: server + ":4002",
		}, nil
	}

	// Prepare succeeds but commit on primary fails (simulating crash).
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		return nil
	}
	var cancelCount int
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		return 0, fmt.Errorf("primary crashed during commit")
	}
	ms.blockVSCancelExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) error {
		cancelCount++
		return nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "failover-vol", SizeBytes: 1 << 30,
	})

	_, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "failover-vol", NewSizeBytes: 2 << 30,
	})
	if err == nil {
		t.Fatal("expected error when primary commit fails")
	}

	// Cancel should have been called on all prepared nodes.
	if cancelCount < 1 {
		t.Fatalf("expected cancel calls, got %d", cancelCount)
	}

	// Registry size should be unchanged.
	entry, _ := ms.blockRegistry.Lookup("failover-vol")
	if entry.SizeBytes != 1<<30 {
		t.Fatalf("registry size should be unchanged: got %d", entry.SizeBytes)
	}
}

func TestMaster_ExpandCoordinated_RestartRecovery(t *testing.T) {
	// After node restart with prepared state, OpenBlockVol clears it.
	// Master re-driving expand would go through full prepare/commit again.
	// This test verifies the coordinator doesn't get stuck after a failed expand.
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":4001",
			ReplicaCtrlAddr: server + ":4002",
		}, nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "restart-vol", SizeBytes: 1 << 30,
	})

	// First expand fails at commit.
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		return 0, fmt.Errorf("crash")
	}
	ms.blockVSCancelExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) error {
		return nil
	}

	ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "restart-vol", NewSizeBytes: 2 << 30,
	})

	// After "restart" (inflight released), retry should work.
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		return 2 << 30, nil
	}

	resp, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "restart-vol", NewSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("retry expand: %v", err)
	}
	if resp.CapacityBytes != 2<<30 {
		t.Fatalf("capacity: got %d", resp.CapacityBytes)
	}
}

func TestMaster_ExpandCoordinated_B09_ReReadsEntryAfterLock(t *testing.T) {
	// B-09: Exercises the actual race window — failover happens BETWEEN
	// the initial Lookup (line 380) and the post-lock re-read (line 419).
	// Uses expandPreReadHook to inject PromoteBestReplica at the exact
	// interleaving point. RF=3 so promotion leaves 1 replica and the
	// coordinated path is taken.
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockRegistry.MarkBlockCapable("vs3:9333")
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":4001",
			ReplicaCtrlAddr: server + ":4002",
		}, nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "b09-vol", SizeBytes: 1 << 30, ReplicaFactor: 3,
	})

	entry, _ := ms.blockRegistry.Lookup("b09-vol")
	originalPrimary := entry.VolumeServer

	// Record which servers receive PREPARE to verify the fresh entry is used.
	var preparedServers []string
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		preparedServers = append(preparedServers, server)
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		return 2 << 30, nil
	}

	// Hook fires AFTER AcquireExpandInflight but BEFORE the re-read Lookup.
	// This is the exact race window: the initial Lookup already returned
	// the old primary, but failover changes it before the re-read.
	hookFired := false
	ms.expandPreReadHook = func() {
		hookFired = true
		ms.blockRegistry.PromoteBestReplica("b09-vol")
	}

	// At this point, the initial Lookup inside ExpandBlockVolume will see
	// originalPrimary. The hook then promotes, changing the primary.
	// The re-read must pick up the new primary.
	resp, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "b09-vol", NewSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	if !hookFired {
		t.Fatal("expandPreReadHook was not called — race window not exercised")
	}
	if resp.CapacityBytes != 2<<30 {
		t.Fatalf("capacity: got %d", resp.CapacityBytes)
	}

	// Verify: after the hook promoted, the re-read must have picked up
	// the new primary. The first PREPARE should go to the new primary.
	entry, _ = ms.blockRegistry.Lookup("b09-vol")
	newPrimary := entry.VolumeServer
	if newPrimary == originalPrimary {
		t.Fatal("promotion didn't change primary")
	}

	if len(preparedServers) == 0 {
		t.Fatal("no prepare calls recorded")
	}
	if preparedServers[0] != newPrimary {
		t.Fatalf("PREPARE went to %q (stale), should go to %q (fresh primary)",
			preparedServers[0], newPrimary)
	}
	// Verify old primary was NOT contacted at all.
	for _, s := range preparedServers {
		if s == originalPrimary {
			t.Fatalf("PREPARE sent to old primary %q — stale entry used", originalPrimary)
		}
	}
}

func TestMaster_ExpandCoordinated_B10_HeartbeatDoesNotDeleteDuringExpand(t *testing.T) {
	// B-10: A full heartbeat from a restarted primary must not delete
	// the registry entry while a coordinated expand is in progress.
	ms := testMasterServerWithExpandMocks(t)
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:            fmt.Sprintf("/data/%s.blk", name),
			IQN:             fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr:       server,
			ReplicaDataAddr: server + ":4001",
			ReplicaCtrlAddr: server + ":4002",
		}, nil
	}

	ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "b10-vol", SizeBytes: 1 << 30,
	})

	entry, _ := ms.blockRegistry.Lookup("b10-vol")
	primaryServer := entry.VolumeServer

	// Simulate: coordinated expand is in flight (acquire the lock).
	expandEpoch := uint64(42)
	if !ms.blockRegistry.AcquireExpandInflight("b10-vol", 2<<30, expandEpoch) {
		t.Fatal("failed to acquire expand inflight")
	}

	// Now simulate primary VS restart: full heartbeat that does NOT report
	// the volume (it hasn't loaded it yet). Without B-10 fix, this deletes
	// the entry from the registry.
	ms.blockRegistry.UpdateFullHeartbeat(primaryServer, []*master_pb.BlockVolumeInfoMessage{
		// Empty: primary restarted and hasn't loaded this volume yet.
	})

	// Entry must still exist — expand is in progress.
	_, ok := ms.blockRegistry.Lookup("b10-vol")
	if !ok {
		t.Fatal("entry deleted during coordinated expand — B-10 not fixed")
	}

	// Verify expand state is preserved.
	entry, _ = ms.blockRegistry.Lookup("b10-vol")
	if !entry.ExpandInProgress {
		t.Fatal("ExpandInProgress should still be true")
	}
	if entry.ExpandEpoch != expandEpoch {
		t.Fatalf("ExpandEpoch: got %d, want %d", entry.ExpandEpoch, expandEpoch)
	}

	// Cleanup.
	ms.blockRegistry.ReleaseExpandInflight("b10-vol")
}
