package weed_server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
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
