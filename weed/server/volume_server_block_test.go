package weed_server

import (
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func createTestBlockVolFile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1024 * 4096,
		BlockSize:  4096,
		ExtentSize: 65536,
		WALSize:    1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	vol.Close()
	return path
}

func TestBlockServiceDisabledByDefault(t *testing.T) {
	// Empty blockDir means feature is disabled.
	bs := StartBlockService("0.0.0.0:3260", "", "", "")
	if bs != nil {
		bs.Shutdown()
		t.Fatal("expected nil BlockService when blockDir is empty")
	}

	// Shutdown on nil should be safe (no panic).
	var nilBS *BlockService
	nilBS.Shutdown()
}

func TestBlockServiceStartAndShutdown(t *testing.T) {
	dir := t.TempDir()
	createTestBlockVolFile(t, dir, "testvol.blk")

	bs := StartBlockService("127.0.0.1:0", dir, "iqn.2024-01.com.test:vol.", "127.0.0.1:3260,1")
	if bs == nil {
		t.Fatal("expected non-nil BlockService")
	}
	defer bs.Shutdown()

	// Verify the volume was registered.
	paths := bs.blockStore.ListBlockVolumes()
	if len(paths) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(paths))
	}

	expected := filepath.Join(dir, "testvol.blk")
	if paths[0] != expected {
		t.Fatalf("expected path %s, got %s", expected, paths[0])
	}
}

// newTestBlockServiceDirect creates a BlockService without iSCSI target for unit testing.
func newTestBlockServiceDirect(t *testing.T) *BlockService {
	t.Helper()
	dir := t.TempDir()
	store := storage.NewBlockVolumeStore()
	t.Cleanup(func() { store.Close() })
	return &BlockService{
		blockStore: store,
		blockDir:   dir,
		listenAddr: "0.0.0.0:3260",
		iqnPrefix:  "iqn.2024-01.com.seaweedfs:vol.",
		replStates: make(map[string]*volReplState),
	}
}

func createTestVolDirect(t *testing.T, bs *BlockService, name string) string {
	t.Helper()
	path := filepath.Join(bs.blockDir, name+".blk")
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{VolumeSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	vol.Close()
	if _, err := bs.blockStore.AddBlockVolume(path, "ssd"); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	return path
}

func TestBlockService_ProcessAssignment_Primary(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary), LeaseTtlMs: 30000},
	})

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	s := vol.Status()
	if s.Role != blockvol.RolePrimary {
		t.Fatalf("expected Primary, got %v", s.Role)
	}
	if s.Epoch != 1 {
		t.Fatalf("expected epoch 1, got %d", s.Epoch)
	}
}

func TestBlockService_ProcessAssignment_Replica(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RoleReplica), LeaseTtlMs: 30000},
	})

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	s := vol.Status()
	if s.Role != blockvol.RoleReplica {
		t.Fatalf("expected Replica, got %v", s.Role)
	}
}

func TestBlockService_ProcessAssignment_UnknownVolume(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	// Should log warning but not panic.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: "/nonexistent.blk", Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary)},
	})
}

func TestBlockService_ProcessAssignment_LeaseRefresh(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary), LeaseTtlMs: 30000},
	})
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary), LeaseTtlMs: 60000},
	})

	vol, _ := bs.blockStore.GetBlockVolume(path)
	s := vol.Status()
	if s.Role != blockvol.RolePrimary || s.Epoch != 1 {
		t.Fatalf("unexpected: role=%v epoch=%d", s.Role, s.Epoch)
	}
}

func TestBlockService_ProcessAssignment_WithReplicaAddrs(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path: path, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs: 30000, ReplicaDataAddr: "10.0.0.2:4260", ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})

	vol, _ := bs.blockStore.GetBlockVolume(path)
	if vol.Status().Role != blockvol.RolePrimary {
		t.Fatalf("expected Primary")
	}
}

func TestBlockService_HeartbeatIncludesReplicaAddrs(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	bs.replMu.Lock()
	bs.replStates[path] = &volReplState{
		replicaDataAddr: "10.0.0.5:4260",
		replicaCtrlAddr: "10.0.0.5:4261",
	}
	bs.replMu.Unlock()

	dataAddr, ctrlAddr := bs.GetReplState(path)
	if dataAddr != "10.0.0.5:4260" || ctrlAddr != "10.0.0.5:4261" {
		t.Fatalf("got data=%q ctrl=%q", dataAddr, ctrlAddr)
	}
}

func TestBlockService_ReplicationPorts_Deterministic(t *testing.T) {
	bs := &BlockService{listenAddr: "0.0.0.0:3260"}
	d1, c1, r1 := bs.ReplicationPorts("/data/vol1.blk")
	d2, c2, r2 := bs.ReplicationPorts("/data/vol1.blk")
	if d1 != d2 || c1 != c2 || r1 != r2 {
		t.Fatalf("ports not deterministic")
	}
	if c1 != d1+1 || r1 != d1+2 {
		t.Fatalf("port offsets wrong: data=%d ctrl=%d rebuild=%d", d1, c1, r1)
	}
}

func TestBlockService_ReplicationPorts_StableAcrossRestarts(t *testing.T) {
	bs1 := &BlockService{listenAddr: "0.0.0.0:3260"}
	bs2 := &BlockService{listenAddr: "0.0.0.0:3260"}
	d1, _, _ := bs1.ReplicationPorts("/data/vol1.blk")
	d2, _, _ := bs2.ReplicationPorts("/data/vol1.blk")
	if d1 != d2 {
		t.Fatalf("ports not stable: %d vs %d", d1, d2)
	}
}

func TestBlockService_ProcessAssignment_InvalidTransition(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	// Assign as primary epoch 5.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 5, Role: blockvol.RoleToWire(blockvol.RolePrimary), LeaseTtlMs: 30000},
	})

	// Try to assign with lower epoch — should be rejected silently.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 3, Role: blockvol.RoleToWire(blockvol.RoleReplica), LeaseTtlMs: 30000},
	})

	vol, _ := bs.blockStore.GetBlockVolume(path)
	s := vol.Status()
	if s.Epoch != 5 {
		t.Fatalf("epoch should still be 5, got %d", s.Epoch)
	}
}
