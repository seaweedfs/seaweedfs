package weed_server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func newTestBlockServiceWithDir(t *testing.T) (*BlockService, string) {
	t.Helper()
	dir := t.TempDir()
	blockDir := filepath.Join(dir, "blocks")
	os.MkdirAll(blockDir, 0755)
	bs := StartBlockService("127.0.0.1:0", blockDir, "iqn.2024.test:", "127.0.0.1:3260,1", NVMeConfig{})
	if bs == nil {
		t.Fatal("StartBlockService returned nil")
	}
	t.Cleanup(func() { bs.Shutdown() })
	return bs, blockDir
}

func TestVS_AllocateBlockVolume(t *testing.T) {
	bs, blockDir := newTestBlockServiceWithDir(t)

	path, iqn, iscsiAddr, err := bs.CreateBlockVol("test-vol", 4*1024*1024, "ssd", "")
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	if path == "" || iqn == "" || iscsiAddr == "" {
		t.Fatalf("empty return values: path=%q iqn=%q addr=%q", path, iqn, iscsiAddr)
	}

	// Verify file exists.
	expectedPath := filepath.Join(blockDir, "test-vol.blk")
	if path != expectedPath {
		t.Fatalf("path: got %q, want %q", path, expectedPath)
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatalf(".blk file not created at %s", path)
	}

	// IQN should contain sanitized name.
	if !strings.Contains(iqn, "test-vol") {
		t.Fatalf("IQN %q should contain 'test-vol'", iqn)
	}
}

func TestVS_AllocateBlockVolume_WithWalSize(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)
	vs := &VolumeServer{blockService: bs}

	resp, err := vs.AllocateBlockVolume(context.Background(), &volume_server_pb.AllocateBlockVolumeRequest{
		Name:         "test-vol-wal",
		SizeBytes:    4 * 1024 * 1024,
		WalSizeBytes: 8 * 1024 * 1024,
		DiskType:     "ssd",
	})
	if err != nil {
		t.Fatalf("AllocateBlockVolume: %v", err)
	}

	vol, err := blockvol.OpenBlockVol(resp.Path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer vol.Close()

	if got := vol.Info().WALSize; got != 8*1024*1024 {
		t.Fatalf("wal_size=%d, want %d", got, 8*1024*1024)
	}
}

func TestVS_AllocateIdempotent(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	path1, iqn1, _, err := bs.CreateBlockVol("vol1", 4*1024*1024, "", "")
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	// Same name+size should return same info.
	path2, iqn2, _, err := bs.CreateBlockVol("vol1", 4*1024*1024, "", "")
	if err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	if path1 != path2 || iqn1 != iqn2 {
		t.Fatalf("idempotent mismatch: (%q,%q) vs (%q,%q)", path1, iqn1, path2, iqn2)
	}
}

func TestVS_AllocateSizeMismatch(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	_, _, _, err := bs.CreateBlockVol("vol1", 4*1024*1024, "", "")
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	// Requesting a LARGER size than existing should fail.
	_, _, _, err = bs.CreateBlockVol("vol1", 8*1024*1024, "", "")
	if err == nil {
		t.Fatal("size mismatch should return error")
	}
}

func TestVS_DeleteBlockVolume(t *testing.T) {
	bs, blockDir := newTestBlockServiceWithDir(t)

	bs.CreateBlockVol("vol1", 4*1024*1024, "", "")
	path := filepath.Join(blockDir, "vol1.blk")

	// File should exist.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file should exist: %v", err)
	}

	if err := bs.DeleteBlockVol("vol1"); err != nil {
		t.Fatalf("DeleteBlockVol: %v", err)
	}

	// File should be gone.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("file should be removed after delete")
	}
}

func TestVS_DeleteNotFound(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	// Deleting non-existent volume should be idempotent.
	if err := bs.DeleteBlockVol("no-such-vol"); err != nil {
		t.Fatalf("delete non-existent should not error: %v", err)
	}
}

func TestVS_SnapshotBlockVol(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	bs.CreateBlockVol("snap-vol", 4*1024*1024, "", "")

	createdAt, sizeBytes, err := bs.SnapshotBlockVol("snap-vol", 1)
	if err != nil {
		t.Fatalf("SnapshotBlockVol: %v", err)
	}
	if createdAt == 0 {
		t.Fatal("createdAt should not be zero")
	}
	if sizeBytes == 0 {
		t.Fatal("sizeBytes should not be zero")
	}
}

func TestVS_SnapshotVolumeNotFound(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	_, _, err := bs.SnapshotBlockVol("nonexistent", 1)
	if err == nil {
		t.Fatal("expected error for nonexistent volume")
	}
}

func TestVS_DeleteBlockSnapshot(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	bs.CreateBlockVol("snap-vol", 4*1024*1024, "", "")
	bs.SnapshotBlockVol("snap-vol", 1)

	if err := bs.DeleteBlockSnapshot("snap-vol", 1); err != nil {
		t.Fatalf("DeleteBlockSnapshot: %v", err)
	}

	// Delete again should be idempotent.
	if err := bs.DeleteBlockSnapshot("snap-vol", 1); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func TestVS_ListBlockSnapshots(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	bs.CreateBlockVol("snap-vol", 4*1024*1024, "", "")
	bs.SnapshotBlockVol("snap-vol", 1)
	bs.SnapshotBlockVol("snap-vol", 2)

	infos, volSize, err := bs.ListBlockSnapshots("snap-vol")
	if err != nil {
		t.Fatalf("ListBlockSnapshots: %v", err)
	}
	if len(infos) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(infos))
	}
	if volSize != 4*1024*1024 {
		t.Fatalf("volSize: got %d, want %d", volSize, 4*1024*1024)
	}
}

func TestVS_ListSnapshotsVolumeNotFound(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	_, _, err := bs.ListBlockSnapshots("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent volume")
	}
}

func TestVS_ExpandBlockVol(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	bs.CreateBlockVol("expand-vol", 4*1024*1024, "", "")

	actualSize, err := bs.ExpandBlockVol("expand-vol", 8*1024*1024)
	if err != nil {
		t.Fatalf("ExpandBlockVol: %v", err)
	}
	if actualSize != 8*1024*1024 {
		t.Fatalf("expected 8MiB, got %d", actualSize)
	}
}

func TestVS_ExpandVolumeNotFound(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)

	_, err := bs.ExpandBlockVol("nonexistent", 8*1024*1024)
	if err == nil {
		t.Fatal("expected error for nonexistent volume")
	}
}

func TestVS_PrepareExpand(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)
	bs.CreateBlockVol("prep-vol", 4*1024*1024, "", "")

	if err := bs.PrepareExpandBlockVol("prep-vol", 8*1024*1024, 1); err != nil {
		t.Fatalf("PrepareExpandBlockVol: %v", err)
	}
}

func TestVS_CommitExpand(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)
	bs.CreateBlockVol("commit-vol", 4*1024*1024, "", "")

	if err := bs.PrepareExpandBlockVol("commit-vol", 8*1024*1024, 42); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	capacity, err := bs.CommitExpandBlockVol("commit-vol", 42)
	if err != nil {
		t.Fatalf("CommitExpandBlockVol: %v", err)
	}
	if capacity != 8*1024*1024 {
		t.Fatalf("capacity: got %d, want %d", capacity, 8*1024*1024)
	}
}

func TestVS_CancelExpand(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)
	bs.CreateBlockVol("cancel-vol", 4*1024*1024, "", "")

	if err := bs.PrepareExpandBlockVol("cancel-vol", 8*1024*1024, 5); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := bs.CancelExpandBlockVol("cancel-vol", 5); err != nil {
		t.Fatalf("CancelExpandBlockVol: %v", err)
	}
}

func TestVS_PrepareExpand_AlreadyInFlight(t *testing.T) {
	bs, _ := newTestBlockServiceWithDir(t)
	bs.CreateBlockVol("inflight-vol", 4*1024*1024, "", "")

	if err := bs.PrepareExpandBlockVol("inflight-vol", 8*1024*1024, 1); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	err := bs.PrepareExpandBlockVol("inflight-vol", 16*1024*1024, 2)
	if err == nil {
		t.Fatal("second prepare should be rejected")
	}
}
