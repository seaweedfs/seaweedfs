package weed_server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	volume_server_pb "github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 11 P4: Snapshot restore workflow closure
//
// Proofs:
//   1. Restore success: product-visible restore reverts to snapshot
//   2. Destructive semantics: post-snapshot writes are lost
//   3. Post-restore coherence: list shows expected snapshot state
//   4. Fail-closed: missing snapshot, missing volume
//
// V1 reuse roles:
//   master_grpc_server_block.go: reuse as bounded adapter (RestoreBlockSnapshot RPC)
//   volume_server_block.go: reuse as bounded adapter (RestoreBlockSnapshot)
//   blockvol.RestoreSnapshot: reuse as execution reality only
// ============================================================

func newRestoreMaster(t *testing.T) (*MasterServer, *storage.BlockVolumeStore, *BlockService) {
	t.Helper()
	dir := t.TempDir()
	store := storage.NewBlockVolumeStore()

	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		sanitized := strings.ReplaceAll(server, ":", "_")
		serverDir := filepath.Join(dir, sanitized)
		os.MkdirAll(serverDir, 0755)
		volPath := filepath.Join(serverDir, fmt.Sprintf("%s.blk", name))
		vol, err := blockvol.CreateBlockVol(volPath, blockvol.CreateOptions{
			VolumeSize: 1 * 1024 * 1024,
			BlockSize:  4096,
			WALSize:    256 * 1024,
		})
		if err != nil {
			return nil, err
		}
		vol.Close()
		if _, err := store.AddBlockVolume(volPath, ""); err != nil {
			return nil, err
		}
		host := server
		if idx := strings.LastIndex(server, ":"); idx >= 0 {
			host = server[:idx]
		}
		return &blockAllocResult{
			Path:      volPath,
			IQN:       fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: host + ":3260",
		}, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error { return nil }

	// Build a real BlockService so ALL callbacks go through the VS adapter.
	// Set blockDir to the vs1 subdir so volumePath(name) resolves correctly.
	bs := &BlockService{
		blockStore: store,
		blockDir:   filepath.Join(dir, strings.ReplaceAll("vs1:9333", ":", "_")),
		listenAddr: "127.0.0.1:3260",
	}

	// Wire snapshot callbacks through real BlockService adapters.
	ms.blockVSSnapshot = func(ctx context.Context, server string, name string, snapID uint32) (int64, uint64, error) {
		return bs.SnapshotBlockVol(name, snapID)
	}
	ms.blockVSDeleteSnap = func(ctx context.Context, server string, name string, snapID uint32) error {
		return bs.DeleteBlockSnapshot(name, snapID)
	}
	ms.blockVSListSnaps = func(ctx context.Context, server string, name string) ([]*volume_server_pb.BlockSnapshotInfo, error) {
		infos, volSize, err := bs.ListBlockSnapshots(name)
		if err != nil {
			return nil, err
		}
		var result []*volume_server_pb.BlockSnapshotInfo
		for _, si := range infos {
			result = append(result, &volume_server_pb.BlockSnapshotInfo{
				SnapshotId:      si.ID,
				CreatedAt:       si.CreatedAt.Unix(),
				VolumeSizeBytes: volSize,
			})
		}
		return result, nil
	}

	// Wire restore callback through REAL BlockService.RestoreBlockSnapshot adapter.
	ms.blockVSRestore = func(ctx context.Context, server string, name string, snapID uint32) error {
		return bs.RestoreBlockSnapshot(name, snapID)
	}

	t.Cleanup(func() { store.Close() })
	return ms, store, bs
}

// --- 1. Restore success: reverts to snapshot ---

func TestP11P4_RestoreSuccess(t *testing.T) {
	ms, store, _ := newRestoreMaster(t)
	ctx := context.Background()

	// Create volume.
	ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "restore-vol-1", SizeBytes: 1 << 20,
	})
	entry, _ := ms.blockRegistry.Lookup("restore-vol-1")

	// Write data, create snapshot.
	store.WithVolume(entry.Path, func(vol *blockvol.BlockVol) error {
		vol.WriteLBA(0, make([]byte, 4096))
		vol.WriteLBA(1, make([]byte, 4096))
		return nil
	})
	ms.CreateBlockSnapshot(ctx, &master_pb.CreateBlockSnapshotRequest{
		VolumeName: "restore-vol-1", SnapshotId: 1,
	})

	// Write MORE data after snapshot.
	store.WithVolume(entry.Path, func(vol *blockvol.BlockVol) error {
		data := make([]byte, 4096)
		for i := range data {
			data[i] = 0xFF
		}
		vol.WriteLBA(0, data) // overwrite LBA 0
		vol.WriteLBA(5, data) // new LBA 5
		return nil
	})

	// Restore to snapshot 1.
	_, err := ms.RestoreBlockSnapshot(ctx, &master_pb.RestoreBlockSnapshotRequest{
		VolumeName: "restore-vol-1", SnapshotId: 1,
	})
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Verify: LBA 0 should be exactly 0x00 (snapshot was taken after zero-fill write).
	var lba0First, lba5First byte
	store.WithVolume(entry.Path, func(vol *blockvol.BlockVol) error {
		data0, _ := vol.ReadLBA(0, 4096)
		lba0First = data0[0]
		data5, _ := vol.ReadLBA(5, 4096)
		lba5First = data5[0]
		return nil
	})
	if lba0First != 0x00 {
		t.Fatalf("LBA 0 = 0x%02X, want 0x00 (reverted to snapshot state)", lba0First)
	}
	if lba5First != 0x00 {
		t.Fatalf("LBA 5 = 0x%02X, want 0x00 (post-snapshot write should be lost)", lba5First)
	}

	t.Logf("P11P4 restore: LBA 0 = 0x%02X (reverted), LBA 5 = 0x%02X (post-snap lost)", lba0First, lba5First)
}

// --- 2. Destructive semantics: snapshots gone after restore ---

func TestP11P4_DestructiveSemantics(t *testing.T) {
	ms, _, _ := newRestoreMaster(t)
	ctx := context.Background()

	ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "restore-vol-2", SizeBytes: 1 << 20,
	})

	// Create snapshots 1, 2, 3.
	for i := uint32(1); i <= 3; i++ {
		ms.CreateBlockSnapshot(ctx, &master_pb.CreateBlockSnapshotRequest{
			VolumeName: "restore-vol-2", SnapshotId: i,
		})
	}

	// Restore to snapshot 1: all snapshots are removed by restore.
	ms.RestoreBlockSnapshot(ctx, &master_pb.RestoreBlockSnapshotRequest{
		VolumeName: "restore-vol-2", SnapshotId: 1,
	})

	// List: should have 0 snapshots (restore removes all).
	listResp, _ := ms.ListBlockSnapshots(ctx, &master_pb.ListBlockSnapshotsRequest{
		VolumeName: "restore-vol-2",
	})
	if len(listResp.Snapshots) != 0 {
		t.Fatalf("expected 0 snapshots after restore, got %d", len(listResp.Snapshots))
	}

	t.Log("P11P4 destructive: restore to snap 1 → all snapshots removed")
}

// --- 3. Fail-closed: missing snapshot ---

func TestP11P4_FailClosed_MissingSnapshot(t *testing.T) {
	ms, _, _ := newRestoreMaster(t)
	ctx := context.Background()

	ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "restore-vol-3", SizeBytes: 1 << 20,
	})

	// Restore nonexistent snapshot.
	_, err := ms.RestoreBlockSnapshot(ctx, &master_pb.RestoreBlockSnapshotRequest{
		VolumeName: "restore-vol-3", SnapshotId: 99,
	})
	if err == nil {
		t.Fatal("restore missing snapshot should fail")
	}
	t.Logf("P11P4 fail-closed: missing snapshot → %v", err)
}

// --- 4. Fail-closed: missing volume ---

func TestP11P4_FailClosed_MissingVolume(t *testing.T) {
	ms, _, _ := newRestoreMaster(t)
	ctx := context.Background()

	_, err := ms.RestoreBlockSnapshot(ctx, &master_pb.RestoreBlockSnapshotRequest{
		VolumeName: "nonexistent-vol", SnapshotId: 1,
	})
	if err == nil {
		t.Fatal("restore on missing volume should fail")
	}
	t.Logf("P11P4 fail-closed: missing volume → %v", err)
}
