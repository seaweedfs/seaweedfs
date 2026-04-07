package weed_server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 11 P1: Snapshot product-path rebinding
//
// Proofs:
//   1. Create: product-visible create → backend snapshot → readback metadata
//   2. List: listed snapshots match backend truth after create and delete
//   3. Delete: snapshot removed from visible set, repeated delete idempotent
//   4. Fail-closed: nonexistent volume, missing snapshot
//   5. Reuse boundary: all V1 surfaces explicitly bounded
//
// V1 reuse roles:
//   master_grpc_server_block.go: reuse as bounded adapter (RPC resolution)
//   volume_server_block.go: reuse as bounded adapter (VS execution)
//   blockvol/*: reuse as execution reality only
// ============================================================

// snapshotTestSetup creates a master + BlockService with a real volume,
// wiring the master's VS snapshot callbacks to the real BlockService.
type snapshotTestSetup struct {
	ms    *MasterServer
	bs    *BlockService
	store *storage.BlockVolumeStore
	dir   string
}

func newSnapshotTestSetup(t *testing.T) *snapshotTestSetup {
	t.Helper()
	dir := t.TempDir()
	store := storage.NewBlockVolumeStore()

	bs := &BlockService{
		blockStore: store,
		blockDir:   dir,
		listenAddr: "127.0.0.1:3260",
	}

	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	s := &snapshotTestSetup{ms: ms, bs: bs, store: store, dir: dir}

	// Wire master allocator to create real volumes.
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		sanitized := strings.ReplaceAll(server, ":", "_")
		serverDir := filepath.Join(dir, sanitized)
		if err := os.MkdirAll(serverDir, 0755); err != nil {
			return nil, err
		}
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
		return &blockAllocResult{
			Path:    volPath,
			IQN:     fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr: server + ":3260",
		}, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		return nil
	}

	// Wire master snapshot callbacks through REAL BlockService adapter methods.
	// This proves the VS adapter layer (SnapshotBlockVol, DeleteBlockSnapshot,
	// ListBlockSnapshots) is part of the tested chain, not bypassed.
	//
	// Set blockDir to the vs1 subdir so BlockService.volumePath(name) resolves
	// to the same paths registered in the store.
	bs.blockDir = filepath.Join(dir, strings.ReplaceAll("vs1:9333", ":", "_"))

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

	t.Cleanup(func() { store.Close() })
	return s
}

func (s *snapshotTestSetup) createVolume(t *testing.T, name string) {
	t.Helper()
	ctx := context.Background()
	_, err := s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      name,
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume(%s): %v", name, err)
	}
}

// --- 1. Create: product-visible snapshot creation ---

func TestP11P1_SnapshotCreate(t *testing.T) {
	s := newSnapshotTestSetup(t)
	s.createVolume(t, "snap-vol-1")

	ctx := context.Background()

	// Product-visible create through master RPC.
	createResp, err := s.ms.CreateBlockSnapshot(ctx, &master_pb.CreateBlockSnapshotRequest{
		VolumeName: "snap-vol-1",
		SnapshotId: 1,
	})
	if err != nil {
		t.Fatalf("CreateBlockSnapshot: %v", err)
	}
	if createResp.SnapshotId != 1 {
		t.Fatalf("SnapshotId=%d, want 1", createResp.SnapshotId)
	}
	if createResp.CreatedAt == 0 {
		t.Fatal("CreatedAt should be non-zero")
	}
	if createResp.SizeBytes == 0 {
		t.Fatal("SizeBytes should be non-zero")
	}

	// Verify: snapshot is visible through list.
	listResp, err := s.ms.ListBlockSnapshots(ctx, &master_pb.ListBlockSnapshotsRequest{
		VolumeName: "snap-vol-1",
	})
	if err != nil {
		t.Fatalf("ListBlockSnapshots: %v", err)
	}
	if len(listResp.Snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(listResp.Snapshots))
	}
	listed := listResp.Snapshots[0]
	if listed.SnapshotId != 1 {
		t.Fatalf("listed SnapshotId=%d, want 1", listed.SnapshotId)
	}
	// Metadata equality: create response and list response must agree.
	if listed.CreatedAt != createResp.CreatedAt {
		t.Fatalf("metadata mismatch: create.CreatedAt=%d != list.CreatedAt=%d",
			createResp.CreatedAt, listed.CreatedAt)
	}
	if listed.VolumeSizeBytes != createResp.SizeBytes {
		t.Fatalf("metadata mismatch: create.SizeBytes=%d != list.VolumeSizeBytes=%d",
			createResp.SizeBytes, listed.VolumeSizeBytes)
	}

	t.Logf("P11P1 create: master RPC → VS adapter → blockvol → snap(id=1, created=%d, size=%d) → list metadata matches",
		createResp.CreatedAt, createResp.SizeBytes)
}

// --- 2. List coherence: after create and delete ---

func TestP11P1_SnapshotListCoherence(t *testing.T) {
	s := newSnapshotTestSetup(t)
	s.createVolume(t, "snap-vol-2")

	ctx := context.Background()

	// Create 3 snapshots.
	for i := uint32(1); i <= 3; i++ {
		_, err := s.ms.CreateBlockSnapshot(ctx, &master_pb.CreateBlockSnapshotRequest{
			VolumeName: "snap-vol-2",
			SnapshotId: i,
		})
		if err != nil {
			t.Fatalf("create snap %d: %v", i, err)
		}
	}

	// List: should have 3.
	listResp, err := s.ms.ListBlockSnapshots(ctx, &master_pb.ListBlockSnapshotsRequest{
		VolumeName: "snap-vol-2",
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listResp.Snapshots) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(listResp.Snapshots))
	}

	// Delete snapshot 2.
	_, err = s.ms.DeleteBlockSnapshot(ctx, &master_pb.DeleteBlockSnapshotRequest{
		VolumeName: "snap-vol-2",
		SnapshotId: 2,
	})
	if err != nil {
		t.Fatalf("delete snap 2: %v", err)
	}

	// List: should have 2 (IDs 1 and 3).
	listResp, err = s.ms.ListBlockSnapshots(ctx, &master_pb.ListBlockSnapshotsRequest{
		VolumeName: "snap-vol-2",
	})
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	if len(listResp.Snapshots) != 2 {
		t.Fatalf("expected 2 snapshots after delete, got %d", len(listResp.Snapshots))
	}

	ids := map[uint32]bool{}
	for _, snap := range listResp.Snapshots {
		ids[snap.SnapshotId] = true
	}
	if !ids[1] || !ids[3] {
		t.Fatalf("expected IDs {1,3}, got %v", ids)
	}
	if ids[2] {
		t.Fatal("deleted snapshot 2 should not be in list")
	}

	t.Log("P11P1 list coherence: create 3 → delete 2 → list shows {1,3}")
}

// --- 3. Delete: remove + repeated delete idempotent ---

func TestP11P1_SnapshotDelete(t *testing.T) {
	s := newSnapshotTestSetup(t)
	s.createVolume(t, "snap-vol-3")

	ctx := context.Background()

	// Create and delete.
	s.ms.CreateBlockSnapshot(ctx, &master_pb.CreateBlockSnapshotRequest{
		VolumeName: "snap-vol-3", SnapshotId: 10,
	})

	_, err := s.ms.DeleteBlockSnapshot(ctx, &master_pb.DeleteBlockSnapshotRequest{
		VolumeName: "snap-vol-3", SnapshotId: 10,
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Repeated delete: should be idempotent (no error).
	_, err = s.ms.DeleteBlockSnapshot(ctx, &master_pb.DeleteBlockSnapshotRequest{
		VolumeName: "snap-vol-3", SnapshotId: 10,
	})
	if err != nil {
		t.Fatalf("repeated delete should be idempotent, got: %v", err)
	}

	// List: empty.
	listResp, _ := s.ms.ListBlockSnapshots(ctx, &master_pb.ListBlockSnapshotsRequest{
		VolumeName: "snap-vol-3",
	})
	if len(listResp.Snapshots) != 0 {
		t.Fatalf("expected 0 snapshots, got %d", len(listResp.Snapshots))
	}

	t.Log("P11P1 delete: create → delete → repeated delete idempotent → list empty")
}

// --- 4. Fail-closed: nonexistent volume, duplicate create ---

func TestP11P1_SnapshotFailClosed(t *testing.T) {
	s := newSnapshotTestSetup(t)
	s.createVolume(t, "snap-vol-4")

	ctx := context.Background()

	// Create on nonexistent volume.
	_, err := s.ms.CreateBlockSnapshot(ctx, &master_pb.CreateBlockSnapshotRequest{
		VolumeName: "nonexistent-vol",
		SnapshotId: 1,
	})
	if err == nil {
		t.Fatal("create on nonexistent volume should fail")
	}

	// List on nonexistent volume.
	_, err = s.ms.ListBlockSnapshots(ctx, &master_pb.ListBlockSnapshotsRequest{
		VolumeName: "nonexistent-vol",
	})
	if err == nil {
		t.Fatal("list on nonexistent volume should fail")
	}

	// Delete on nonexistent volume: idempotent (volume gone → snapshot gone).
	_, err = s.ms.DeleteBlockSnapshot(ctx, &master_pb.DeleteBlockSnapshotRequest{
		VolumeName: "nonexistent-vol",
		SnapshotId: 1,
	})
	if err != nil {
		t.Fatalf("delete on nonexistent volume should be idempotent, got: %v", err)
	}

	// Duplicate create (same ID twice).
	s.ms.CreateBlockSnapshot(ctx, &master_pb.CreateBlockSnapshotRequest{
		VolumeName: "snap-vol-4", SnapshotId: 99,
	})
	_, err = s.ms.CreateBlockSnapshot(ctx, &master_pb.CreateBlockSnapshotRequest{
		VolumeName: "snap-vol-4", SnapshotId: 99,
	})
	if err == nil {
		t.Fatal("duplicate create should fail")
	}

	t.Log("P11P1 fail-closed: nonexistent vol, duplicate create all handled correctly")
}
