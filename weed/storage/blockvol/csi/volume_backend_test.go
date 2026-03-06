package csi

import (
	"context"
	"testing"
)

func TestBackend_LocalCreate(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)

	info, err := backend.CreateVolume(context.Background(), "vol1", 4*1024*1024)
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if info.VolumeID != "vol1" {
		t.Fatalf("VolumeID: got %q, want vol1", info.VolumeID)
	}
	if info.CapacityBytes != 4*1024*1024 {
		t.Fatalf("CapacityBytes: got %d, want %d", info.CapacityBytes, 4*1024*1024)
	}
	if info.IQN == "" {
		t.Fatal("IQN should not be empty")
	}

	// Lookup should find it.
	looked, err := backend.LookupVolume(context.Background(), "vol1")
	if err != nil {
		t.Fatalf("LookupVolume: %v", err)
	}
	if looked.VolumeID != "vol1" || looked.IQN != info.IQN {
		t.Fatalf("LookupVolume mismatch: got %+v", looked)
	}
}

func TestBackend_LocalDelete(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)

	if _, err := backend.CreateVolume(context.Background(), "vol1", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := backend.DeleteVolume(context.Background(), "vol1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Lookup should fail.
	if _, err := backend.LookupVolume(context.Background(), "vol1"); err == nil {
		t.Fatal("lookup should fail after delete")
	}
}

func TestBackend_LocalIdempotent(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)

	info1, err := backend.CreateVolume(context.Background(), "vol1", 4*1024*1024)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	// Same name + same size = idempotent.
	info2, err := backend.CreateVolume(context.Background(), "vol1", 4*1024*1024)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}

	if info1.IQN != info2.IQN {
		t.Fatalf("IQN mismatch: %q vs %q", info1.IQN, info2.IQN)
	}
}

func TestBackend_LocalDeleteIdempotent(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)

	// Deleting non-existent volume should not error.
	if err := backend.DeleteVolume(context.Background(), "nonexistent"); err != nil {
		t.Fatalf("delete nonexistent: %v", err)
	}
}

func TestBackend_LocalLookupNotFound(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)

	_, err := backend.LookupVolume(context.Background(), "missing")
	if err == nil {
		t.Fatal("lookup missing should return error")
	}
}

func TestBackend_LocalCreateDeleteListSnapshots(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	ctx := context.Background()

	// Create volume.
	if _, err := backend.CreateVolume(ctx, "snap-vol", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Create snapshot.
	snap, err := backend.CreateSnapshot(ctx, "snap-vol", 1)
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap.SnapshotID != 1 {
		t.Fatalf("SnapshotID: got %d, want 1", snap.SnapshotID)
	}
	if snap.CreatedAt == 0 {
		t.Fatal("CreatedAt should not be zero")
	}

	// List snapshots.
	snaps, err := backend.ListSnapshots(ctx, "snap-vol")
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snaps))
	}

	// Delete snapshot.
	if err := backend.DeleteSnapshot(ctx, "snap-vol", 1); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	// Idempotent delete.
	if err := backend.DeleteSnapshot(ctx, "snap-vol", 1); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}

	// List should be empty now.
	snaps, err = backend.ListSnapshots(ctx, "snap-vol")
	if err != nil {
		t.Fatalf("ListSnapshots after delete: %v", err)
	}
	if len(snaps) != 0 {
		t.Fatalf("expected 0 snapshots after delete, got %d", len(snaps))
	}
}

func TestBackend_LocalExpandVolume(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	ctx := context.Background()

	if _, err := backend.CreateVolume(ctx, "expand-vol", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}

	newSize, err := backend.ExpandVolume(ctx, "expand-vol", 8*1024*1024)
	if err != nil {
		t.Fatalf("ExpandVolume: %v", err)
	}
	if newSize != 8*1024*1024 {
		t.Fatalf("new size: got %d, want %d", newSize, 8*1024*1024)
	}

	// Verify via lookup.
	info, err := backend.LookupVolume(ctx, "expand-vol")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if info.CapacityBytes != 8*1024*1024 {
		t.Fatalf("capacity after expand: got %d, want %d", info.CapacityBytes, 8*1024*1024)
	}
}

func TestBackend_LocalExpandNoShrink(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	ctx := context.Background()

	if _, err := backend.CreateVolume(ctx, "noshrink-vol", 8*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err := backend.ExpandVolume(ctx, "noshrink-vol", 4*1024*1024)
	if err == nil {
		t.Fatal("expected error when shrinking")
	}
}

func TestBackend_LocalSnapshotVolumeNotFound(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	ctx := context.Background()

	_, err := backend.CreateSnapshot(ctx, "missing", 1)
	if err == nil {
		t.Fatal("expected error for missing volume")
	}
}
