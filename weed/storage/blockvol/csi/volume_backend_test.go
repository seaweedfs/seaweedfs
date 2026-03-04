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
