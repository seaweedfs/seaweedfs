package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// testVolCaps returns a standard volume capability for testing.
func testVolCaps() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{
			Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
		},
		AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}}
}

func testVolCap() *csi.VolumeCapability {
	return testVolCaps()[0]
}

func TestController_CreateVolume(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "test-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if resp.Volume.VolumeId != "test-vol" {
		t.Fatalf("volume_id: got %q, want %q", resp.Volume.VolumeId, "test-vol")
	}
	if resp.Volume.CapacityBytes != 4*1024*1024 {
		t.Fatalf("capacity: got %d, want %d", resp.Volume.CapacityBytes, 4*1024*1024)
	}
	if !mgr.VolumeExists("test-vol") {
		t.Fatal("expected volume to exist")
	}

	// Verify volume_context has iSCSI info.
	if resp.Volume.VolumeContext == nil {
		t.Fatal("expected volume_context to be set")
	}
	if resp.Volume.VolumeContext["iqn"] == "" {
		t.Fatal("expected iqn in volume_context")
	}
}

func TestController_CreateIdempotent(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	req := &csi.CreateVolumeRequest{
		Name: "idem-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
	}

	if _, err := cs.CreateVolume(context.Background(), req); err != nil {
		t.Fatalf("first create: %v", err)
	}

	// Second create with same size should succeed (idempotent).
	resp, err := cs.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if resp.Volume.VolumeId != "idem-vol" {
		t.Fatalf("volume_id: got %q, want %q", resp.Volume.VolumeId, "idem-vol")
	}
}

func TestController_DeleteVolume(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	// Create then delete.
	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "del-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: "del-vol",
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if mgr.VolumeExists("del-vol") {
		t.Fatal("expected volume to not exist after delete")
	}
}

func TestController_DeleteNotFound(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	// Delete non-existent volume -- should succeed (CSI spec idempotency).
	_, err := cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
		VolumeId: "nonexistent",
	})
	if err != nil {
		t.Fatalf("delete non-existent: %v", err)
	}
}
