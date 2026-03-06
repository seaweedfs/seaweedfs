package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestControllerPublish_HappyPath(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	// Create a volume first.
	mgr.CreateVolume("pub-vol", 4*1024*1024)

	resp, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "pub-vol",
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("ControllerPublishVolume: %v", err)
	}
	if resp.PublishContext == nil {
		t.Fatal("expected publish_context")
	}
	if resp.PublishContext["iscsiAddr"] == "" {
		t.Fatal("expected iscsiAddr in publish_context")
	}
	if resp.PublishContext["iqn"] == "" {
		t.Fatal("expected iqn in publish_context")
	}
}

func TestControllerPublish_MissingVolumeID(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		NodeId: "node-1",
	})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestControllerPublish_MissingNodeID(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "vol1",
	})
	if err == nil {
		t.Fatal("expected error for missing node ID")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestControllerPublish_NotFound(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	_, err := cs.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: "nonexistent",
		NodeId:   "node-1",
	})
	if err == nil {
		t.Fatal("expected error for not found")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", st.Code())
	}
}

func TestControllerUnpublish_Success(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	_, err := cs.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{
		VolumeId: "any-vol",
		NodeId:   "node-1",
	})
	if err != nil {
		t.Fatalf("ControllerUnpublishVolume: %v", err)
	}
}

func TestController_Capabilities(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	resp, err := cs.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("ControllerGetCapabilities: %v", err)
	}

	want := map[csi.ControllerServiceCapability_RPC_Type]bool{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME:   true,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME: true,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT: true,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS:         true,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME:          true,
	}

	got := make(map[csi.ControllerServiceCapability_RPC_Type]bool)
	for _, cap := range resp.Capabilities {
		rpc := cap.GetRpc()
		if rpc != nil {
			got[rpc.Type] = true
		}
	}

	for capType := range want {
		if !got[capType] {
			t.Fatalf("missing capability: %v", capType)
		}
	}
}

func TestController_CreateSnapshot(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	// Create volume first.
	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "snap-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	resp, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "my-snapshot",
		SourceVolumeId: "snap-vol",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if resp.Snapshot.SnapshotId == "" {
		t.Fatal("snapshot ID should not be empty")
	}
	if resp.Snapshot.SourceVolumeId != "snap-vol" {
		t.Fatalf("SourceVolumeId: got %q, want snap-vol", resp.Snapshot.SourceVolumeId)
	}
	if !resp.Snapshot.ReadyToUse {
		t.Fatal("snapshot should be ready")
	}
}

func TestController_CreateSnapshotIdempotent(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "snap-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	// First create.
	resp1, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "my-snapshot",
		SourceVolumeId: "snap-vol",
	})
	if err != nil {
		t.Fatalf("first CreateSnapshot: %v", err)
	}

	// Same name on same volume -- CSI idempotency: should return existing snapshot.
	resp2, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "my-snapshot",
		SourceVolumeId: "snap-vol",
	})
	if err != nil {
		t.Fatalf("idempotent CreateSnapshot: %v", err)
	}
	if resp2.Snapshot.SnapshotId != resp1.Snapshot.SnapshotId {
		t.Fatalf("snapshot IDs differ: %q vs %q", resp2.Snapshot.SnapshotId, resp1.Snapshot.SnapshotId)
	}
	if resp2.Snapshot.SourceVolumeId != "snap-vol" {
		t.Fatalf("source volume: got %q", resp2.Snapshot.SourceVolumeId)
	}
}

func TestController_DeleteSnapshot(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "snap-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	resp, _ := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "del-snap",
		SourceVolumeId: "snap-vol",
	})

	_, err := cs.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: resp.Snapshot.SnapshotId,
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
}

func TestController_DeleteSnapshotNotFound(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	// Delete non-existent snapshot should succeed (idempotent).
	_, err := cs.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: "snap-nonexistent-vol-42",
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot non-existent: %v", err)
	}
}

func TestController_ListSnapshotsByVolume(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "list-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name: "snap-a", SourceVolumeId: "list-vol",
	})
	cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name: "snap-b", SourceVolumeId: "list-vol",
	})

	resp, err := cs.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SourceVolumeId: "list-vol",
	})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(resp.Entries) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(resp.Entries))
	}
}

func TestController_ExpandVolume(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "expand-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	resp, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "expand-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("ControllerExpandVolume: %v", err)
	}
	if resp.CapacityBytes != 8*1024*1024 {
		t.Fatalf("CapacityBytes: got %d, want %d", resp.CapacityBytes, 8*1024*1024)
	}
	if !resp.NodeExpansionRequired {
		t.Fatal("NodeExpansionRequired should be true")
	}
}

func TestController_ExpandNoShrink(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "noshrink-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})

	_, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "noshrink-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected error for shrink")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestController_ExpandMissingVolumeID(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	_, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}
