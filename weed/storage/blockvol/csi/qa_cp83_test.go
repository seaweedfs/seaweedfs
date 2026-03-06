package csi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestQA_Snap_CreateOnNonExistentVolume verifies NotFound when creating snapshot
// on a volume that doesn't exist.
func TestQA_Snap_CreateOnNonExistentVolume(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	_, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "orphan-snap",
		SourceVolumeId: "nonexistent-volume",
	})
	if err == nil {
		t.Fatal("expected error for snapshot on nonexistent volume")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v: %s", st.Code(), st.Message())
	}
}

// TestQA_Snap_DeleteNonExistentSnapshot verifies idempotent success when deleting
// a snapshot that doesn't exist (CSI spec: delete should succeed).
func TestQA_Snap_DeleteNonExistentSnapshot(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	// Create volume but no snapshot.
	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "del-snap-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	// Delete a well-formed snapshot ID for an existing volume, but snapshot doesn't exist.
	snapID := FormatSnapshotID("del-snap-vol", 99999)
	_, err := cs.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
		SnapshotId: snapID,
	})
	if err != nil {
		t.Fatalf("delete nonexistent snapshot should succeed (idempotent), got: %v", err)
	}
}

// TestQA_Snap_SnapshotIDParsing exercises ParseSnapshotID with weird, empty,
// and unicode strings. Delete should treat unparseable IDs as idempotent success.
func TestQA_Snap_SnapshotIDParsing(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cases := []struct {
		name string
		id   string
	}{
		{"empty", ""},
		{"no prefix", "garbage-123"},
		{"only prefix", "snap-"},
		{"no numeric suffix", "snap-vol-abc"},
		{"unicode volume", "snap-日本語ボリューム-42"},
		{"missing separator", "snap-noseparator"},
		{"double dash", "snap--42"},
		{"huge number", "snap-vol-99999999999999999999"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.id == "" {
				// Empty ID → InvalidArgument from DeleteSnapshot.
				_, err := cs.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
					SnapshotId: tc.id,
				})
				if err == nil {
					t.Fatal("expected error for empty snapshot ID")
				}
				st, _ := status.FromError(err)
				if st.Code() != codes.InvalidArgument {
					t.Fatalf("expected InvalidArgument for empty ID, got %v", st.Code())
				}
				return
			}

			// Non-empty but unparseable → idempotent success.
			_, err := cs.DeleteSnapshot(context.Background(), &csi.DeleteSnapshotRequest{
				SnapshotId: tc.id,
			})
			if err != nil {
				t.Fatalf("delete unparseable snapshot ID should succeed (idempotent), got: %v", err)
			}
		})
	}
}

// TestQA_Snap_ExpandWithActiveSnapshots verifies FailedPrecondition when expanding
// a volume that has active snapshots.
func TestQA_Snap_ExpandWithActiveSnapshots(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	// Create volume.
	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "snap-expand-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	// Create snapshot.
	_, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "blocking-snap",
		SourceVolumeId: "snap-expand-vol",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Now try to expand --engine should reject due to active snapshots.
	_, err = cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "snap-expand-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected error for expand with active snapshots")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v: %s", st.Code(), st.Message())
	}
	if !strings.Contains(st.Message(), "snapshots") {
		t.Fatalf("error message should mention snapshots: %s", st.Message())
	}
}

// TestQA_Snap_SnapshotCollisionHandled tests that two different names hashing to
// the same FNV-32a ID are handled with AlreadyExists.
func TestQA_Snap_SnapshotCollisionHandled(t *testing.T) {
	// Use a mock backend that always returns "already exists" after the first create,
	// simulating what happens when two names produce the same FNV-32a hash.
	mock := &collisionMockBackend{
		createCount: 0,
	}
	cs := &controllerServer{backend: mock}

	// First snapshot succeeds.
	_, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "first-name",
		SourceVolumeId: "collision-vol",
	})
	if err != nil {
		t.Fatalf("first CreateSnapshot: %v", err)
	}

	// Second snapshot: mock simulates FNV collision by returning "already exists".
	_, err = cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "second-name",
		SourceVolumeId: "collision-vol",
	})
	if err == nil {
		t.Fatal("expected error for collision")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v: %s", st.Code(), st.Message())
	}
	if !strings.Contains(st.Message(), "collision") {
		t.Fatalf("error message should mention collision: %s", st.Message())
	}
}

// TestQA_Expand_RescanFailure verifies that NodeExpandVolume handles rescan failure
// by returning Internal error.
func TestQA_Expand_RescanFailure(t *testing.T) {
	mi := newMockISCSIUtil()
	mi.getDeviceResult = "/dev/sdb"
	mi.rescanErr = errors.New("iscsiadm rescan: device not found")
	mm := newMockMountUtil()

	ns := &nodeServer{
		mgr:       nil,
		nodeID:    "test-node-1",
		iqnPrefix: "iqn.2024.com.seaweedfs",
		iscsiUtil: mi,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[test-node] ", log.LstdFlags),
		staged: map[string]*stagedVolumeInfo{
			"rescan-fail-vol": {
				iqn:         "iqn.2024.com.seaweedfs:rescan-fail-vol",
				iscsiAddr:   "10.0.0.5:3260",
				isLocal:     false,
				fsType:      "ext4",
				stagingPath: "/staging/rescan-fail-vol",
			},
		},
	}

	// Mark as logged in so session check passes.
	mi.loggedIn["iqn.2024.com.seaweedfs:rescan-fail-vol"] = true

	_, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:      "rescan-fail-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 16 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected error from rescan failure")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %v: %s", st.Code(), st.Message())
	}
}

// TestQA_FullLifecycleWithSnapshotExpand tests the full lifecycle:
// create volume → snapshot → delete snapshot → expand → verify.
func TestQA_FullLifecycleWithSnapshotExpand(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}
	ctx := context.Background()

	// 1. Create volume.
	volResp, err := cs.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "lifecycle-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if volResp.Volume.CapacityBytes != 4*1024*1024 {
		t.Fatalf("initial capacity: got %d", volResp.Volume.CapacityBytes)
	}

	// 2. Create snapshot.
	snapResp, err := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name:           "lifecycle-snap",
		SourceVolumeId: "lifecycle-vol",
	})
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if !snapResp.Snapshot.ReadyToUse {
		t.Fatal("snapshot should be ready")
	}
	snapID := snapResp.Snapshot.SnapshotId

	// 3. List snapshots --should find it.
	listResp, err := cs.ListSnapshots(ctx, &csi.ListSnapshotsRequest{
		SourceVolumeId: "lifecycle-vol",
	})
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(listResp.Entries) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(listResp.Entries))
	}

	// 4. Expand should fail with active snapshot.
	_, err = cs.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      "lifecycle-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected expand to fail with active snapshot")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", st.Code())
	}

	// 5. Delete snapshot.
	_, err = cs.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{
		SnapshotId: snapID,
	})
	if err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}

	// 6. Now expand should succeed.
	expandResp, err := cs.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      "lifecycle-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("ControllerExpandVolume: %v", err)
	}
	if expandResp.CapacityBytes != 8*1024*1024 {
		t.Fatalf("expanded capacity: got %d, want %d", expandResp.CapacityBytes, 8*1024*1024)
	}
	if !expandResp.NodeExpansionRequired {
		t.Fatal("NodeExpansionRequired should be true")
	}

	// 7. List snapshots --should be empty now.
	listResp, err = cs.ListSnapshots(ctx, &csi.ListSnapshotsRequest{
		SourceVolumeId: "lifecycle-vol",
	})
	if err != nil {
		t.Fatalf("ListSnapshots after delete: %v", err)
	}
	if len(listResp.Entries) != 0 {
		t.Fatalf("expected 0 snapshots after delete, got %d", len(listResp.Entries))
	}
}

// TestQA_Snap_ListSnapshotsBackendError verifies that ListSnapshots propagates
// backend errors as Internal instead of silently returning empty list.
func TestQA_Snap_ListSnapshotsBackendError(t *testing.T) {
	mock := &errorBackend{
		listErr: fmt.Errorf("connection refused"),
	}
	cs := &controllerServer{backend: mock}

	// By snapshot_id.
	_, err := cs.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SnapshotId: FormatSnapshotID("vol1", 42),
	})
	if err == nil {
		t.Fatal("expected error from backend failure")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %v: %s", st.Code(), st.Message())
	}

	// By source_volume_id.
	_, err = cs.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SourceVolumeId: "vol1",
	})
	if err == nil {
		t.Fatal("expected error from backend failure")
	}
	st, _ = status.FromError(err)
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %v: %s", st.Code(), st.Message())
	}
}

// TestQA_Snap_ListSnapshotsNotFoundVolume verifies that ListSnapshots returns
// empty list (not error) when volume is not found.
func TestQA_Snap_ListSnapshotsNotFoundVolume(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	resp, err := cs.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SourceVolumeId: "nonexistent-vol",
	})
	if err != nil {
		t.Fatalf("ListSnapshots on missing volume should succeed, got: %v", err)
	}
	if len(resp.Entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(resp.Entries))
	}
}

// --- collisionMockBackend ---

// collisionMockBackend is a VolumeBackend that simulates FNV-32a hash collision
// on CreateSnapshot. The first CreateSnapshot succeeds; all subsequent ones
// return "snapshot already exists" to simulate what happens when two different
// names produce the same FNV-32a hash.
type collisionMockBackend struct {
	createCount int
}

func (m *collisionMockBackend) CreateVolume(_ context.Context, name string, sizeBytes uint64) (*VolumeInfo, error) {
	return &VolumeInfo{VolumeID: name, CapacityBytes: sizeBytes}, nil
}

func (m *collisionMockBackend) DeleteVolume(_ context.Context, name string) error {
	return nil
}

func (m *collisionMockBackend) LookupVolume(_ context.Context, name string) (*VolumeInfo, error) {
	return &VolumeInfo{VolumeID: name}, nil
}

func (m *collisionMockBackend) CreateSnapshot(_ context.Context, volumeID string, snapID uint32) (*SnapshotInfo, error) {
	m.createCount++
	if m.createCount > 1 {
		return nil, fmt.Errorf("snapshot already exists")
	}
	return &SnapshotInfo{SnapshotID: snapID, VolumeID: volumeID, SizeBytes: 4 * 1024 * 1024}, nil
}

func (m *collisionMockBackend) DeleteSnapshot(_ context.Context, volumeID string, snapID uint32) error {
	return nil
}

func (m *collisionMockBackend) ListSnapshots(_ context.Context, volumeID string) ([]*SnapshotInfo, error) {
	return nil, nil
}

func (m *collisionMockBackend) ExpandVolume(_ context.Context, volumeID string, newSizeBytes uint64) (uint64, error) {
	return newSizeBytes, nil
}

// --- errorBackend ---

// errorBackend is a VolumeBackend that returns configurable errors for testing
// error propagation paths.
type errorBackend struct {
	listErr error
}

func (m *errorBackend) CreateVolume(_ context.Context, name string, sizeBytes uint64) (*VolumeInfo, error) {
	return &VolumeInfo{VolumeID: name, CapacityBytes: sizeBytes}, nil
}

func (m *errorBackend) DeleteVolume(_ context.Context, name string) error { return nil }

func (m *errorBackend) LookupVolume(_ context.Context, name string) (*VolumeInfo, error) {
	return &VolumeInfo{VolumeID: name}, nil
}

func (m *errorBackend) CreateSnapshot(_ context.Context, volumeID string, snapID uint32) (*SnapshotInfo, error) {
	return &SnapshotInfo{SnapshotID: snapID, VolumeID: volumeID}, nil
}

func (m *errorBackend) DeleteSnapshot(_ context.Context, volumeID string, snapID uint32) error {
	return nil
}

func (m *errorBackend) ListSnapshots(_ context.Context, volumeID string) ([]*SnapshotInfo, error) {
	return nil, m.listErr
}

func (m *errorBackend) ExpandVolume(_ context.Context, volumeID string, newSizeBytes uint64) (uint64, error) {
	return newSizeBytes, nil
}
