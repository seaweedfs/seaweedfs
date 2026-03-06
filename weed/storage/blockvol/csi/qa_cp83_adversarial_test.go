package csi

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================================
// QA Adversarial Tests for CP8-3: CSI Snapshots + Expansion
// Covers concurrency, edge cases, error injection, and lifecycle races.
// ============================================================================

// --- A. Concurrent Snapshot Operations ---

// TestQA_CP83_ConcurrentCreateSameSnapshot: 10 goroutines create the same
// snapshot name on the same volume. Exactly one should create; the rest
// should get idempotent success (same snapID on same volume).
func TestQA_CP83_ConcurrentCreateSameSnapshot(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "conc-snap-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	const N = 10
	var wg sync.WaitGroup
	errs := make([]error, N)
	resps := make([]*csi.CreateSnapshotResponse, N)

	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			resp, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
				Name:           "same-snap-name",
				SourceVolumeId: "conc-snap-vol",
			})
			errs[idx] = err
			resps[idx] = resp
		}(i)
	}
	wg.Wait()

	// All should succeed (one creates, rest get idempotent).
	var snapID string
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: unexpected error: %v", i, errs[i])
			continue
		}
		if snapID == "" {
			snapID = resps[i].Snapshot.SnapshotId
		} else if resps[i].Snapshot.SnapshotId != snapID {
			t.Errorf("goroutine %d: snapshot ID mismatch: %s vs %s", i, resps[i].Snapshot.SnapshotId, snapID)
		}
	}
}

// TestQA_CP83_ConcurrentCreateDifferentSnapshots: 5 goroutines each create
// a uniquely named snapshot on the same volume. All should succeed.
func TestQA_CP83_ConcurrentCreateDifferentSnapshots(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "multi-snap-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	const N = 5
	var wg sync.WaitGroup
	errs := make([]error, N)

	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
				Name:           fmt.Sprintf("snap-%d", idx),
				SourceVolumeId: "multi-snap-vol",
			})
		}(i)
	}
	wg.Wait()

	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: unexpected error: %v", i, errs[i])
		}
	}

	// Verify all snapshots exist.
	listResp, err := cs.ListSnapshots(context.Background(), &csi.ListSnapshotsRequest{
		SourceVolumeId: "multi-snap-vol",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.Entries) != N {
		t.Fatalf("expected %d snapshots, got %d", N, len(listResp.Entries))
	}
}

// TestQA_CP83_ConcurrentSnapshotAndExpand: one goroutine creates a snapshot,
// another tries to expand. The expand should fail (FailedPrecondition) if
// the snapshot was created first, or the snapshot should succeed.
func TestQA_CP83_ConcurrentSnapshotAndExpand(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "race-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	var wg sync.WaitGroup
	var snapErr, expandErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		_, snapErr = cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
			Name:           "race-snap",
			SourceVolumeId: "race-vol",
		})
	}()
	go func() {
		defer wg.Done()
		_, expandErr = cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
			VolumeId:      "race-vol",
			CapacityRange: &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
		})
	}()
	wg.Wait()

	// One of two outcomes:
	// 1. Snapshot wins → expand gets FailedPrecondition
	// 2. Expand wins → snapshot still succeeds (taken after expand)
	if snapErr != nil {
		t.Fatalf("snapshot should always succeed, got: %v", snapErr)
	}
	if expandErr != nil {
		st, _ := status.FromError(expandErr)
		if st.Code() != codes.FailedPrecondition {
			t.Fatalf("expand error should be FailedPrecondition, got %v: %s", st.Code(), st.Message())
		}
	}
	// No panic = pass.
}

// --- B. Expand Edge Cases ---

// TestQA_CP83_ExpandToSameSize: expand to current size should succeed (no-op).
func TestQA_CP83_ExpandToSameSize(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "same-size-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	resp, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "same-size-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	// Engine may return success (no-op) or error (can't shrink to same).
	// Either way, no panic.
	if err != nil {
		st, _ := status.FromError(err)
		// InvalidArgument (shrink guard) is acceptable for same-size.
		if st.Code() != codes.InvalidArgument {
			t.Fatalf("unexpected error: %v", err)
		}
	} else if resp.CapacityBytes != 4*1024*1024 {
		t.Fatalf("expected 4MB, got %d", resp.CapacityBytes)
	}
}

// TestQA_CP83_ExpandToSmallerSize: shrink should be rejected.
func TestQA_CP83_ExpandToSmallerSize(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "shrink-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})

	_, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "shrink-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected error for shrink")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for shrink, got %v", st.Code())
	}
}

// TestQA_CP83_ExpandWithZeroBytes: expand with 0 bytes should be rejected.
func TestQA_CP83_ExpandWithZeroBytes(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "zero-expand-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	_, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "zero-expand-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 0},
	})
	if err == nil {
		t.Fatal("expected error for zero expand")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

// TestQA_CP83_ExpandNilCapacity: expand with nil CapacityRange should be rejected.
func TestQA_CP83_ExpandNilCapacity(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	_, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "nil-cap-vol",
		CapacityRange: nil,
	})
	if err == nil {
		t.Fatal("expected error for nil capacity range")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

// TestQA_CP83_ExpandNonAligned: expand to a non-block-aligned size should
// round up and succeed.
func TestQA_CP83_ExpandNonAligned(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "align-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	// Request non-aligned size (5MB + 1 byte).
	requested := int64(5*1024*1024 + 1)
	resp, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "align-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: requested},
	})
	if err != nil {
		t.Fatalf("non-aligned expand should succeed: %v", err)
	}
	// Result should be rounded up to next 4096 boundary.
	expected := int64((5*1024*1024+1)/4096+1) * 4096
	if resp.CapacityBytes != expected {
		t.Fatalf("expected rounded %d, got %d", expected, resp.CapacityBytes)
	}
}

// TestQA_CP83_ExpandMissingVolume: expand on nonexistent volume.
func TestQA_CP83_ExpandMissingVolume(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	_, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "no-such-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected error for missing volume")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v: %s", st.Code(), st.Message())
	}
}

// --- C. Snapshot Lifecycle ---

// TestQA_CP83_DoubleSnapshot: create two snapshots, delete first, verify
// second still exists, delete second, verify empty.
func TestQA_CP83_DoubleSnapshot(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}
	ctx := context.Background()

	cs.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "double-snap-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	snap1, err := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "snap-alpha", SourceVolumeId: "double-snap-vol",
	})
	if err != nil {
		t.Fatal(err)
	}
	snap2, err := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "snap-beta", SourceVolumeId: "double-snap-vol",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Delete first.
	cs.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snap1.Snapshot.SnapshotId})

	// List should show 1.
	list, _ := cs.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SourceVolumeId: "double-snap-vol"})
	if len(list.Entries) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(list.Entries))
	}
	if list.Entries[0].Snapshot.SnapshotId != snap2.Snapshot.SnapshotId {
		t.Fatalf("wrong snapshot survived")
	}

	// Delete second.
	cs.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snap2.Snapshot.SnapshotId})

	// List should be empty.
	list, _ = cs.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SourceVolumeId: "double-snap-vol"})
	if len(list.Entries) != 0 {
		t.Fatalf("expected 0 snapshots, got %d", len(list.Entries))
	}
}

// TestQA_CP83_SnapshotOnDeletedVolume: delete volume, then try snapshot.
func TestQA_CP83_SnapshotOnDeletedVolume(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}
	ctx := context.Background()

	cs.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "doomed-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	cs.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "doomed-vol"})

	_, err := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "orphan", SourceVolumeId: "doomed-vol",
	})
	if err == nil {
		t.Fatal("expected error for snapshot on deleted volume")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", st.Code())
	}
}

// TestQA_CP83_DeleteVolumeWithActiveSnapshots: volume with snapshots can be
// deleted (engine cleans up snap files).
func TestQA_CP83_DeleteVolumeWithActiveSnapshots(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}
	ctx := context.Background()

	cs.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "snap-then-delete-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "pre-delete-snap", SourceVolumeId: "snap-then-delete-vol",
	})

	_, err := cs.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "snap-then-delete-vol"})
	if err != nil {
		t.Fatalf("delete volume with snapshots should succeed: %v", err)
	}

	// Verify volume is gone.
	_, err = cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "after-delete-snap", SourceVolumeId: "snap-then-delete-vol",
	})
	if err == nil {
		t.Fatal("expected error for snapshot on deleted volume")
	}
}

// TestQA_CP83_FullCycleSnapExpandSnap: create → snap → delete snap → expand →
// snap at new size → verify snapshot reports new size.
func TestQA_CP83_FullCycleSnapExpandSnap(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}
	ctx := context.Background()

	cs.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "cycle-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	// Snapshot at 4MB.
	snap1, _ := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "pre-expand-snap", SourceVolumeId: "cycle-vol",
	})
	if snap1.Snapshot.SizeBytes != 4*1024*1024 {
		t.Fatalf("snap1 size: got %d, want %d", snap1.Snapshot.SizeBytes, 4*1024*1024)
	}

	// Delete snap so we can expand.
	cs.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: snap1.Snapshot.SnapshotId})

	// Expand to 8MB.
	_, err := cs.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      "cycle-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}

	// Snapshot at 8MB.
	snap2, _ := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "post-expand-snap", SourceVolumeId: "cycle-vol",
	})
	if snap2.Snapshot.SizeBytes != 8*1024*1024 {
		t.Fatalf("snap2 size: got %d, want %d", snap2.Snapshot.SizeBytes, 8*1024*1024)
	}
}

// --- D. Error Injection ---

// TestQA_CP83_ExpandBackendFailure: backend returns error on expand.
func TestQA_CP83_ExpandBackendFailure(t *testing.T) {
	mock := &expandFailBackend{expandErr: fmt.Errorf("disk full")}
	cs := &controllerServer{backend: mock}

	_, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId:      "fail-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected error from backend")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %v", st.Code())
	}
}

// TestQA_CP83_SnapshotBackendFailure: backend returns error on create snapshot.
func TestQA_CP83_SnapshotBackendFailure(t *testing.T) {
	mock := &expandFailBackend{snapErr: fmt.Errorf("I/O error")}
	cs := &controllerServer{backend: mock}

	_, err := cs.CreateSnapshot(context.Background(), &csi.CreateSnapshotRequest{
		Name:           "fail-snap",
		SourceVolumeId: "fail-vol",
	})
	if err == nil {
		t.Fatal("expected error from backend")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %v", st.Code())
	}
}

// --- E. ListSnapshots Edge Cases ---

// TestQA_CP83_ListSnapshotsBySnapshotID: filter by snapshot ID returns exactly 1.
func TestQA_CP83_ListSnapshotsBySnapshotID(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}
	ctx := context.Background()

	cs.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "list-by-id-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	snapResp, _ := cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "list-snap", SourceVolumeId: "list-by-id-vol",
	})

	list, err := cs.ListSnapshots(ctx, &csi.ListSnapshotsRequest{
		SnapshotId: snapResp.Snapshot.SnapshotId,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Entries) != 1 {
		t.Fatalf("expected 1, got %d", len(list.Entries))
	}
	if list.Entries[0].Snapshot.SnapshotId != snapResp.Snapshot.SnapshotId {
		t.Fatalf("wrong snapshot")
	}
}

// TestQA_CP83_ListSnapshotsBySnapshotID_NotFound: filter by non-existent
// snapshot ID returns empty.
func TestQA_CP83_ListSnapshotsBySnapshotID_NotFound(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}
	ctx := context.Background()

	cs.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "list-miss-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	list, err := cs.ListSnapshots(ctx, &csi.ListSnapshotsRequest{
		SnapshotId: FormatSnapshotID("list-miss-vol", 99999),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Entries) != 0 {
		t.Fatalf("expected 0, got %d", len(list.Entries))
	}
}

// TestQA_CP83_ListSnapshotsNoFilter: no filter returns empty (documented behavior).
func TestQA_CP83_ListSnapshotsNoFilter(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}
	ctx := context.Background()

	cs.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "no-filter-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})
	cs.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{
		Name: "no-filter-snap", SourceVolumeId: "no-filter-vol",
	})

	list, err := cs.ListSnapshots(ctx, &csi.ListSnapshotsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Entries) != 0 {
		t.Fatalf("expected 0 (no filter not supported), got %d", len(list.Entries))
	}
}

// --- F. Concurrent Expand ---

// TestQA_CP83_ConcurrentExpand: 5 goroutines expand the same volume to
// increasing sizes. No panics, final size should be the largest requested.
func TestQA_CP83_ConcurrentExpand(t *testing.T) {
	mgr := newTestManager(t)
	backend := NewLocalVolumeBackend(mgr)
	cs := &controllerServer{backend: backend}

	cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "conc-expand-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
	})

	const N = 5
	var wg sync.WaitGroup
	var maxCapacity atomic.Int64

	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			newSize := int64((idx + 2) * 4 * 1024 * 1024) // 8MB, 12MB, 16MB, 20MB, 24MB
			resp, err := cs.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
				VolumeId:      "conc-expand-vol",
				CapacityRange: &csi.CapacityRange{RequiredBytes: newSize},
			})
			if err != nil {
				// Some may fail with "shrink" if a larger expand completed first.
				return
			}
			for {
				old := maxCapacity.Load()
				if resp.CapacityBytes <= old {
					break
				}
				if maxCapacity.CompareAndSwap(old, resp.CapacityBytes) {
					break
				}
			}
		}(i)
	}
	wg.Wait()

	// Final capacity should be at least 24MB.
	if maxCapacity.Load() < 24*1024*1024 {
		t.Fatalf("expected max capacity >= 24MB, got %d", maxCapacity.Load())
	}
}

// --- G. Snapshot ID Helpers ---

// TestQA_CP83_FormatParseRoundTrip: verify format/parse round-trip for various names.
func TestQA_CP83_FormatParseRoundTrip(t *testing.T) {
	cases := []struct {
		vol    string
		snapID uint32
	}{
		{"simple", 42},
		{"pvc-12345", 99},
		{"vol-with-many-dashes", 1},
		{"vol", 0},           // ID 0
		{"vol", 0xFFFFFFFF},  // max uint32
	}
	for _, tc := range cases {
		formatted := FormatSnapshotID(tc.vol, tc.snapID)
		gotVol, gotSnap, err := ParseSnapshotID(formatted)
		if err != nil {
			t.Errorf("parse(%q): %v", formatted, err)
			continue
		}
		if gotVol != tc.vol || gotSnap != tc.snapID {
			t.Errorf("round-trip failed: vol=%q snap=%d → %q → vol=%q snap=%d",
				tc.vol, tc.snapID, formatted, gotVol, gotSnap)
		}
	}
}

// --- Mock backends for error injection ---

type expandFailBackend struct {
	expandErr error
	snapErr   error
}

func (m *expandFailBackend) CreateVolume(_ context.Context, name string, sizeBytes uint64) (*VolumeInfo, error) {
	return &VolumeInfo{VolumeID: name, CapacityBytes: sizeBytes}, nil
}
func (m *expandFailBackend) DeleteVolume(_ context.Context, _ string) error { return nil }
func (m *expandFailBackend) LookupVolume(_ context.Context, name string) (*VolumeInfo, error) {
	return &VolumeInfo{VolumeID: name}, nil
}
func (m *expandFailBackend) CreateSnapshot(_ context.Context, volumeID string, snapID uint32) (*SnapshotInfo, error) {
	if m.snapErr != nil {
		return nil, m.snapErr
	}
	return &SnapshotInfo{SnapshotID: snapID, VolumeID: volumeID}, nil
}
func (m *expandFailBackend) DeleteSnapshot(_ context.Context, _ string, _ uint32) error { return nil }
func (m *expandFailBackend) ListSnapshots(_ context.Context, _ string) ([]*SnapshotInfo, error) {
	return nil, nil
}
func (m *expandFailBackend) ExpandVolume(_ context.Context, _ string, _ uint64) (uint64, error) {
	return 0, m.expandErr
}
