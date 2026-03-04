package csi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// --- VolumeManager adversarial tests ---

// QA-VM-1: DeleteVolume leaks snapshot delta files (.snap.N).
// CreateVolume then call BlockVol's CreateSnapshot, then DeleteVolume.
// After delete, .snap.* files should not remain on disk.
func TestQA_VM_DeleteLeaksSnapshotFiles(t *testing.T) {
	mgr := newTestManager(t)

	if err := mgr.CreateVolume("snap-leak", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Access the managed volume to create a snapshot through the engine.
	mgr.mu.RLock()
	mv := mgr.volumes["snap-leak"]
	mgr.mu.RUnlock()
	if mv == nil {
		t.Fatal("volume not tracked")
	}

	// Create a snapshot to generate a .snap.0 delta file.
	if err := mv.vol.CreateSnapshot(0); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	// Write some data so the flusher creates CoW entries in the delta.
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAB
	}
	if err := mv.vol.WriteLBA(0, data); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Verify .snap.0 file exists on disk.
	snapPattern := filepath.Join(mgr.dataDir, "snap-leak.blk.snap.*")
	matches, _ := filepath.Glob(snapPattern)
	if len(matches) == 0 {
		t.Skipf("no snapshot delta files created (flusher may not have CoW'd yet)")
	}
	t.Logf("snapshot files before delete: %v", matches)

	// Delete the volume.
	if err := mgr.DeleteVolume("snap-leak"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// BUG: check for leaked snapshot files.
	matches, _ = filepath.Glob(snapPattern)
	if len(matches) > 0 {
		t.Errorf("BUG: snapshot delta files leaked after DeleteVolume: %v", matches)
	}

	// Also check the main .blk file is gone.
	volPath := filepath.Join(mgr.dataDir, "snap-leak.blk")
	if _, err := os.Stat(volPath); !os.IsNotExist(err) {
		t.Errorf("BUG: .blk file still exists after delete")
	}
}

// QA-VM-2: Start is retryable after failure.
// If initial Start fails (e.g., port in use), calling Start again after the
// port is freed should succeed.
func TestQA_VM_StartNotRetryableAfterFailure(t *testing.T) {
	dir := t.TempDir()
	logger := log.New(os.Stderr, "[test-qa] ", log.LstdFlags)

	// Start first manager to occupy the port.
	mgr1 := NewVolumeManager(dir, "127.0.0.1:19876", "iqn.test", logger)
	if err := mgr1.Start(context.Background()); err != nil {
		t.Fatalf("first start: %v", err)
	}
	defer mgr1.Stop()

	// Second manager on same port should fail.
	mgr2 := NewVolumeManager(dir, "127.0.0.1:19876", "iqn.test", logger)
	err := mgr2.Start(context.Background())
	if err == nil {
		mgr2.Stop()
		t.Fatal("expected second start to fail (port in use)")
	}
	t.Logf("first start failed as expected: %v", err)

	// Free the port, then retry — should succeed now.
	mgr1.Stop()

	err = mgr2.Start(context.Background())
	if err != nil {
		t.Fatalf("second start should succeed after port freed: %v", err)
	}
	defer mgr2.Stop()

	// Manager should be fully functional.
	if err := mgr2.CreateVolume("test", 4*1024*1024); err != nil {
		t.Fatalf("CreateVolume after retry: %v", err)
	}
}

// QA-VM-3: Stop then re-Start should work — manager should be fully functional.
func TestQA_VM_StopThenRestart(t *testing.T) {
	dir := t.TempDir()
	logger := log.New(os.Stderr, "[test-qa] ", log.LstdFlags)

	mgr := NewVolumeManager(dir, "127.0.0.1:0", "iqn.test", logger)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err := mgr.CreateVolume("v1", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}

	mgr.Stop()

	// Start after Stop should work.
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("restart after stop: %v", err)
	}
	defer mgr.Stop()

	// Manager should be fully functional — create a new volume.
	if err := mgr.CreateVolume("v2", 4*1024*1024); err != nil {
		t.Fatalf("create after restart: %v", err)
	}
	if !mgr.VolumeExists("v2") {
		t.Error("volume created but not tracked")
	}
}

// QA-VM-4: CreateVolume with 0 size should return clear error.
func TestQA_VM_CreateZeroSize(t *testing.T) {
	mgr := newTestManager(t)
	err := mgr.CreateVolume("zero", 0)
	if err == nil {
		t.Error("BUG: CreateVolume with 0 size should fail")
		mgr.DeleteVolume("zero")
	} else {
		t.Logf("correctly rejected 0-size: %v", err)
	}
}

// QA-VM-5: Concurrent CreateVolume + DeleteVolume for same name.
func TestQA_VM_ConcurrentCreateDeleteSameName(t *testing.T) {
	mgr := newTestManager(t)

	var wg sync.WaitGroup
	var panicked atomic.Bool

	// Run 20 goroutines, half creating and half deleting the same volume.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicked.Store(true)
					t.Errorf("PANIC in goroutine %d: %v", idx, r)
				}
			}()
			if idx%2 == 0 {
				mgr.CreateVolume("race-vol", 4*1024*1024)
			} else {
				mgr.DeleteVolume("race-vol")
			}
		}(i)
	}
	wg.Wait()

	if panicked.Load() {
		t.Fatal("BUG: concurrent create/delete caused panic")
	}
}

// QA-VM-6: Filename and IQN sanitization are consistent.
// Both sanitizeFilename and SanitizeIQN lowercase, so "VolA" and "vola"
// map to the same file and same IQN — treated as the same volume (idempotent).
func TestQA_VM_SanitizationDivergence(t *testing.T) {
	mgr := newTestManager(t)

	// Both sanitizers now lowercase, so "VolA" and "vola" produce:
	//   filename: "vola.blk" (both)
	//   IQN: ":vola" (both)
	// This means they are the same volume — second create is idempotent.

	err1 := mgr.CreateVolume("VolA", 4*1024*1024)
	if err1 != nil {
		t.Fatalf("create VolA: %v", err1)
	}

	// "vola" should be idempotent (same file, same IQN, same in-memory name "VolA").
	// But note: volume names are tracked as-is in the map ("VolA" != "vola"),
	// so the second create goes to file "vola.blk" which is the same file as
	// "VolA" -> "vola.blk". The existing-file adoption path handles this.
	err2 := mgr.CreateVolume("vola", 4*1024*1024)
	if err2 != nil {
		t.Fatalf("create vola (should be idempotent via file adoption): %v", err2)
	}

	iqn1 := mgr.VolumeIQN("VolA")
	iqn2 := mgr.VolumeIQN("vola")

	if iqn1 != iqn2 {
		t.Errorf("IQN mismatch: VolA=%q, vola=%q (should be identical after lowercasing)", iqn1, iqn2)
	}
}

// QA-VM-7: OpenVolume for a volume that's already open and tracked should be idempotent.
// But what if the file was modified externally between close and reopen?
func TestQA_VM_OpenAlreadyTracked(t *testing.T) {
	mgr := newTestManager(t)

	if err := mgr.CreateVolume("tracked", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Open again while already tracked — should be idempotent (no error).
	if err := mgr.OpenVolume("tracked"); err != nil {
		t.Fatalf("second open (expected idempotent): %v", err)
	}
}

// QA-VM-8: DeleteVolume for untracked volume — does it clean up .blk file from disk?
func TestQA_VM_DeleteUntrackedWithFileOnDisk(t *testing.T) {
	mgr := newTestManager(t)

	// Create then close (removes from tracking but keeps file on disk).
	if err := mgr.CreateVolume("orphan", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := mgr.CloseVolume("orphan"); err != nil {
		t.Fatalf("close: %v", err)
	}
	if mgr.VolumeExists("orphan") {
		t.Fatal("expected volume to be untracked after close")
	}

	// File should still exist on disk.
	volPath := filepath.Join(mgr.dataDir, "orphan.blk")
	if _, err := os.Stat(volPath); err != nil {
		t.Fatalf("expected .blk file to exist: %v", err)
	}

	// DeleteVolume for untracked name should still clean up file.
	if err := mgr.DeleteVolume("orphan"); err != nil {
		t.Fatalf("delete untracked: %v", err)
	}

	if _, err := os.Stat(volPath); !os.IsNotExist(err) {
		t.Errorf("BUG: .blk file not cleaned up by DeleteVolume for untracked volume")
	}
}

// --- Controller adversarial tests ---

// QA-CTRL-1: CreateVolume with LimitBytes smaller than RequiredBytes.
// CSI spec says limit_bytes is the maximum size. If set and smaller than required,
// it should be an error.
func TestQA_Ctrl_CreateLimitLessThanRequired(t *testing.T) {
	mgr := newTestManager(t)
	cs := &controllerServer{backend: NewLocalVolumeBackend(mgr)}

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "limit-vol",
		VolumeCapabilities: testVolCaps(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 10 * 1024 * 1024,
			LimitBytes:    1 * 1024 * 1024,
		},
	})
	if err == nil {
		mgr.DeleteVolume("limit-vol")
		t.Fatal("expected CreateVolume to reject LimitBytes < RequiredBytes")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got: %v", err)
	}
	t.Logf("correctly rejected: code=%v msg=%s", st.Code(), st.Message())
}

// QA-CTRL-2: CreateVolume with RequiredBytes=0 and LimitBytes set.
// Should use LimitBytes as the size.
func TestQA_Ctrl_CreateOnlyLimitBytes(t *testing.T) {
	mgr := newTestManager(t)
	cs := &controllerServer{backend: NewLocalVolumeBackend(mgr)}

	resp, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "limit-only",
		VolumeCapabilities: testVolCaps(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 0,
			LimitBytes:    2 * 1024 * 1024,
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	// Volume should be created but size should not exceed LimitBytes.
	if resp.Volume.CapacityBytes > 2*1024*1024 {
		t.Errorf("BUG: volume size %d exceeds LimitBytes %d",
			resp.Volume.CapacityBytes, 2*1024*1024)
	}
}

// QA-CTRL-3: CreateVolume with name containing path traversal.
func TestQA_Ctrl_CreatePathTraversal(t *testing.T) {
	mgr := newTestManager(t)
	cs := &controllerServer{backend: NewLocalVolumeBackend(mgr)}

	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "../../etc/shadow",
		VolumeCapabilities: testVolCaps(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 4 * 1024 * 1024,
		},
	})
	if err != nil {
		t.Logf("path traversal rejected: %v", err)
		return
	}

	// If it succeeded, verify the file was NOT created outside dataDir.
	if _, statErr := os.Stat("../../etc/shadow.blk"); statErr == nil {
		t.Fatal("BUG: path traversal created file outside data directory!")
	}

	// Check it went to a sanitized name inside dataDir.
	sanitized := filepath.Join(mgr.dataDir, "..-..-etc-shadow.blk")
	if _, statErr := os.Stat(sanitized); statErr == nil {
		t.Logf("file created with sanitized name: %s (safe)", sanitized)
	}

	mgr.DeleteVolume("../../etc/shadow")
}

// QA-CTRL-4: ValidateVolumeCapabilities after restart (not tracked in memory).
// By design, VolumeManager does not auto-discover volumes on startup.
// Volumes are re-tracked when kubelet re-calls CreateVolume or NodeStageVolume.
// ValidateVolumeCapabilities returns NotFound for orphaned volumes — expected.
func TestQA_Ctrl_ValidateAfterRestart(t *testing.T) {
	dir := t.TempDir()
	logger := log.New(os.Stderr, "[test-qa] ", log.LstdFlags)

	// Phase 1: create volume, stop.
	mgr1 := NewVolumeManager(dir, "127.0.0.1:0", "iqn.test", logger)
	if err := mgr1.Start(context.Background()); err != nil {
		t.Fatalf("start1: %v", err)
	}
	if err := mgr1.CreateVolume("validate-vol", 4*1024*1024); err != nil {
		t.Fatalf("create: %v", err)
	}
	mgr1.Stop()

	// Phase 2: new manager (simulates restart — no auto-discovery).
	mgr2 := NewVolumeManager(dir, "127.0.0.1:0", "iqn.test", logger)
	if err := mgr2.Start(context.Background()); err != nil {
		t.Fatalf("start2: %v", err)
	}
	defer mgr2.Stop()

	cs := &controllerServer{backend: NewLocalVolumeBackend(mgr2)}

	// ValidateVolumeCapabilities for volume that exists on disk but not in memory.
	// Expected: NotFound (by design — volumes are re-tracked via CreateVolume).
	_, err := cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: "validate-vol",
		VolumeCapabilities: []*csi.VolumeCapability{
			{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}},
		},
	})
	if err == nil {
		t.Fatal("expected NotFound for volume not yet re-tracked after restart")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got: %v", err)
	}
	t.Log("correctly returns NotFound for volume not yet re-tracked (by design)")

	// After CreateVolume re-adopts it, Validate should succeed.
	if err := mgr2.CreateVolume("validate-vol", 4*1024*1024); err != nil {
		t.Fatalf("re-adopt: %v", err)
	}
	_, err = cs.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: "validate-vol",
		VolumeCapabilities: []*csi.VolumeCapability{
			{AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}}},
		},
	})
	if err != nil {
		t.Fatalf("after re-adopt, expected success: %v", err)
	}
}

// QA-CTRL-5: CreateVolume with size that overflows uint64 after rounding.
func TestQA_Ctrl_CreateMaxSize(t *testing.T) {
	mgr := newTestManager(t)
	cs := &controllerServer{backend: NewLocalVolumeBackend(mgr)}

	// Request just under max int64 — rounding up to blockSize could overflow.
	_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "huge",
		VolumeCapabilities: testVolCaps(),
		CapacityRange: &csi.CapacityRange{
			RequiredBytes: 1<<63 - 1, // max int64
		},
	})
	if err == nil {
		t.Error("BUG: should reject unreasonably large size or fail gracefully")
		mgr.DeleteVolume("huge")
	} else {
		t.Logf("large size handled: %v", err)
	}
}

// --- Node adversarial tests ---

// QA-NODE-1: NodeStageVolume for a volume that doesn't exist.
func TestQA_Node_StageNonExistentVolume(t *testing.T) {
	ns, _, _ := newTestNodeServer(t)

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "does-not-exist",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolCap(),
	})
	if err == nil {
		t.Error("BUG: should fail for non-existent volume")
	} else {
		t.Logf("correctly rejected: %v", err)
	}
}

// QA-NODE-2: NodeUnstageVolume when all operations fail — should propagate first error.
func TestQA_Node_UnstageAllFail(t *testing.T) {
	ns, mi, mm := newTestNodeServer(t)

	mm.unmountErr = errors.New("unmount failed")
	mi.logoutErr = errors.New("logout failed")

	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: "/tmp/staging",
	})
	if err == nil {
		t.Error("BUG: should return error when unmount and logout both fail")
	} else {
		// Should report the first error (unmount).
		if !strings.Contains(err.Error(), "unmount") {
			t.Errorf("expected unmount error to be first, got: %v", err)
		}
	}
}

// QA-NODE-3: NodePublishVolume when staging path is not actually mounted.
// This should either fail or at least warn — bind-mounting an empty dir
// could silently give the pod an empty volume.
func TestQA_Node_PublishWithoutStaging(t *testing.T) {
	ns, _, _ := newTestNodeServer(t)

	stagingPath := t.TempDir()
	targetPath := t.TempDir()

	// Staging path is NOT mounted.
	// NodePublishVolume should either check or just mount (depends on behavior).
	_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
		TargetPath:        targetPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
			},
		},
	})
	// This likely succeeds (bind mounts empty dir) — which is wrong.
	if err == nil {
		t.Log("WARNING: NodePublishVolume succeeded when staging path was not mounted (bind-mounts empty dir)")
	}
}

// QA-NODE-4: NodeStageVolume idempotency doesn't verify correct volume.
// If something else is mounted at the staging path, Stage returns success
// without verifying it's our volume.
func TestQA_Node_StageWrongVolumeAtPath(t *testing.T) {
	ns, _, mm := newTestNodeServer(t)

	stagingPath := t.TempDir()

	// Pre-mark staging path as mounted (simulating another volume mounted there).
	mm.isMountedTargets[stagingPath] = true

	// NodeStageVolume for a different volume — should it succeed?
	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
	})
	if err == nil {
		t.Log("WARNING: NodeStageVolume returned success because staging path was already mounted, " +
			"but it could be a different volume (no verification of mount source)")
	}
}

// QA-NODE-5: Double NodeUnstageVolume — should be idempotent.
func TestQA_Node_DoubleUnstage(t *testing.T) {
	ns, _, _ := newTestNodeServer(t)

	stagingPath := t.TempDir()

	// First unstage — nothing to undo, but should succeed.
	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		t.Fatalf("first unstage: %v", err)
	}

	// Second unstage — should also succeed (idempotent).
	_, err = ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		t.Errorf("BUG: double unstage should be idempotent: %v", err)
	}
}

// QA-NODE-6: NodeGetInfo returns correct topology and max volumes.
func TestQA_Node_GetInfo(t *testing.T) {
	ns, _, _ := newTestNodeServer(t)

	resp, err := ns.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo: %v", err)
	}
	if resp.NodeId != "test-node-1" {
		t.Errorf("node_id: got %q, want %q", resp.NodeId, "test-node-1")
	}
	if resp.MaxVolumesPerNode <= 0 {
		t.Errorf("max_volumes: got %d, want > 0", resp.MaxVolumesPerNode)
	}
	if resp.AccessibleTopology == nil {
		t.Error("expected non-nil topology")
	}
}

// QA-NODE-7: NodeStageVolume with iSCSI discovery failure should clean up.
func TestQA_Node_StageDiscoveryFailureCleanup(t *testing.T) {
	ns, mi, _ := newTestNodeServer(t)

	mi.discoveryErr = errors.New("unreachable portal")

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
	})
	if err == nil {
		t.Fatal("expected error from discovery failure")
	}

	// Volume should be cleaned up.
	if ns.mgr.VolumeExists("test-vol") {
		t.Error("BUG: volume still tracked after discovery failure (resource leak)")
	}
}

// --- Server adversarial tests ---

// QA-SRV-1: parseEndpoint with unsupported scheme.
func TestQA_Srv_ParseEndpointBadScheme(t *testing.T) {
	_, _, err := parseEndpoint("http://localhost:50051")
	if err == nil {
		t.Error("BUG: should reject http:// scheme")
	}
}

// QA-SRV-2: parseEndpoint with various formats.
func TestQA_Srv_ParseEndpointFormats(t *testing.T) {
	tests := []struct {
		input     string
		wantProto string
		wantAddr  string
		wantErr   bool
	}{
		{"unix:///csi/csi.sock", "unix", "/csi/csi.sock", false},
		{"unix:///var/lib/kubelet/plugins/block.csi/csi.sock", "unix", "/var/lib/kubelet/plugins/block.csi/csi.sock", false},
		{"tcp://0.0.0.0:50051", "tcp", "0.0.0.0:50051", false},
		{"ftp://host/path", "", "", true},
		{"", "", "", true},
	}
	for _, tt := range tests {
		proto, addr, err := parseEndpoint(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseEndpoint(%q): expected error", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseEndpoint(%q): %v", tt.input, err)
			continue
		}
		if proto != tt.wantProto || addr != tt.wantAddr {
			t.Errorf("parseEndpoint(%q): got (%q, %q), want (%q, %q)",
				tt.input, proto, addr, tt.wantProto, tt.wantAddr)
		}
	}
}

// QA-SRV-3: NewCSIDriver with empty NodeID should fail.
func TestQA_Srv_DriverEmptyNodeID(t *testing.T) {
	_, err := NewCSIDriver(DriverConfig{
		Endpoint: "unix:///tmp/test.sock",
		DataDir:  t.TempDir(),
		NodeID:   "",
	})
	if err == nil {
		t.Error("BUG: should reject empty NodeID")
	}
}

// --- Identity adversarial tests ---

// QA-ID-1: Identity methods should work with nil requests.
func TestQA_Identity_NilRequests(t *testing.T) {
	s := &identityServer{}

	if _, err := s.GetPluginInfo(context.Background(), nil); err != nil {
		t.Errorf("GetPluginInfo(nil): %v", err)
	}
	if _, err := s.GetPluginCapabilities(context.Background(), nil); err != nil {
		t.Errorf("GetPluginCapabilities(nil): %v", err)
	}
	if _, err := s.Probe(context.Background(), nil); err != nil {
		t.Errorf("Probe(nil): %v", err)
	}
}

// --- SanitizeIQN adversarial tests ---

// QA-IQN-1: IQN with only invalid characters should not produce empty string.
func TestQA_IQN_AllInvalidChars(t *testing.T) {
	iqn := SanitizeIQN("!@#$%^&*()")
	if iqn == "" {
		t.Error("BUG: SanitizeIQN produced empty string for all-invalid input")
	}
	t.Logf("SanitizeIQN('!@#$%%^&*()') = %q", iqn)
}

// QA-IQN-2: Empty string input.
func TestQA_IQN_Empty(t *testing.T) {
	iqn := SanitizeIQN("")
	// Empty is technically valid but probably wrong — should the caller validate?
	t.Logf("SanitizeIQN('') = %q (len=%d)", iqn, len(iqn))
}

// QA-IQN-3: IQN at exactly 64 chars should NOT get hash suffix.
func TestQA_IQN_ExactlyMaxLength(t *testing.T) {
	name := strings.Repeat("a", 64)
	iqn := SanitizeIQN(name)
	if len(iqn) != 64 {
		t.Errorf("expected 64 chars, got %d: %q", len(iqn), iqn)
	}
	// Should not have hash suffix at exactly 64.
	if strings.Contains(iqn, "-") && len(name) == 64 {
		// This would mean it was unnecessarily truncated.
		t.Log("at-boundary: has dash but input was exactly 64 chars")
	}
}

// QA-IQN-4: IQN at 65 chars should get hash suffix.
func TestQA_IQN_OneOverMax(t *testing.T) {
	name := strings.Repeat("a", 65)
	iqn := SanitizeIQN(name)
	if len(iqn) > 64 {
		t.Errorf("expected max 64 chars, got %d", len(iqn))
	}
	// Verify hash suffix is present.
	parts := strings.Split(iqn, "-")
	if len(parts) < 2 {
		t.Errorf("expected hash suffix after truncation: %q", iqn)
	}
}

// QA-IQN-5: Two names that differ only by case should produce different IQNs
// (or we should document that case is folded).
func TestQA_IQN_CaseFolding(t *testing.T) {
	iqn1 := SanitizeIQN("MyVolume")
	iqn2 := SanitizeIQN("myvolume")
	if iqn1 != iqn2 {
		t.Errorf("case folding: %q != %q (different IQNs for same logical name)", iqn1, iqn2)
	}
	// This is expected — IQN lowercases. But the FILENAMES may differ.
	t.Logf("SanitizeIQN('MyVolume')=%q, SanitizeIQN('myvolume')=%q", iqn1, iqn2)
}

// --- Cross-cutting adversarial tests ---

// QA-X-1: Full lifecycle: create -> stage -> publish -> unpublish -> unstage -> delete.
// Run twice to verify second lifecycle works.
func TestQA_FullLifecycleTwice(t *testing.T) {
	dir := t.TempDir()
	logger := log.New(os.Stderr, "[test-qa] ", log.LstdFlags)
	mgr := NewVolumeManager(dir, "127.0.0.1:0", "iqn.2024.com.seaweedfs", logger)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer mgr.Stop()

	mi := newMockISCSIUtil()
	mi.getDeviceResult = "/dev/sda"
	mm := newMockMountUtil()

	cs := &controllerServer{backend: NewLocalVolumeBackend(mgr)}
	ns := &nodeServer{
		mgr:       mgr,
		nodeID:    "test-node",
		iscsiUtil: mi,
		mountUtil: mm,
		logger:    logger,
	}

	for round := 0; round < 2; round++ {
		volName := fmt.Sprintf("lifecycle-%d", round)
		t.Logf("--- round %d ---", round)

		// Create
		_, err := cs.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
			Name:               volName,
			VolumeCapabilities: testVolCaps(),
			CapacityRange:      &csi.CapacityRange{RequiredBytes: 4 * 1024 * 1024},
		})
		if err != nil {
			t.Fatalf("round %d create: %v", round, err)
		}

		// Close so stage can reopen.
		mgr.CloseVolume(volName)

		stagingPath := filepath.Join(t.TempDir(), "staging")
		targetPath := filepath.Join(t.TempDir(), "target")

		// Stage
		_, err = ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
			VolumeId:          volName,
			StagingTargetPath: stagingPath,
			VolumeCapability:  testVolCap(),
		})
		if err != nil {
			t.Fatalf("round %d stage: %v", round, err)
		}

		// Publish
		_, err = ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
			VolumeId:          volName,
			StagingTargetPath: stagingPath,
			TargetPath:        targetPath,
		})
		if err != nil {
			t.Fatalf("round %d publish: %v", round, err)
		}

		// Unpublish
		_, err = ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId:   volName,
			TargetPath: targetPath,
		})
		if err != nil {
			t.Fatalf("round %d unpublish: %v", round, err)
		}

		// Unstage
		_, err = ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
			VolumeId:          volName,
			StagingTargetPath: stagingPath,
		})
		if err != nil {
			t.Fatalf("round %d unstage: %v", round, err)
		}

		// Delete
		_, err = cs.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{
			VolumeId: volName,
		})
		if err != nil {
			t.Fatalf("round %d delete: %v", round, err)
		}

		// Verify file gone.
		volPath := filepath.Join(dir, sanitizeFilename(volName)+".blk")
		if _, statErr := os.Stat(volPath); !os.IsNotExist(statErr) {
			t.Errorf("round %d: .blk file not cleaned up: %v", round, statErr)
		}
	}
}
