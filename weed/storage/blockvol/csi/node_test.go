package csi

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestNodeServer(t *testing.T) (*nodeServer, *mockISCSIUtil, *mockMountUtil) {
	t.Helper()
	mgr := newTestManager(t)

	// Pre-create a volume for stage/unstage tests.
	if err := mgr.CreateVolume("test-vol", 4*1024*1024); err != nil {
		t.Fatalf("create test-vol: %v", err)
	}
	// Close it so OpenVolume in NodeStageVolume can reopen it.
	if err := mgr.CloseVolume("test-vol"); err != nil {
		t.Fatalf("close test-vol: %v", err)
	}

	mi := newMockISCSIUtil()
	mi.getDeviceResult = "/dev/sda"
	mm := newMockMountUtil()

	ns := &nodeServer{
		mgr:       mgr,
		nodeID:    "test-node-1",
		iqnPrefix: "iqn.2024.com.seaweedfs",
		iscsiUtil: mi,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[test-node] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}
	return ns, mi, mm
}

func TestNode_StageUnstage(t *testing.T) {
	ns, mi, mm := newTestNodeServer(t)

	stagingPath := t.TempDir()

	// Stage
	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"},
			},
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}

	// Verify calls.
	if len(mi.calls) < 3 {
		t.Fatalf("expected at least 3 iscsi calls, got %d: %v", len(mi.calls), mi.calls)
	}
	if mi.calls[0] != "discovery:"+ns.mgr.ListenAddr() {
		t.Fatalf("expected discovery call, got %q", mi.calls[0])
	}

	if len(mm.calls) < 1 {
		t.Fatalf("expected at least 1 mount call, got %d: %v", len(mm.calls), mm.calls)
	}

	// Verify staged map entry.
	ns.stagedMu.Lock()
	info, ok := ns.staged["test-vol"]
	ns.stagedMu.Unlock()
	if !ok {
		t.Fatal("expected test-vol in staged map")
	}
	if !info.isLocal {
		t.Fatal("expected isLocal=true for local volume")
	}

	// Unstage
	_, err = ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}

	// Verify staged map cleared.
	ns.stagedMu.Lock()
	_, ok = ns.staged["test-vol"]
	ns.stagedMu.Unlock()
	if ok {
		t.Fatal("expected test-vol removed from staged map")
	}
}

func TestNode_PublishUnpublish(t *testing.T) {
	ns, _, _ := newTestNodeServer(t)

	stagingPath := t.TempDir()
	targetPath := t.TempDir()

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
	if err != nil {
		t.Fatalf("NodePublishVolume: %v", err)
	}

	_, err = ns.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
		VolumeId:   "test-vol",
		TargetPath: targetPath,
	})
	if err != nil {
		t.Fatalf("NodeUnpublishVolume: %v", err)
	}
}

func TestNode_StageIdempotent(t *testing.T) {
	ns, _, mm := newTestNodeServer(t)

	stagingPath := t.TempDir()

	// Mark as already mounted -> stage should be idempotent.
	mm.isMountedTargets[stagingPath] = true

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
	})
	if err != nil {
		t.Fatalf("idempotent NodeStageVolume: %v", err)
	}

	// No iscsi or mount calls should have been made.
	if len(mm.calls) != 0 {
		t.Fatalf("expected 0 mount calls, got %d: %v", len(mm.calls), mm.calls)
	}
}

func TestNode_StageLoginFailure(t *testing.T) {
	ns, mi, _ := newTestNodeServer(t)

	mi.loginErr = errors.New("connection refused")

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
	})
	if err == nil {
		t.Fatal("expected error from login failure")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("expected Internal error, got: %v", err)
	}
}

func TestNode_StageMkfsFailure(t *testing.T) {
	ns, _, mm := newTestNodeServer(t)

	mm.formatAndMountErr = errors.New("mkfs failed")

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
	})
	if err == nil {
		t.Fatal("expected error from mkfs failure")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Internal {
		t.Fatalf("expected Internal error, got: %v", err)
	}
}

// TestNode_StageLoginFailureCleanup verifies that the volume is closed/disconnected
// when login fails during NodeStageVolume (Finding #3: resource leak).
func TestNode_StageLoginFailureCleanup(t *testing.T) {
	ns, mi, _ := newTestNodeServer(t)

	mi.loginErr = errors.New("connection refused")

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
	})
	if err == nil {
		t.Fatal("expected error from login failure")
	}

	// Volume should have been cleaned up (closed) after error.
	if ns.mgr.VolumeExists("test-vol") {
		t.Fatal("expected volume to be closed after login failure (resource leak)")
	}
}

// TestNode_PublishMissingStagingPath verifies that NodePublishVolume rejects
// empty StagingTargetPath (Finding #2).
func TestNode_PublishMissingStagingPath(t *testing.T) {
	ns, _, _ := newTestNodeServer(t)

	_, err := ns.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: "",
		TargetPath:        t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected error for empty StagingTargetPath")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument error, got: %v", err)
	}
}

// --- Remote target tests ---

// TestNode_StageRemoteTarget verifies NodeStageVolume reads iSCSI info from volume_context
// instead of using local VolumeManager.
func TestNode_StageRemoteTarget(t *testing.T) {
	mi := newMockISCSIUtil()
	mi.getDeviceResult = "/dev/sdb"
	mm := newMockMountUtil()

	ns := &nodeServer{
		mgr:       nil, // no local manager
		nodeID:    "test-node-1",
		iqnPrefix: "iqn.2024.com.seaweedfs",
		iscsiUtil: mi,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[test-node] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "remote-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		VolumeContext: map[string]string{
			"iscsiAddr": "10.0.0.5:3260",
			"iqn":       "iqn.2024.com.seaweedfs:remote-vol",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}

	// Verify discovery was called with remote portal.
	if len(mi.calls) < 1 || mi.calls[0] != "discovery:10.0.0.5:3260" {
		t.Fatalf("expected discovery with remote portal, got: %v", mi.calls)
	}

	// Verify staged map has remote info.
	ns.stagedMu.Lock()
	info, ok := ns.staged["remote-vol"]
	ns.stagedMu.Unlock()
	if !ok {
		t.Fatal("expected remote-vol in staged map")
	}
	if info.isLocal {
		t.Fatal("expected isLocal=false for remote volume")
	}
	if info.iqn != "iqn.2024.com.seaweedfs:remote-vol" {
		t.Fatalf("unexpected IQN: %s", info.iqn)
	}
}

// TestNode_UnstageRemoteTarget verifies unstage uses the staged map IQN.
func TestNode_UnstageRemoteTarget(t *testing.T) {
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		mgr:       nil,
		nodeID:    "test-node-1",
		iqnPrefix: "iqn.2024.com.seaweedfs",
		iscsiUtil: mi,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[test-node] ", log.LstdFlags),
		staged: map[string]*stagedVolumeInfo{
			"remote-vol": {
				iqn:       "iqn.2024.com.seaweedfs:remote-vol",
				iscsiAddr: "10.0.0.5:3260",
				isLocal:   false,
			},
		},
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "remote-vol",
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}

	// Verify logout was called with correct IQN.
	foundLogout := false
	for _, c := range mi.calls {
		if c == "logout:iqn.2024.com.seaweedfs:remote-vol" {
			foundLogout = true
		}
	}
	if !foundLogout {
		t.Fatalf("expected logout call with remote IQN, got: %v", mi.calls)
	}
}

// TestNode_UnstageAfterRestart verifies IQN derivation when staged map is empty.
func TestNode_UnstageAfterRestart(t *testing.T) {
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		mgr:       nil,
		nodeID:    "test-node-1",
		iqnPrefix: "iqn.2024.com.seaweedfs",
		iscsiUtil: mi,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[test-node] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo), // empty (simulates restart)
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "restart-vol",
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}

	// Verify logout was called with derived IQN.
	foundLogout := false
	for _, c := range mi.calls {
		if c == "logout:iqn.2024.com.seaweedfs:restart-vol" {
			foundLogout = true
		}
	}
	if !foundLogout {
		t.Fatalf("expected logout with derived IQN, got: %v", mi.calls)
	}
}

// TestNode_UnstageRetryKeepsStagedEntry verifies that if unmount fails,
// the staged entry is preserved for correct retry behavior.
func TestNode_UnstageRetryKeepsStagedEntry(t *testing.T) {
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()
	mm.unmountErr = errors.New("device busy")

	ns := &nodeServer{
		mgr:       nil,
		nodeID:    "test-node-1",
		iqnPrefix: "iqn.2024.com.seaweedfs",
		iscsiUtil: mi,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[test-node] ", log.LstdFlags),
		staged: map[string]*stagedVolumeInfo{
			"busy-vol": {
				iqn:       "iqn.2024.com.seaweedfs:busy-vol",
				iscsiAddr: "10.0.0.5:3260",
				isLocal:   false,
			},
		},
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "busy-vol",
		StagingTargetPath: stagingPath,
	})
	if err == nil {
		t.Fatal("expected error from unmount failure")
	}

	// Staged entry should still be present for retry.
	ns.stagedMu.Lock()
	info, ok := ns.staged["busy-vol"]
	ns.stagedMu.Unlock()
	if !ok {
		t.Fatal("staged entry should be preserved on failure for retry")
	}
	if info.iqn != "iqn.2024.com.seaweedfs:busy-vol" {
		t.Fatalf("unexpected IQN: %s", info.iqn)
	}
}

// TestNode_StageFallbackLocal verifies local fallback when no volume_context is provided.
func TestNode_StageFallbackLocal(t *testing.T) {
	ns, mi, _ := newTestNodeServer(t)

	stagingPath := t.TempDir()

	// Stage without volume_context — should use local VolumeManager.
	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "test-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}

	// Should have called discovery with local addr.
	if len(mi.calls) < 1 {
		t.Fatal("expected iSCSI calls")
	}
	if mi.calls[0] != "discovery:"+ns.mgr.ListenAddr() {
		t.Fatalf("expected local discovery, got %q", mi.calls[0])
	}

	// Verify staged as local.
	ns.stagedMu.Lock()
	info := ns.staged["test-vol"]
	ns.stagedMu.Unlock()
	if info == nil || !info.isLocal {
		t.Fatal("expected isLocal=true for local fallback")
	}
}
