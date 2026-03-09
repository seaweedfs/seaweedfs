package csi

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================
// QA-NVME-CP102: Adversarial tests for CSI NVMe transport path
// ============================================================

// --- QA-NVME-1: Transport file corruption ---

// readTransportFile should return "" for garbage content, preventing
// the node from selecting a wrong transport after restart.
func TestQA_NVMe_TransportFile_GarbageContent(t *testing.T) {
	mn := newMockNVMeUtil()
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		nqnPrefix: "nqn.2024-01.com.seaweedfs:vol.",
		iqnPrefix: "iqn.2024.com.seaweedfs",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo), // empty (restart)
	}

	stagingPath := t.TempDir()

	// Write garbage into .transport file.
	os.WriteFile(filepath.Join(stagingPath, transportFile), []byte("rdma-nonsense"), 0600)

	// Unstage should fall back to iSCSI (default) since "rdma-nonsense" is invalid.
	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "garbage-transport-vol",
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}

	// iSCSI logout should be called (default fallback).
	foundLogout := false
	for _, c := range mi.calls {
		if strings.HasPrefix(c, "logout:") {
			foundLogout = true
		}
	}
	if !foundLogout {
		t.Fatalf("expected iSCSI logout (default fallback for garbage transport), got: mi=%v mn=%v", mi.calls, mn.calls)
	}

	// NVMe disconnect should NOT be called.
	for _, c := range mn.calls {
		if strings.HasPrefix(c, "disconnect:") {
			t.Fatalf("NVMe disconnect should not be called for garbage transport file, got: %v", mn.calls)
		}
	}
}

// Empty .transport file should also default to iSCSI.
func TestQA_NVMe_TransportFile_Empty(t *testing.T) {
	stagingPath := t.TempDir()
	os.WriteFile(filepath.Join(stagingPath, transportFile), []byte(""), 0600)
	result := readTransportFile(stagingPath)
	if result != "" {
		t.Fatalf("expected empty string for empty .transport, got %q", result)
	}
}

// .transport file with extra whitespace should not match.
func TestQA_NVMe_TransportFile_Whitespace(t *testing.T) {
	stagingPath := t.TempDir()
	os.WriteFile(filepath.Join(stagingPath, transportFile), []byte("nvme\n"), 0600)
	result := readTransportFile(stagingPath)
	if result != "" {
		t.Fatalf("expected empty string for 'nvme\\n', got %q", result)
	}
}

// --- QA-NVME-2: IsConnected error during stage ---

// If IsConnected returns an error (not false), stage should fail — don't blindly connect.
func TestQA_NVMe_Stage_IsConnectedError(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	// Override IsConnected to return error.
	errCheck := errors.New("nvme list-subsys: permission denied")
	brokenMn := &isConnectedErrorNVMe{mockNVMeUtil: mn, err: errCheck}

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  brokenMn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "check-error-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext: map[string]string{
			"nvmeAddr": "10.0.0.5:4420",
			"nqn":      "nqn.2024.com.seaweedfs:check-error-vol",
		},
	})
	if err == nil {
		t.Fatal("expected error when IsConnected fails")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %v", st.Code())
	}
	if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("expected permission denied in error, got: %v", err)
	}

	// Should NOT have attempted Connect.
	for _, c := range mn.calls {
		if strings.HasPrefix(c, "connect:") {
			t.Fatalf("should not attempt connect after IsConnected error, got: %v", mn.calls)
		}
	}
}

// --- QA-NVME-3: Partial NVMe metadata ---

// NVMe addr present but NQN missing → should fall through to iSCSI.
func TestQA_NVMe_Stage_PartialMetadata_MissingNQN(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mi := newMockISCSIUtil()
	mi.getDeviceResult = "/dev/sdb"
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "partial-nvme-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext: map[string]string{
			"iscsiAddr": "10.0.0.5:3260",
			"iqn":       "iqn.2024.com.seaweedfs:partial-nvme-vol",
			"nvmeAddr":  "10.0.0.5:4420",
			// NQN intentionally missing
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}

	// Should have used iSCSI (NVMe metadata incomplete).
	if len(mi.calls) == 0 {
		t.Fatal("expected iSCSI calls")
	}
	if len(mn.calls) > 0 {
		t.Fatalf("expected no NVMe calls with missing NQN, got: %v", mn.calls)
	}
}

// NQN present but addr missing → should fall through to iSCSI.
func TestQA_NVMe_Stage_PartialMetadata_MissingAddr(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mi := newMockISCSIUtil()
	mi.getDeviceResult = "/dev/sdb"
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "partial-nvme-vol2",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext: map[string]string{
			"iscsiAddr": "10.0.0.5:3260",
			"iqn":       "iqn.2024.com.seaweedfs:partial-nvme-vol2",
			// nvmeAddr intentionally missing
			"nqn": "nqn.2024.com.seaweedfs:partial-nvme-vol2",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}

	// Should have used iSCSI.
	if len(mi.calls) == 0 {
		t.Fatal("expected iSCSI calls")
	}
	if len(mn.calls) > 0 {
		t.Fatalf("expected no NVMe calls with missing addr, got: %v", mn.calls)
	}
}

// --- QA-NVME-4: NVMe-only (no iSCSI metadata) ---

// Volume with ONLY NVMe metadata, no iSCSI. Connect failure should
// return error (no fallback possible).
func TestQA_NVMe_Stage_NVMeOnlyNoISCSI_ConnectFails(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mn.connectErr = errors.New("connection refused")
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "nvme-only-fail-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext: map[string]string{
			// NO iSCSI metadata at all
			"nvmeAddr": "10.0.0.5:4420",
			"nqn":      "nqn.2024.com.seaweedfs:nvme-only-fail-vol",
		},
	})
	if err == nil {
		t.Fatal("expected error when NVMe-only connect fails")
	}
	// Must not have tried iSCSI.
	if len(mi.calls) > 0 {
		t.Fatalf("should not attempt iSCSI when no iSCSI metadata, got: %v", mi.calls)
	}
}

// --- QA-NVME-5: No transport at all ---

// No iSCSI, no NVMe, no local mgr → FailedPrecondition.
func TestQA_NVMe_Stage_NoTransportAtAll(t *testing.T) {
	mn := newMockNVMeUtil()
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "no-transport-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext:     map[string]string{}, // empty
	})
	if err == nil {
		t.Fatal("expected error with no transport")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", st.Code())
	}
}

// --- QA-NVME-6: Cascading failures during unstage ---

// Both unmount AND disconnect fail → first error returned, volume stays staged.
func TestQA_NVMe_Unstage_CascadingFailures(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.disconnectErr = errors.New("disconnect failed")
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()
	mm.unmountErr = errors.New("device busy")

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged: map[string]*stagedVolumeInfo{
			"cascade-vol": {
				nqn:       "nqn.2024.com.seaweedfs:cascade-vol",
				nvmeAddr:  "10.0.0.5:4420",
				transport: transportNVMe,
			},
		},
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "cascade-vol",
		StagingTargetPath: stagingPath,
	})
	if err == nil {
		t.Fatal("expected error with cascading failures")
	}
	// First error should be the unmount error (it's tried first).
	if !strings.Contains(err.Error(), "device busy") {
		t.Fatalf("expected unmount error first, got: %v", err)
	}

	// Volume should remain staged for retry.
	ns.stagedMu.Lock()
	_, stillStaged := ns.staged["cascade-vol"]
	ns.stagedMu.Unlock()
	if !stillStaged {
		t.Fatal("volume should remain in staged map after cascading failure")
	}

	// Disconnect should still be attempted even after unmount failure.
	foundDisconnect := false
	for _, c := range mn.calls {
		if strings.HasPrefix(c, "disconnect:") {
			foundDisconnect = true
		}
	}
	if !foundDisconnect {
		t.Fatal("disconnect should still be attempted even after unmount failure")
	}
}

// --- QA-NVME-7: Expand with nvmeUtil nil but transport=NVMe ---

// This can happen if the CSI plugin restarts without NVMe support but
// a volume was previously staged via NVMe.
func TestQA_NVMe_Expand_NVMeUtilNil(t *testing.T) {
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  nil, // NVMe support gone after restart
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged: map[string]*stagedVolumeInfo{
			"orphan-nvme-vol": {
				nqn:         "nqn.2024.com.seaweedfs:orphan-nvme-vol",
				transport:   transportNVMe,
				fsType:      "ext4",
				stagingPath: "/staging/orphan",
			},
		},
	}

	_, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:      "orphan-nvme-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 16 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected error when nvmeUtil is nil for NVMe transport")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %v", st.Code())
	}
	if !strings.Contains(err.Error(), "nvme util not available") {
		t.Fatalf("expected 'nvme util not available', got: %v", err)
	}
}

// --- QA-NVME-8: Expand on unstaged volume ---

func TestQA_NVMe_Expand_NotStaged(t *testing.T) {
	mn := newMockNVMeUtil()
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	_, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:      "nonexistent-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 8 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected error expanding unstaged volume")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", st.Code())
	}
}

// --- QA-NVME-9: Concurrent stage NVMe + unstage ---

// Race between staging via NVMe and unstaging the same volume.
func TestQA_NVMe_ConcurrentStageUnstage(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mn.getDeviceResult = "/dev/nvme0n1"
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
			VolumeId:          "race-vol",
			StagingTargetPath: stagingPath,
			VolumeCapability:  testVolCap(),
			PublishContext: map[string]string{
				"nvmeAddr": "10.0.0.5:4420",
				"nqn":      "nqn.2024.com.seaweedfs:race-vol",
			},
		})
	}()

	go func() {
		defer wg.Done()
		ns.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
			VolumeId:          "race-vol",
			StagingTargetPath: stagingPath,
		})
	}()

	wg.Wait()
	// No panic is the main assertion. The staged map should be in a consistent state.
	ns.stagedMu.Lock()
	// We don't assert specific state because the race outcome is non-deterministic.
	ns.stagedMu.Unlock()
}

// --- QA-NVME-10: Rapid stage/unstage cycles ---

func TestQA_NVMe_RapidStageUnstageCycles(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mn.getDeviceResult = "/dev/nvme0n1"
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	ctx := context.Background()

	for i := 0; i < 20; i++ {
		stagingPath := t.TempDir()
		_, err := ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
			VolumeId:          "rapid-vol",
			StagingTargetPath: stagingPath,
			VolumeCapability:  testVolCap(),
			PublishContext: map[string]string{
				"nvmeAddr": "10.0.0.5:4420",
				"nqn":      "nqn.2024.com.seaweedfs:rapid-vol",
			},
		})
		if err != nil {
			t.Fatalf("stage cycle %d: %v", i, err)
		}
		_, err = ns.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{
			VolumeId:          "rapid-vol",
			StagingTargetPath: stagingPath,
		})
		if err != nil {
			t.Fatalf("unstage cycle %d: %v", i, err)
		}
	}

	// Verify staged map is clean after all cycles.
	ns.stagedMu.Lock()
	if _, ok := ns.staged["rapid-vol"]; ok {
		t.Fatal("rapid-vol should not be in staged map after all cycles complete")
	}
	ns.stagedMu.Unlock()
}

// --- QA-NVME-11: VolumeContext vs PublishContext precedence ---

// PublishContext should take precedence over VolumeContext for NVMe metadata.
func TestQA_NVMe_ContextPrecedence(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mn.getDeviceResult = "/dev/nvme0n1"
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "precedence-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext: map[string]string{
			"nvmeAddr": "10.0.0.1:4420",
			"nqn":      "nqn.publish.context:precedence-vol",
		},
		VolumeContext: map[string]string{
			"nvmeAddr": "10.0.0.2:4420",
			"nqn":      "nqn.volume.context:precedence-vol",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}

	// Verify PublishContext NQN was used (not VolumeContext).
	foundCorrectConnect := false
	for _, c := range mn.calls {
		if strings.Contains(c, "nqn.publish.context") && strings.Contains(c, "10.0.0.1:4420") {
			foundCorrectConnect = true
		}
		if strings.Contains(c, "nqn.volume.context") {
			t.Fatalf("VolumeContext should not be used when PublishContext has NVMe data, got: %v", mn.calls)
		}
	}
	if !foundCorrectConnect {
		t.Fatalf("expected connect with PublishContext NQN, got: %v", mn.calls)
	}
}

// VolumeContext should be used when PublishContext has no NVMe data.
func TestQA_NVMe_VolumeContextFallback(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mn.getDeviceResult = "/dev/nvme0n1"
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "volctx-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext: map[string]string{
			// No NVMe keys in PublishContext
			"iscsiAddr": "10.0.0.5:3260",
			"iqn":       "iqn.2024.com.seaweedfs:volctx-vol",
		},
		VolumeContext: map[string]string{
			"nvmeAddr": "10.0.0.3:4420",
			"nqn":      "nqn.volume.context:volctx-vol",
		},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume: %v", err)
	}

	// Verify VolumeContext NVMe was used.
	foundVolCtxConnect := false
	for _, c := range mn.calls {
		if strings.Contains(c, "nqn.volume.context") {
			foundVolCtxConnect = true
		}
	}
	if !foundVolCtxConnect {
		t.Fatalf("expected connect with VolumeContext NQN, got: %v", mn.calls)
	}
}

// --- QA-NVME-12: Restart probe with NVMe probe failure ---

// Unstage after restart: no .transport file, probe returns error → default to iSCSI.
func TestQA_NVMe_Unstage_ProbeFails_DefaultsISCSI(t *testing.T) {
	mn := &isConnectedErrorNVMe{
		mockNVMeUtil: newMockNVMeUtil(),
		err:          errors.New("probe failed"),
	}
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		nqnPrefix: "nqn.2024-01.com.seaweedfs:vol.",
		iqnPrefix: "iqn.2024.com.seaweedfs",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo), // empty (restart)
	}

	stagingPath := t.TempDir()
	// No .transport file.

	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "probe-fail-vol",
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		t.Fatalf("NodeUnstageVolume: %v", err)
	}

	// Should default to iSCSI since probe errored (not connected).
	foundLogout := false
	for _, c := range mi.calls {
		if strings.HasPrefix(c, "logout:") {
			foundLogout = true
		}
	}
	if !foundLogout {
		t.Fatalf("expected iSCSI logout (default after probe error), got: mi=%v", mi.calls)
	}
}

// --- QA-NVME-13: Stage with cancelled context ---

func TestQA_NVMe_Stage_CancelledContext(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	stagingPath := t.TempDir()

	// With mock, context cancellation won't propagate (mocks don't check ctx).
	// But this verifies no panic or data corruption with a cancelled context.
	ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          "cancel-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext: map[string]string{
			"nvmeAddr": "10.0.0.5:4420",
			"nqn":      "nqn.2024.com.seaweedfs:cancel-vol",
		},
	})
	// No panic is the assertion.
}

// --- QA-NVME-14: Unstage NVMe with nil nvmeUtil after restart ---

// After restart, nvmeUtil may be nil but .transport says NVMe.
// Unstage should handle gracefully (skip disconnect, don't crash).
func TestQA_NVMe_Unstage_NVMeUtilNil_TransportFileNVMe(t *testing.T) {
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		nqnPrefix: "nqn.2024-01.com.seaweedfs:vol.",
		iqnPrefix: "iqn.2024.com.seaweedfs",
		iscsiUtil: mi,
		nvmeUtil:  nil, // NVMe not available after restart
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()
	os.WriteFile(filepath.Join(stagingPath, transportFile), []byte("nvme"), 0600)

	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "nil-util-unstage-vol",
		StagingTargetPath: stagingPath,
	})
	// Should succeed — skip disconnect since nvmeUtil is nil, guard checks nqn != "" && nvmeUtil != nil.
	if err != nil {
		t.Fatalf("expected success (graceful skip), got: %v", err)
	}
}

// --- QA-NVME-15: Concurrent stage of different volumes ---

func TestQA_NVMe_ConcurrentDifferentVolumes(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mn.getDeviceResult = "/dev/nvme0n1"
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		volID := strings.Replace("par-vol-X", "X", string(rune('a'+i)), 1)
		stagingPath := t.TempDir()
		go func(vid, sp string) {
			defer wg.Done()
			ns.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
				VolumeId:          vid,
				StagingTargetPath: sp,
				VolumeCapability:  testVolCap(),
				PublishContext: map[string]string{
					"nvmeAddr": "10.0.0.5:4420",
					"nqn":      "nqn.2024.com.seaweedfs:" + vid,
				},
			})
		}(volID, stagingPath)
	}
	wg.Wait()

	// All 5 volumes should be staged. No panic.
	ns.stagedMu.Lock()
	count := len(ns.staged)
	ns.stagedMu.Unlock()
	if count != 5 {
		t.Fatalf("expected 5 staged volumes, got %d", count)
	}
}

// --- QA-NVME-16: Missing required fields ---

func TestQA_NVMe_Stage_MissingVolumeID(t *testing.T) {
	mn := newMockNVMeUtil()
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "",
		StagingTargetPath: "/some/path",
		VolumeCapability:  testVolCap(),
	})
	if err == nil {
		t.Fatal("expected error for missing volume ID")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestQA_NVMe_Stage_MissingStagingPath(t *testing.T) {
	mn := newMockNVMeUtil()
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "some-vol",
		StagingTargetPath: "",
		VolumeCapability:  testVolCap(),
	})
	if err == nil {
		t.Fatal("expected error for missing staging path")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestQA_NVMe_Stage_MissingCapability(t *testing.T) {
	mn := newMockNVMeUtil()
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "some-vol",
		StagingTargetPath: "/some/path",
		VolumeCapability:  nil,
	})
	if err == nil {
		t.Fatal("expected error for missing capability")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

// --- QA-NVME-17: Unstage idempotency (not in map, no .transport) ---

// Unstage a volume that was never staged and has no .transport file.
// Should succeed (idempotent).
func TestQA_NVMe_Unstage_NeverStaged(t *testing.T) {
	mn := newMockNVMeUtil()
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		nqnPrefix: "nqn.2024-01.com.seaweedfs:vol.",
		iqnPrefix: "iqn.2024.com.seaweedfs",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeUnstageVolume(context.Background(), &csi.NodeUnstageVolumeRequest{
		VolumeId:          "never-staged-vol",
		StagingTargetPath: stagingPath,
	})
	if err != nil {
		t.Fatalf("expected idempotent success, got: %v", err)
	}
}

// --- QA-NVME-18: Transport stickiness across iSCSI→NVMe upgrade ---

// Volume first staged via iSCSI, then re-staged (after controller re-publishes with NVMe).
// Transport should stay iSCSI until explicit unstage+re-stage.
func TestQA_NVMe_TransportSticky_ISCSIToNVMe(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mn.getDeviceResult = "/dev/nvme0n1"
	mi := newMockISCSIUtil()
	mi.getDeviceResult = "/dev/sdb"
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	// First stage: iSCSI only.
	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "upgrade-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext: map[string]string{
			"iscsiAddr": "10.0.0.5:3260",
			"iqn":       "iqn.2024.com.seaweedfs:upgrade-vol",
		},
	})
	if err != nil {
		t.Fatalf("first stage (iSCSI): %v", err)
	}

	ns.stagedMu.Lock()
	info := ns.staged["upgrade-vol"]
	ns.stagedMu.Unlock()
	if info.transport != transportISCSI {
		t.Fatalf("first stage: expected iscsi, got %q", info.transport)
	}

	// Re-stage attempt with NVMe metadata (already mounted → idempotent).
	mm.isMountedTargets[stagingPath] = true
	_, err = ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "upgrade-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext: map[string]string{
			"iscsiAddr": "10.0.0.5:3260",
			"iqn":       "iqn.2024.com.seaweedfs:upgrade-vol",
			"nvmeAddr":  "10.0.0.5:4420",
			"nqn":       "nqn.2024.com.seaweedfs:upgrade-vol",
		},
	})
	if err != nil {
		t.Fatalf("re-stage: %v", err)
	}

	// Transport must remain iSCSI — no implicit upgrade.
	ns.stagedMu.Lock()
	info = ns.staged["upgrade-vol"]
	ns.stagedMu.Unlock()
	if info.transport != transportISCSI {
		t.Fatalf("re-stage: expected iscsi (sticky), got %q", info.transport)
	}
}

// --- QA-NVME-19: GetDeviceByNQN returns empty device ---

func TestQA_NVMe_Stage_GetDeviceReturnsEmpty(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.nvmeTCPAvailable = true
	mn.getDeviceResult = "" // empty device path
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged:    make(map[string]*stagedVolumeInfo),
	}

	stagingPath := t.TempDir()

	_, err := ns.NodeStageVolume(context.Background(), &csi.NodeStageVolumeRequest{
		VolumeId:          "empty-dev-vol",
		StagingTargetPath: stagingPath,
		VolumeCapability:  testVolCap(),
		PublishContext: map[string]string{
			"nvmeAddr": "10.0.0.5:4420",
			"nqn":      "nqn.2024.com.seaweedfs:empty-dev-vol",
		},
	})
	// FormatAndMount will get "" as device — should fail at mount.
	// The important thing: no panic, and cleanup disconnect is called.
	if err == nil {
		// Some mock configs might silently pass with empty device, which is also fine
		// as long as no crash occurs.
		return
	}

	// Verify cleanup: disconnect called.
	foundDisconnect := false
	for _, c := range mn.calls {
		if strings.HasPrefix(c, "disconnect:") {
			foundDisconnect = true
		}
	}
	if !foundDisconnect {
		t.Fatalf("expected disconnect in cleanup after empty device, got: %v", mn.calls)
	}
}

// --- QA-NVME-20: Expand with GetDeviceByNQN failure ---

func TestQA_NVMe_Expand_GetDeviceFails(t *testing.T) {
	mn := newMockNVMeUtil()
	mn.getDeviceErr = errors.New("device vanished")
	mi := newMockISCSIUtil()
	mm := newMockMountUtil()

	ns := &nodeServer{
		nodeID:    "test-node-1",
		iscsiUtil: mi,
		nvmeUtil:  mn,
		mountUtil: mm,
		logger:    log.New(os.Stderr, "[qa-nvme] ", log.LstdFlags),
		staged: map[string]*stagedVolumeInfo{
			"dev-vanish-vol": {
				nqn:         "nqn.2024.com.seaweedfs:dev-vanish-vol",
				transport:   transportNVMe,
				fsType:      "ext4",
				stagingPath: "/staging/vanish",
			},
		},
	}

	_, err := ns.NodeExpandVolume(context.Background(), &csi.NodeExpandVolumeRequest{
		VolumeId:      "dev-vanish-vol",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 16 * 1024 * 1024},
	})
	if err == nil {
		t.Fatal("expected error when GetDeviceByNQN fails during expand")
	}
	if !strings.Contains(err.Error(), "device vanished") {
		t.Fatalf("expected 'device vanished' in error, got: %v", err)
	}
}

// --- Helper types ---

// isConnectedErrorNVMe wraps mockNVMeUtil but makes IsConnected return an error.
type isConnectedErrorNVMe struct {
	*mockNVMeUtil
	err error
}

func (m *isConnectedErrorNVMe) IsConnected(_ context.Context, _ string) (bool, error) {
	return false, m.err
}
