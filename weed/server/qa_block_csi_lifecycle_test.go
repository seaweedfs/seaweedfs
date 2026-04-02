package weed_server

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	bsi "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ============================================================
// Phase 11 P2: CSI lifecycle rebinding with REAL master backend
//
// These tests wire the CSI controller through a VolumeBackend that
// calls REAL MasterServer.CreateBlockVolume / LookupBlockVolume /
// DeleteBlockVolume — the same master logic that MasterVolumeClient
// calls over gRPC. The node runs with mgr=nil so publish_context
// consumption is the only viable staging path.
//
// Chain:
//   CSI controller → masterServerBackend → MasterServer.CreateBlockVolume (real)
//   → blockRegistry (real) → blockVSAllocate (real blockvol creation)
//   → VolumeInfo with master-produced iscsiAddr/iqn
//   → ControllerPublish → publish_context
//   → CSI node (mgr=nil) → consumes publish_context → isLocal=false
// ============================================================

// masterServerBackend implements csi.VolumeBackend by calling real
// MasterServer methods. This is the SAME logic that MasterVolumeClient
// executes over gRPC, but called in-process for testability.
type masterServerBackend struct {
	ms *MasterServer
}

func (b *masterServerBackend) CreateVolume(ctx context.Context, name string, sizeBytes uint64) (*bsi.VolumeInfo, error) {
	resp, err := b.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      name,
		SizeBytes: sizeBytes,
	})
	if err != nil {
		return nil, err
	}
	return &bsi.VolumeInfo{
		VolumeID:      resp.VolumeId,
		ISCSIAddr:     resp.IscsiAddr,
		IQN:           resp.Iqn,
		CapacityBytes: resp.CapacityBytes,
		NvmeAddr:      resp.NvmeAddr,
		NQN:           resp.Nqn,
	}, nil
}

func (b *masterServerBackend) DeleteVolume(ctx context.Context, name string) error {
	_, err := b.ms.DeleteBlockVolume(ctx, &master_pb.DeleteBlockVolumeRequest{Name: name})
	return err
}

func (b *masterServerBackend) LookupVolume(ctx context.Context, name string) (*bsi.VolumeInfo, error) {
	resp, err := b.ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: name})
	if err != nil {
		return nil, err
	}
	return &bsi.VolumeInfo{
		VolumeID:      name,
		ISCSIAddr:     resp.IscsiAddr,
		IQN:           resp.Iqn,
		CapacityBytes: resp.CapacityBytes,
		NvmeAddr:      resp.NvmeAddr,
		NQN:           resp.Nqn,
	}, nil
}

func (b *masterServerBackend) CreateSnapshot(ctx context.Context, volumeID string, snapID uint32) (*bsi.SnapshotInfo, error) {
	return nil, fmt.Errorf("not in P2 scope")
}
func (b *masterServerBackend) DeleteSnapshot(ctx context.Context, volumeID string, snapID uint32) error {
	return fmt.Errorf("not in P2 scope")
}
func (b *masterServerBackend) ListSnapshots(ctx context.Context, volumeID string) ([]*bsi.SnapshotInfo, error) {
	return nil, fmt.Errorf("not in P2 scope")
}
func (b *masterServerBackend) ExpandVolume(ctx context.Context, volumeID string, newSizeBytes uint64) (uint64, error) {
	return 0, fmt.Errorf("not in P2 scope")
}

func newCSILifecycleSetup(t *testing.T) (*bsi.ExportedControllerServer, *bsi.ExportedNodeServer, *MasterServer, string) {
	t.Helper()
	dir := t.TempDir()

	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		sanitized := strings.ReplaceAll(server, ":", "_")
		serverDir := filepath.Join(dir, sanitized)
		os.MkdirAll(serverDir, 0755)
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
		// Derive a valid ip:port iSCSI address from the server identity.
		// Server identity is "vs1:9333" — extract the host part for the iSCSI portal.
		iscsiHost := server
		if idx := strings.LastIndex(server, ":"); idx >= 0 {
			iscsiHost = server[:idx]
		}
		return &blockAllocResult{
			Path:      volPath,
			IQN:       fmt.Sprintf("iqn.2024.master:%s", name),
			ISCSIAddr: iscsiHost + ":3260",
		}, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		// Real delete: remove the .blk file from the server's directory.
		sanitized := strings.ReplaceAll(server, ":", "_")
		volPath := filepath.Join(dir, sanitized, fmt.Sprintf("%s.blk", name))
		return os.Remove(volPath)
	}

	backend := &masterServerBackend{ms: ms}

	ctrl := bsi.NewExportedControllerServer(backend)
	node := bsi.NewExportedNodeServer("test-node-csi", nil, log.New(os.Stderr, "csi-node: ", log.LstdFlags))

	return ctrl, node, ms, dir
}

// --- 1. Master-produced publish_context ---

func TestP11P2_CSI_MasterProduced_PublishContext(t *testing.T) {
	ctrl, _, ms, _ := newCSILifecycleSetup(t)
	ctx := context.Background()

	createResp, err := ctrl.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name:               "csi-vol-1",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 20},
		VolumeCapabilities: []*csi.VolumeCapability{singleNodeWriter()},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	pubResp, err := ctrl.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: createResp.Volume.VolumeId, NodeId: "test-node-csi",
		VolumeCapability: singleNodeWriter(),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Verify: publish_context carries master-produced target info.
	entry, _ := ms.blockRegistry.Lookup("csi-vol-1")
	// Derive expected iSCSI portal: host from VolumeServer + :3260.
	vsHost := entry.VolumeServer
	if idx := strings.LastIndex(vsHost, ":"); idx >= 0 {
		vsHost = vsHost[:idx]
	}
	expectedISCSI := vsHost + ":3260"
	expectedIQN := fmt.Sprintf("iqn.2024.master:%s", "csi-vol-1")
	if pubResp.PublishContext["iscsiAddr"] != expectedISCSI {
		t.Fatalf("iscsiAddr=%q, want %q (master-produced)", pubResp.PublishContext["iscsiAddr"], expectedISCSI)
	}
	if pubResp.PublishContext["iqn"] != expectedIQN {
		t.Fatalf("iqn=%q, want %q (master-produced)", pubResp.PublishContext["iqn"], expectedIQN)
	}

	t.Logf("P11P2 master-produced: iscsiAddr=%s iqn=%s (both asserted)",
		pubResp.PublishContext["iscsiAddr"], pubResp.PublishContext["iqn"])
}

// --- 2. Full lifecycle: master backend + mgr=nil node ---

func TestP11P2_CSI_FullLifecycle(t *testing.T) {
	ctrl, node, _, testDir := newCSILifecycleSetup(t)
	ctx := context.Background()

	// Create.
	createResp, err := ctrl.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: "csi-vol-2", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 20},
		VolumeCapabilities: []*csi.VolumeCapability{singleNodeWriter()},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	volID := createResp.Volume.VolumeId

	// Publish.
	pubResp, err := ctrl.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: volID, NodeId: "test-node-csi", VolumeCapability: singleNodeWriter(),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Stage (mgr=nil → publish_context only path).
	stagingDir := filepath.Join(t.TempDir(), "staging")
	os.MkdirAll(stagingDir, 0755)
	_, err = node.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId: volID, StagingTargetPath: stagingDir,
		PublishContext: pubResp.PublishContext, VolumeCapability: singleNodeWriter(),
	})
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}

	// Assert: isLocal=false and staged addr matches publish_context.
	staged := node.GetStagedInfo(volID)
	if staged == nil {
		t.Fatal("not in staged map")
	}
	if staged.IsLocal {
		t.Fatal("isLocal must be false (mgr=nil)")
	}
	if staged.ISCSIAddr != pubResp.PublishContext["iscsiAddr"] {
		t.Fatalf("staged addr=%q != publish_context=%q", staged.ISCSIAddr, pubResp.PublishContext["iscsiAddr"])
	}

	// NodePublish.
	targetDir := filepath.Join(t.TempDir(), "target")
	os.MkdirAll(targetDir, 0755)
	_, err = node.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId: volID, StagingTargetPath: stagingDir, TargetPath: targetDir,
		VolumeCapability: singleNodeWriter(),
	})
	if err != nil {
		t.Fatalf("NodePublish: %v", err)
	}

	// Unpublish.
	if _, err := node.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: volID, TargetPath: targetDir}); err != nil {
		t.Fatalf("NodeUnpublish: %v", err)
	}
	// Unstage.
	if _, err := node.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: volID, StagingTargetPath: stagingDir}); err != nil {
		t.Fatalf("NodeUnstage: %v", err)
	}
	// Delete (real VS-side file removal via blockVSDelete).
	if _, err := ctrl.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: volID}); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	// Verify: .blk file is gone on disk (real file removal).
	volFilePath := filepath.Join(testDir, "vs1_9333", "csi-vol-2.blk")
	if _, err := os.Stat(volFilePath); !os.IsNotExist(err) {
		t.Fatalf(".blk file should be gone after delete: %s (err=%v)", volFilePath, err)
	}

	// Verify: publish fails (controller-level — volume unregistered).
	_, err = ctrl.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: volID, NodeId: "test-node-csi", VolumeCapability: singleNodeWriter(),
	})
	if err == nil {
		t.Fatal("publish after delete should fail")
	}

	t.Log("P11P2 lifecycle: master-created → publish → stage(isLocal=false) → pub → unpub → unstage → delete → gone")
}

// --- 3. Fail-closed: missing context + mgr=nil ---

func TestP11P2_CSI_FailClosed_NoContext(t *testing.T) {
	ctrl, node, _, _ := newCSILifecycleSetup(t)
	ctx := context.Background()

	createResp, err := ctrl.CreateVolume(ctx, &csi.CreateVolumeRequest{
		Name: "csi-vol-fc", CapacityRange: &csi.CapacityRange{RequiredBytes: 1 << 20},
		VolumeCapabilities: []*csi.VolumeCapability{singleNodeWriter()},
	})
	if err != nil {
		t.Fatalf("setup CreateVolume: %v", err)
	}

	stagingDir := filepath.Join(t.TempDir(), "staging")
	os.MkdirAll(stagingDir, 0755)

	_, stageErr := node.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId: createResp.Volume.VolumeId, StagingTargetPath: stagingDir,
		VolumeCapability: singleNodeWriter(),
	})
	if stageErr == nil {
		t.Fatal("stage without context + mgr=nil should fail")
	}
	st, ok := status.FromError(stageErr)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", stageErr)
	}
	t.Logf("P11P2 fail-closed: %s — %s", st.Code(), st.Message())
}

// --- 4. Fail-closed: missing volume ---

func TestP11P2_CSI_FailClosed_MissingVolume(t *testing.T) {
	ctrl, _, _, _ := newCSILifecycleSetup(t)
	ctx := context.Background()

	_, err := ctrl.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{
		VolumeId: "nonexistent", NodeId: "n", VolumeCapability: singleNodeWriter(),
	})
	if err == nil {
		t.Fatal("should fail")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %s", st.Code())
	}

	// Delete nonexistent: idempotent.
	_, delErr := ctrl.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "nonexistent"})
	if delErr != nil {
		t.Fatalf("delete nonexistent should be idempotent: %v", delErr)
	}

	t.Log("P11P2 fail-closed: NotFound on publish, idempotent on delete")
}

func singleNodeWriter() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}
