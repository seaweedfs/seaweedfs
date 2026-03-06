package csi

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stagedVolumeInfo tracks info needed for NodeUnstageVolume and NodeExpandVolume.
type stagedVolumeInfo struct {
	iqn         string
	iscsiAddr   string
	isLocal     bool   // true if volume is served by local VolumeManager
	fsType      string // filesystem type (ext4, xfs, etc.)
	stagingPath string // staging mount path
}

type nodeServer struct {
	csi.UnimplementedNodeServer
	mgr       *VolumeManager // may be nil in controller-only mode
	nodeID    string
	iqnPrefix string // for IQN derivation fallback on restart
	iscsiUtil ISCSIUtil
	mountUtil MountUtil
	logger    *log.Logger

	stagedMu sync.Mutex
	staged   map[string]*stagedVolumeInfo // volumeID -> staged info
}

func (s *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	volumeID := req.VolumeId
	stagingPath := req.StagingTargetPath

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if req.VolumeCapability == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}

	// Idempotency: if already mounted at staging path, return OK.
	mounted, err := s.mountUtil.IsMounted(ctx, stagingPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check mount: %v", err)
	}
	if mounted {
		s.logger.Printf("NodeStageVolume: %s already mounted at %s", volumeID, stagingPath)
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// Determine iSCSI target info.
	// Priority: publish_context (fresh from ControllerPublish, reflects failover)
	//         > volume_context (from CreateVolume, may be stale after failover)
	//         > local volume manager fallback.
	var iqn, portal string
	isLocal := false

	if req.PublishContext != nil && req.PublishContext["iscsiAddr"] != "" && req.PublishContext["iqn"] != "" {
		// Fresh address from ControllerPublishVolume (reflects current primary).
		portal = req.PublishContext["iscsiAddr"]
		iqn = req.PublishContext["iqn"]
	} else if req.VolumeContext != nil && req.VolumeContext["iscsiAddr"] != "" && req.VolumeContext["iqn"] != "" {
		// Fallback: volume_context from CreateVolume (may be stale after failover).
		portal = req.VolumeContext["iscsiAddr"]
		iqn = req.VolumeContext["iqn"]
	} else if s.mgr != nil {
		// Local fallback: open volume via local VolumeManager.
		isLocal = true
		if err := s.mgr.OpenVolume(volumeID); err != nil {
			return nil, status.Errorf(codes.Internal, "open volume: %v", err)
		}
		iqn = s.mgr.VolumeIQN(volumeID)
		portal = s.mgr.ListenAddr()
	} else {
		return nil, status.Error(codes.FailedPrecondition, "no volume_context and no local volume manager")
	}

	// Cleanup on error.
	success := false
	defer func() {
		if !success {
			s.logger.Printf("NodeStageVolume: cleaning up %s after error", volumeID)
			if isLocal && s.mgr != nil {
				s.mgr.CloseVolume(volumeID)
			}
		}
	}()

	// Check if already logged in, skip login if so.
	loggedIn, err := s.iscsiUtil.IsLoggedIn(ctx, iqn)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check iscsi login: %v", err)
	}

	if !loggedIn {
		// Discovery + login.
		if err := s.iscsiUtil.Discovery(ctx, portal); err != nil {
			return nil, status.Errorf(codes.Internal, "iscsi discovery: %v", err)
		}
		if err := s.iscsiUtil.Login(ctx, iqn, portal); err != nil {
			return nil, status.Errorf(codes.Internal, "iscsi login: %v", err)
		}
	}

	// Wait for device to appear.
	device, err := s.iscsiUtil.GetDeviceByIQN(ctx, iqn)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get device: %v", err)
	}

	// Ensure staging directory exists.
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "create staging dir: %v", err)
	}

	// Format (if needed) and mount.
	fsType := "ext4"
	if req.VolumeCapability != nil {
		if mnt := req.VolumeCapability.GetMount(); mnt != nil && mnt.FsType != "" {
			fsType = mnt.FsType
		}
	}

	if err := s.mountUtil.FormatAndMount(ctx, device, stagingPath, fsType); err != nil {
		return nil, status.Errorf(codes.Internal, "format and mount: %v", err)
	}

	// Track staged volume for unstage.
	s.stagedMu.Lock()
	if s.staged == nil {
		s.staged = make(map[string]*stagedVolumeInfo)
	}
	s.staged[volumeID] = &stagedVolumeInfo{
		iqn:         iqn,
		iscsiAddr:   portal,
		isLocal:     isLocal,
		fsType:      fsType,
		stagingPath: stagingPath,
	}
	s.stagedMu.Unlock()

	success = true
	s.logger.Printf("NodeStageVolume: %s staged at %s (device=%s, iqn=%s, local=%v)", volumeID, stagingPath, device, iqn, isLocal)
	return &csi.NodeStageVolumeResponse{}, nil
}

func (s *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeID := req.VolumeId
	stagingPath := req.StagingTargetPath

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	// Look up staged info. If not found (e.g. driver restarted), derive IQN.
	s.stagedMu.Lock()
	info := s.staged[volumeID]
	s.stagedMu.Unlock()

	var iqn string
	isLocal := false
	if info != nil {
		iqn = info.iqn
		isLocal = info.isLocal
	} else {
		// Restart fallback: derive IQN from volumeID.
		// iscsiadm -m node -T <iqn> --logout works without knowing the portal.
		if s.mgr != nil {
			iqn = s.mgr.VolumeIQN(volumeID)
			isLocal = true
		} else if s.iqnPrefix != "" {
			iqn = s.iqnPrefix + ":" + blockvol.SanitizeIQN(volumeID)
		}
		s.logger.Printf("NodeUnstageVolume: %s not in staged map, derived iqn=%s", volumeID, iqn)
	}

	// Best-effort cleanup: always attempt all steps even if one fails.
	var firstErr error

	// Unmount.
	if err := s.mountUtil.Unmount(ctx, stagingPath); err != nil {
		s.logger.Printf("NodeUnstageVolume: unmount error: %v", err)
		firstErr = err
	}

	// iSCSI logout.
	if iqn != "" {
		if err := s.iscsiUtil.Logout(ctx, iqn); err != nil {
			s.logger.Printf("NodeUnstageVolume: logout error: %v", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Close the local volume if applicable.
	if isLocal && s.mgr != nil {
		if err := s.mgr.CloseVolume(volumeID); err != nil {
			s.logger.Printf("NodeUnstageVolume: close volume error: %v", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		// Keep staged entry so retry has correct isLocal/iqn info.
		return nil, status.Errorf(codes.Internal, "unstage: %v", firstErr)
	}

	// Remove from staged map only after successful cleanup.
	s.stagedMu.Lock()
	delete(s.staged, volumeID)
	s.stagedMu.Unlock()

	s.logger.Printf("NodeUnstageVolume: %s unstaged from %s", volumeID, stagingPath)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (s *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	volumeID := req.VolumeId
	targetPath := req.TargetPath
	stagingPath := req.StagingTargetPath

	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if targetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if stagingPath == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}

	// Idempotency: if already bind-mounted, return OK.
	mounted, err := s.mountUtil.IsMounted(ctx, targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check mount: %v", err)
	}
	if mounted {
		s.logger.Printf("NodePublishVolume: %s already mounted at %s", volumeID, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Ensure target directory exists.
	if err := os.MkdirAll(targetPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "create target dir: %v", err)
	}

	// Bind mount staging path to target path.
	readOnly := req.Readonly
	if err := s.mountUtil.BindMount(ctx, stagingPath, targetPath, readOnly); err != nil {
		return nil, status.Errorf(codes.Internal, "bind mount: %v", err)
	}

	s.logger.Printf("NodePublishVolume: %s published at %s", volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}

	// Idempotent: only unmount if still mounted.
	mounted, err := s.mountUtil.IsMounted(ctx, req.TargetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check mount: %v", err)
	}
	if mounted {
		if err := s.mountUtil.Unmount(ctx, req.TargetPath); err != nil {
			return nil, status.Errorf(codes.Internal, "unmount: %v", err)
		}
	}

	// CSI spec: remove mount point at target path.
	os.RemoveAll(req.TargetPath)

	s.logger.Printf("NodeUnpublishVolume: %s unpublished from %s", req.VolumeId, req.TargetPath)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (s *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// Look up staged volume info.
	s.stagedMu.Lock()
	info, ok := s.staged[req.VolumeId]
	s.stagedMu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %q not staged", req.VolumeId)
	}

	// Check that iSCSI session is active.
	loggedIn, err := s.iscsiUtil.IsLoggedIn(ctx, info.iqn)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check iSCSI session: %v", err)
	}
	if !loggedIn {
		return nil, status.Errorf(codes.FailedPrecondition, "volume %q not staged: iSCSI session not active", req.VolumeId)
	}

	// Rescan device to pick up new size.
	if err := s.iscsiUtil.RescanDevice(ctx, info.iqn); err != nil {
		return nil, status.Errorf(codes.Internal, "rescan iSCSI device: %v", err)
	}

	// Find the device path.
	device, err := s.iscsiUtil.GetDeviceByIQN(ctx, info.iqn)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "find device: %v", err)
	}

	// Determine mount path and fsType.
	mountPath := info.stagingPath
	if req.VolumePath != "" {
		mountPath = req.VolumePath
	}
	fsType := info.fsType
	if fsType == "" {
		fsType = "ext4"
	}

	// Resize the filesystem.
	if err := s.mountUtil.ResizeFS(ctx, device, mountPath, fsType); err != nil {
		return nil, status.Errorf(codes.Internal, "resize filesystem: %v", err)
	}

	s.logger.Printf("NodeExpandVolume: %s expanded (device=%s, fs=%s)", req.VolumeId, device, fsType)

	capacity := int64(0)
	if req.CapacityRange != nil {
		capacity = req.CapacityRange.RequiredBytes
	}
	return &csi.NodeExpandVolumeResponse{
		CapacityBytes: capacity,
	}, nil
}

func (s *nodeServer) NodeGetCapabilities(_ context.Context, _ *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	caps := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
	}

	var result []*csi.NodeServiceCapability
	for _, c := range caps {
		result = append(result, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{Type: c},
			},
		})
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: result}, nil
}

func (s *nodeServer) NodeGetInfo(_ context.Context, _ *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:            s.nodeID,
		MaxVolumesPerNode: 256,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{
				fmt.Sprintf("topology.%s/node", DriverName): s.nodeID,
			},
		},
	}, nil
}
