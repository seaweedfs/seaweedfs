package csi

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	transportISCSI = "iscsi"
	transportNVMe  = "nvme"
)

// stagedVolumeInfo tracks info needed for NodeUnstageVolume and NodeExpandVolume.
type stagedVolumeInfo struct {
	iqn         string
	iscsiAddr   string
	nqn         string // NVMe subsystem NQN
	nvmeAddr    string // NVMe/TCP target address
	transport   string // "iscsi" or "nvme"
	isLocal     bool   // true if volume is served by local VolumeManager
	fsType      string // filesystem type (ext4, xfs, etc.)
	stagingPath string // staging mount path
}

type nodeServer struct {
	csi.UnimplementedNodeServer
	mgr       *VolumeManager // may be nil in controller-only mode
	nodeID    string
	iqnPrefix string // for IQN derivation fallback on restart
	nqnPrefix string // for NQN derivation fallback on restart
	iscsiUtil ISCSIUtil
	nvmeUtil  NVMeUtil // may be nil if NVMe not available
	mountUtil MountUtil
	logger    *log.Logger

	stagedMu sync.Mutex
	staged   map[string]*stagedVolumeInfo // volumeID -> staged info
}

// transportFile is the filename written inside the staging directory to persist
// the transport type across CSI plugin restarts.
const transportFile = ".transport"

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

	// Resolve iSCSI target info.
	// Priority: publish_context > volume_context > local volume manager fallback.
	var iqn, portal string
	isLocal := false

	if req.PublishContext != nil && req.PublishContext["iscsiAddr"] != "" && req.PublishContext["iqn"] != "" {
		portal = req.PublishContext["iscsiAddr"]
		iqn = req.PublishContext["iqn"]
	} else if req.VolumeContext != nil && req.VolumeContext["iscsiAddr"] != "" && req.VolumeContext["iqn"] != "" {
		portal = req.VolumeContext["iscsiAddr"]
		iqn = req.VolumeContext["iqn"]
	} else if s.mgr != nil {
		isLocal = true
		if err := s.mgr.OpenVolume(volumeID); err != nil {
			return nil, status.Errorf(codes.Internal, "open volume: %v", err)
		}
		iqn = s.mgr.VolumeIQN(volumeID)
		portal = s.mgr.ListenAddr()
	}

	// Resolve NVMe target info (same priority chain).
	// PublishContext > VolumeContext > local VolumeManager.
	var nqn, nvmeAddr string
	if req.PublishContext != nil && req.PublishContext["nvmeAddr"] != "" && req.PublishContext["nqn"] != "" {
		nvmeAddr = req.PublishContext["nvmeAddr"]
		nqn = req.PublishContext["nqn"]
	} else if req.VolumeContext != nil && req.VolumeContext["nvmeAddr"] != "" && req.VolumeContext["nqn"] != "" {
		nvmeAddr = req.VolumeContext["nvmeAddr"]
		nqn = req.VolumeContext["nqn"]
	} else if s.mgr != nil && s.mgr.NvmeAddr() != "" {
		nvmeAddr = s.mgr.NvmeAddr()
		nqn = s.mgr.VolumeNQN(volumeID)
	}

	// No transport info at all (neither iSCSI nor NVMe resolved, no local mgr).
	if iqn == "" && nqn == "" {
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

	// Transport selection: prefer NVMe if supported, fall back to iSCSI.
	var device, transport string

	nvmeAvailable := nvmeAddr != "" && nqn != "" && s.nvmeUtil != nil && s.nvmeUtil.IsNVMeTCPAvailable()
	if nvmeAvailable {
		// NVMe path — fail fast on error, no fallback to iSCSI.
		transport = transportNVMe

		connected, cerr := s.nvmeUtil.IsConnected(ctx, nqn)
		if cerr != nil {
			return nil, status.Errorf(codes.Internal, "check nvme connection: %v", cerr)
		}
		if !connected {
			if cerr := s.nvmeUtil.Connect(ctx, nqn, nvmeAddr); cerr != nil {
				return nil, status.Errorf(codes.Internal, "nvme connect: %v", cerr)
			}
		}

		// Cleanup NVMe on subsequent failures.
		defer func() {
			if !success {
				s.nvmeUtil.Disconnect(ctx, nqn)
			}
		}()

		device, err = s.nvmeUtil.GetDeviceByNQN(ctx, nqn)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "nvme get device: %v", err)
		}
	} else if iqn != "" && portal != "" {
		// iSCSI path (existing code).
		transport = transportISCSI

		loggedIn, lerr := s.iscsiUtil.IsLoggedIn(ctx, iqn)
		if lerr != nil {
			return nil, status.Errorf(codes.Internal, "check iscsi login: %v", lerr)
		}
		if !loggedIn {
			if err := s.iscsiUtil.Discovery(ctx, portal); err != nil {
				return nil, status.Errorf(codes.Internal, "iscsi discovery: %v", err)
			}
			if err := s.iscsiUtil.Login(ctx, iqn, portal); err != nil {
				return nil, status.Errorf(codes.Internal, "iscsi login: %v", err)
			}
		}

		device, err = s.iscsiUtil.GetDeviceByIQN(ctx, iqn)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get device: %v", err)
		}
	} else {
		return nil, status.Error(codes.FailedPrecondition, "no transport available")
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

	// Write transport marker for restart recovery.
	if werr := writeTransportFile(stagingPath, transport); werr != nil {
		s.logger.Printf("NodeStageVolume: %s: %v (non-fatal)", volumeID, werr)
	}

	// Track staged volume for unstage.
	s.stagedMu.Lock()
	if s.staged == nil {
		s.staged = make(map[string]*stagedVolumeInfo)
	}
	s.staged[volumeID] = &stagedVolumeInfo{
		iqn:         iqn,
		iscsiAddr:   portal,
		nqn:         nqn,
		nvmeAddr:    nvmeAddr,
		transport:   transport,
		isLocal:     isLocal,
		fsType:      fsType,
		stagingPath: stagingPath,
	}
	s.stagedMu.Unlock()

	success = true
	s.logger.Printf("NodeStageVolume: %s staged at %s (device=%s, transport=%s, local=%v)", volumeID, stagingPath, device, transport, isLocal)
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

	// Look up staged info.
	s.stagedMu.Lock()
	info := s.staged[volumeID]
	s.stagedMu.Unlock()

	// Determine transport and identifiers.
	var iqn, nqn, transport string
	isLocal := false

	if info != nil {
		iqn = info.iqn
		nqn = info.nqn
		transport = info.transport
		isLocal = info.isLocal
	} else {
		// Restart fallback: read .transport file from staging path.
		transport = readTransportFile(stagingPath)

		// Derive identifiers.
		if s.mgr != nil {
			iqn = s.mgr.VolumeIQN(volumeID)
			nqn = s.mgr.VolumeNQN(volumeID)
			isLocal = true
		} else {
			if s.iqnPrefix != "" {
				iqn = s.iqnPrefix + ":" + blockvol.SanitizeIQN(volumeID)
			}
			if s.nqnPrefix != "" {
				nqn = blockvol.BuildNQN(s.nqnPrefix, volumeID)
			}
		}

		// If no .transport file, probe NVMe connection to determine transport.
		if transport == "" && nqn != "" && s.nvmeUtil != nil {
			if connected, _ := s.nvmeUtil.IsConnected(ctx, nqn); connected {
				transport = transportNVMe
			}
		}
		if transport == "" {
			transport = transportISCSI // default fallback
		}

		s.logger.Printf("NodeUnstageVolume: %s not in staged map, derived transport=%s", volumeID, transport)
	}

	// Best-effort cleanup: always attempt all steps even if one fails.
	var firstErr error

	// Unmount.
	if err := s.mountUtil.Unmount(ctx, stagingPath); err != nil {
		s.logger.Printf("NodeUnstageVolume: unmount error: %v", err)
		firstErr = err
	}

	// Disconnect transport.
	switch transport {
	case transportNVMe:
		if nqn != "" && s.nvmeUtil != nil {
			if err := s.nvmeUtil.Disconnect(ctx, nqn); err != nil {
				s.logger.Printf("NodeUnstageVolume: nvme disconnect error: %v", err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	default: // iSCSI
		if iqn != "" {
			if err := s.iscsiUtil.Logout(ctx, iqn); err != nil {
				s.logger.Printf("NodeUnstageVolume: logout error: %v", err)
				if firstErr == nil {
					firstErr = err
				}
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
		// Keep staged entry so retry has correct info.
		return nil, status.Errorf(codes.Internal, "unstage: %v", firstErr)
	}

	// Clean up transport file.
	os.Remove(filepath.Join(stagingPath, transportFile))

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

	var device string
	var err error

	switch info.transport {
	case transportNVMe:
		// NVMe: rescan namespace, then find device.
		if s.nvmeUtil == nil {
			return nil, status.Errorf(codes.Internal, "nvme util not available")
		}
		if err := s.nvmeUtil.Rescan(ctx, info.nqn); err != nil {
			return nil, status.Errorf(codes.Internal, "nvme rescan: %v", err)
		}
		device, err = s.nvmeUtil.GetDeviceByNQN(ctx, info.nqn)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "nvme find device: %v", err)
		}
	default: // iSCSI
		// Check that iSCSI session is active.
		loggedIn, lerr := s.iscsiUtil.IsLoggedIn(ctx, info.iqn)
		if lerr != nil {
			return nil, status.Errorf(codes.Internal, "check iSCSI session: %v", lerr)
		}
		if !loggedIn {
			return nil, status.Errorf(codes.FailedPrecondition, "volume %q not staged: iSCSI session not active", req.VolumeId)
		}
		if err := s.iscsiUtil.RescanDevice(ctx, info.iqn); err != nil {
			return nil, status.Errorf(codes.Internal, "rescan iSCSI device: %v", err)
		}
		device, err = s.iscsiUtil.GetDeviceByIQN(ctx, info.iqn)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "find device: %v", err)
		}
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

	s.logger.Printf("NodeExpandVolume: %s expanded (device=%s, transport=%s, fs=%s)", req.VolumeId, device, info.transport, fsType)

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

// writeTransportFile writes the transport type to a file inside the staging path
// so it can be recovered after CSI plugin restart.
func writeTransportFile(stagingPath, transport string) error {
	path := filepath.Join(stagingPath, transportFile)
	if err := os.WriteFile(path, []byte(transport), 0600); err != nil {
		return fmt.Errorf("write transport file: %w", err)
	}
	return nil
}

// readTransportFile reads the transport type from the staging path.
// Returns empty string if the file doesn't exist.
func readTransportFile(stagingPath string) string {
	path := filepath.Join(stagingPath, transportFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	t := string(data)
	if t == transportISCSI || t == transportNVMe {
		return t
	}
	return ""
}
