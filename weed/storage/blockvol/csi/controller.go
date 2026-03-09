package csi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultVolumeSizeBytes = 1 << 30 // 1 GiB
	minVolumeSizeBytes     = 1 << 20 // 1 MiB
	blockSize              = 4096
)

type controllerServer struct {
	csi.UnimplementedControllerServer
	backend VolumeBackend
}

func (s *controllerServer) CreateVolume(_ context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}
	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	sizeBytes := int64(defaultVolumeSizeBytes)
	if req.CapacityRange != nil && req.CapacityRange.RequiredBytes > 0 {
		sizeBytes = req.CapacityRange.RequiredBytes
	} else if req.CapacityRange != nil && req.CapacityRange.LimitBytes > 0 {
		// No RequiredBytes set -- use LimitBytes as the target size.
		sizeBytes = req.CapacityRange.LimitBytes
	}
	if req.CapacityRange != nil && req.CapacityRange.LimitBytes > 0 {
		if req.CapacityRange.RequiredBytes > req.CapacityRange.LimitBytes {
			return nil, status.Errorf(codes.InvalidArgument,
				"required_bytes (%d) exceeds limit_bytes (%d)",
				req.CapacityRange.RequiredBytes, req.CapacityRange.LimitBytes)
		}
	}
	if sizeBytes < minVolumeSizeBytes {
		sizeBytes = minVolumeSizeBytes
	}
	// Round up to block size.
	if sizeBytes%blockSize != 0 {
		sizeBytes = (sizeBytes/blockSize + 1) * blockSize
	}
	// Verify rounded size still respects LimitBytes.
	if req.CapacityRange != nil && req.CapacityRange.LimitBytes > 0 {
		if sizeBytes > req.CapacityRange.LimitBytes {
			return nil, status.Errorf(codes.InvalidArgument,
				"volume size (%d) after rounding exceeds limit_bytes (%d)",
				sizeBytes, req.CapacityRange.LimitBytes)
		}
	}

	info, err := s.backend.CreateVolume(context.Background(), req.Name, uint64(sizeBytes))
	if err != nil {
		if errors.Is(err, ErrVolumeSizeMismatch) {
			return nil, status.Errorf(codes.AlreadyExists, "volume %q exists with different size", req.Name)
		}
		return nil, status.Errorf(codes.Internal, "create volume: %v", err)
	}

	resp := &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      info.VolumeID,
			CapacityBytes: int64(info.CapacityBytes),
		},
	}

	// Attach volume_context with target info for NodeStageVolume.
	hasISCSI := info.ISCSIAddr != "" || info.IQN != ""
	hasNVMe := info.NvmeAddr != ""
	if hasISCSI || hasNVMe {
		resp.Volume.VolumeContext = make(map[string]string)
		if hasISCSI {
			resp.Volume.VolumeContext["iscsiAddr"] = info.ISCSIAddr
			resp.Volume.VolumeContext["iqn"] = info.IQN
		}
		if hasNVMe {
			resp.Volume.VolumeContext["nvmeAddr"] = info.NvmeAddr
			resp.Volume.VolumeContext["nqn"] = info.NQN
		}
	}

	return resp, nil
}

func (s *controllerServer) DeleteVolume(_ context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// Idempotent: DeleteVolume succeeds even if volume doesn't exist.
	if err := s.backend.DeleteVolume(context.Background(), req.VolumeId); err != nil {
		return nil, status.Errorf(codes.Internal, "delete volume: %v", err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (s *controllerServer) ControllerPublishVolume(_ context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node ID is required")
	}

	info, err := s.backend.LookupVolume(context.Background(), req.VolumeId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q not found: %v", req.VolumeId, err)
	}

	pubCtx := map[string]string{
		"iscsiAddr": info.ISCSIAddr,
		"iqn":       info.IQN,
	}
	if info.NvmeAddr != "" {
		pubCtx["nvmeAddr"] = info.NvmeAddr
		pubCtx["nqn"] = info.NQN
	}
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: pubCtx,
	}, nil
}

func (s *controllerServer) ControllerUnpublishVolume(_ context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	// No-op: RWO enforced by iSCSI initiator single-login.
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (s *controllerServer) CreateSnapshot(_ context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if req.SourceVolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "source volume ID is required")
	}
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name is required")
	}

	snapID := snapshotNameToID(req.Name)
	info, err := s.backend.CreateSnapshot(context.Background(), req.SourceVolumeId, snapID)
	if err != nil {
		errMsg := err.Error()
		// CSI idempotency: if the snapshot already exists on the same volume,
		// return it as success. Only return AlreadyExists for true collisions
		// (different source volume using the same FNV hash).
		if strings.Contains(errMsg, "snapshot exists") || strings.Contains(errMsg, "already exists") {
			snaps, lerr := s.backend.ListSnapshots(context.Background(), req.SourceVolumeId)
			if lerr == nil {
				for _, snap := range snaps {
					if snap.SnapshotID == snapID {
						// Same name retried on same volume -- idempotent success.
						csiSnapID := FormatSnapshotID(req.SourceVolumeId, snapID)
						return &csi.CreateSnapshotResponse{
							Snapshot: &csi.Snapshot{
								SnapshotId:     csiSnapID,
								SourceVolumeId: req.SourceVolumeId,
								CreationTime:   timestamppb.New(time.Unix(snap.CreatedAt, 0)),
								ReadyToUse:     true,
								SizeBytes:      int64(snap.SizeBytes),
							},
						}, nil
					}
				}
			}
			// Snapshot exists but not on this volume -- true FNV collision.
			return nil, status.Errorf(codes.AlreadyExists,
				"snapshot ID collision: FNV hash %d already used by a different snapshot", snapID)
		}
		if strings.Contains(errMsg, "not found") {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", req.SourceVolumeId)
		}
		return nil, status.Errorf(codes.Internal, "create snapshot: %v", err)
	}

	csiSnapID := FormatSnapshotID(req.SourceVolumeId, snapID)
	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     csiSnapID,
			SourceVolumeId: req.SourceVolumeId,
			CreationTime:   timestamppb.New(time.Unix(info.CreatedAt, 0)),
			ReadyToUse:     true,
			SizeBytes:      int64(info.SizeBytes),
		},
	}, nil
}

func (s *controllerServer) DeleteSnapshot(_ context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if req.SnapshotId == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}

	volumeID, snapID, err := ParseSnapshotID(req.SnapshotId)
	if err != nil {
		// Can't parse → treat as already deleted (idempotent).
		return &csi.DeleteSnapshotResponse{}, nil
	}

	if berr := s.backend.DeleteSnapshot(context.Background(), volumeID, snapID); berr != nil {
		// Volume not found → snapshot doesn't exist → success (idempotent).
		if strings.Contains(berr.Error(), "not found") {
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "delete snapshot: %v", berr)
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func (s *controllerServer) ListSnapshots(_ context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	// If filtering by snapshot_id, parse it to get the volume.
	if req.SnapshotId != "" {
		volumeID, snapID, err := ParseSnapshotID(req.SnapshotId)
		if err != nil {
			return &csi.ListSnapshotsResponse{}, nil
		}

		snaps, lerr := s.backend.ListSnapshots(context.Background(), volumeID)
		if lerr != nil {
			if strings.Contains(lerr.Error(), "not found") {
				return &csi.ListSnapshotsResponse{}, nil
			}
			return nil, status.Errorf(codes.Internal, "list snapshots: %v", lerr)
		}

		for _, snap := range snaps {
			if snap.SnapshotID == snapID {
				return &csi.ListSnapshotsResponse{
					Entries: []*csi.ListSnapshotsResponse_Entry{{
						Snapshot: &csi.Snapshot{
							SnapshotId:     req.SnapshotId,
							SourceVolumeId: volumeID,
							SizeBytes:      int64(snap.SizeBytes),
							ReadyToUse:     true,
						},
					}},
				}, nil
			}
		}
		return &csi.ListSnapshotsResponse{}, nil
	}

	// If filtering by source_volume_id, list snapshots for that volume.
	if req.SourceVolumeId != "" {
		snaps, err := s.backend.ListSnapshots(context.Background(), req.SourceVolumeId)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return &csi.ListSnapshotsResponse{}, nil
			}
			return nil, status.Errorf(codes.Internal, "list snapshots: %v", err)
		}

		resp := &csi.ListSnapshotsResponse{}
		for _, snap := range snaps {
			resp.Entries = append(resp.Entries, &csi.ListSnapshotsResponse_Entry{
				Snapshot: &csi.Snapshot{
					SnapshotId:     FormatSnapshotID(req.SourceVolumeId, snap.SnapshotID),
					SourceVolumeId: req.SourceVolumeId,
					SizeBytes:      int64(snap.SizeBytes),
					ReadyToUse:     true,
				},
			})
		}
		return resp, nil
	}

	// No filter: not supported for distributed mode (would need to enumerate all volumes).
	return &csi.ListSnapshotsResponse{}, nil
}

func (s *controllerServer) ControllerExpandVolume(_ context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if req.CapacityRange == nil || req.CapacityRange.RequiredBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity_range.required_bytes must be > 0")
	}

	newSize := uint64(req.CapacityRange.RequiredBytes)
	// Round up to block size.
	if newSize%blockSize != 0 {
		newSize = (newSize/blockSize + 1) * blockSize
	}

	capacity, err := s.backend.ExpandVolume(context.Background(), req.VolumeId, newSize)
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "shrink") {
			return nil, status.Errorf(codes.InvalidArgument, "cannot shrink volume")
		}
		if strings.Contains(errMsg, "snapshots") {
			return nil, status.Errorf(codes.FailedPrecondition,
				"cannot resize with active snapshots: delete snapshots first")
		}
		if strings.Contains(errMsg, "not found") {
			return nil, status.Errorf(codes.NotFound, "volume %q not found", req.VolumeId)
		}
		return nil, status.Errorf(codes.Internal, "expand volume: %v", err)
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         int64(capacity),
		NodeExpansionRequired: true,
	}, nil
}

func (s *controllerServer) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}

	var result []*csi.ControllerServiceCapability
	for _, c := range caps {
		result = append(result, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{Type: c},
			},
		})
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: result}, nil
}

func (s *controllerServer) ValidateVolumeCapabilities(_ context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	if len(req.VolumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	if _, err := s.backend.LookupVolume(context.Background(), req.VolumeId); err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %q not found", req.VolumeId)
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.VolumeCapabilities,
		},
	}, nil
}

// errSnapshotsPreventResize is used by the mock backend for testing.
var errSnapshotsPreventResize = fmt.Errorf("blockvol: cannot resize with active snapshots")
