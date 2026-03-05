package csi

import (
	"context"
	"errors"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		// No RequiredBytes set — use LimitBytes as the target size.
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

	// Attach volume_context with iSCSI target info for NodeStageVolume.
	if info.ISCSIAddr != "" || info.IQN != "" {
		resp.Volume.VolumeContext = map[string]string{
			"iscsiAddr": info.ISCSIAddr,
			"iqn":       info.IQN,
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

	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{
			"iscsiAddr": info.ISCSIAddr,
			"iqn":       info.IQN,
		},
	}, nil
}

func (s *controllerServer) ControllerUnpublishVolume(_ context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}
	// No-op: RWO enforced by iSCSI initiator single-login.
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (s *controllerServer) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
					},
				},
			},
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
					},
				},
			},
		},
	}, nil
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
