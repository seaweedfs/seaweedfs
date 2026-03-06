package weed_server

import (
	"context"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// AllocateBlockVolume creates a new block volume on this volume server.
func (vs *VolumeServer) AllocateBlockVolume(_ context.Context, req *volume_server_pb.AllocateBlockVolumeRequest) (*volume_server_pb.AllocateBlockVolumeResponse, error) {
	if vs.blockService == nil {
		return nil, fmt.Errorf("block service not enabled on this volume server")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if req.SizeBytes == 0 {
		return nil, fmt.Errorf("size_bytes must be > 0")
	}

	path, iqn, iscsiAddr, err := vs.blockService.CreateBlockVol(req.Name, req.SizeBytes, req.DiskType, req.DurabilityMode)
	if err != nil {
		return nil, fmt.Errorf("create block volume %q: %w", req.Name, err)
	}

	// R1-1: Return deterministic replication ports so master can wire WAL shipping.
	dataPort, ctrlPort, rebuildPort := vs.blockService.ReplicationPorts(path)
	host := vs.blockService.ListenAddr()
	if idx := strings.LastIndex(host, ":"); idx >= 0 {
		host = host[:idx]
	}

	return &volume_server_pb.AllocateBlockVolumeResponse{
		Path:              path,
		Iqn:               iqn,
		IscsiAddr:         iscsiAddr,
		ReplicaDataAddr:   fmt.Sprintf("%s:%d", host, dataPort),
		ReplicaCtrlAddr:   fmt.Sprintf("%s:%d", host, ctrlPort),
		RebuildListenAddr: fmt.Sprintf("%s:%d", host, rebuildPort),
	}, nil
}

// VolumeServerDeleteBlockVolume deletes a block volume on this volume server.
func (vs *VolumeServer) VolumeServerDeleteBlockVolume(_ context.Context, req *volume_server_pb.VolumeServerDeleteBlockVolumeRequest) (*volume_server_pb.VolumeServerDeleteBlockVolumeResponse, error) {
	if vs.blockService == nil {
		return nil, fmt.Errorf("block service not enabled on this volume server")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	if err := vs.blockService.DeleteBlockVol(req.Name); err != nil {
		return nil, fmt.Errorf("delete block volume %q: %w", req.Name, err)
	}

	return &volume_server_pb.VolumeServerDeleteBlockVolumeResponse{}, nil
}

// SnapshotBlockVolume creates a snapshot on a block volume.
func (vs *VolumeServer) SnapshotBlockVolume(_ context.Context, req *volume_server_pb.SnapshotBlockVolumeRequest) (*volume_server_pb.SnapshotBlockVolumeResponse, error) {
	if vs.blockService == nil {
		return nil, fmt.Errorf("block service not enabled on this volume server")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	createdAt, sizeBytes, err := vs.blockService.SnapshotBlockVol(req.Name, req.SnapshotId)
	if err != nil {
		return nil, fmt.Errorf("snapshot block volume %q: %w", req.Name, err)
	}

	return &volume_server_pb.SnapshotBlockVolumeResponse{
		SnapshotId: req.SnapshotId,
		CreatedAt:  createdAt,
		SizeBytes:  sizeBytes,
	}, nil
}

// DeleteBlockSnapshot deletes a snapshot from a block volume.
func (vs *VolumeServer) DeleteBlockSnapshot(_ context.Context, req *volume_server_pb.DeleteBlockSnapshotRequest) (*volume_server_pb.DeleteBlockSnapshotResponse, error) {
	if vs.blockService == nil {
		return nil, fmt.Errorf("block service not enabled on this volume server")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	if err := vs.blockService.DeleteBlockSnapshot(req.Name, req.SnapshotId); err != nil {
		return nil, fmt.Errorf("delete block snapshot %q/%d: %w", req.Name, req.SnapshotId, err)
	}

	return &volume_server_pb.DeleteBlockSnapshotResponse{}, nil
}

// ListBlockSnapshots lists all snapshots on a block volume.
func (vs *VolumeServer) ListBlockSnapshots(_ context.Context, req *volume_server_pb.ListBlockSnapshotsRequest) (*volume_server_pb.ListBlockSnapshotsResponse, error) {
	if vs.blockService == nil {
		return nil, fmt.Errorf("block service not enabled on this volume server")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	infos, volSize, err := vs.blockService.ListBlockSnapshots(req.Name)
	if err != nil {
		return nil, fmt.Errorf("list block snapshots %q: %w", req.Name, err)
	}

	resp := &volume_server_pb.ListBlockSnapshotsResponse{}
	for _, s := range infos {
		resp.Snapshots = append(resp.Snapshots, &volume_server_pb.BlockSnapshotInfo{
			SnapshotId:      s.ID,
			CreatedAt:       s.CreatedAt.Unix(),
			VolumeSizeBytes: volSize,
		})
	}
	return resp, nil
}

// ExpandBlockVolume expands a block volume to a new size.
func (vs *VolumeServer) ExpandBlockVolume(_ context.Context, req *volume_server_pb.ExpandBlockVolumeRequest) (*volume_server_pb.ExpandBlockVolumeResponse, error) {
	if vs.blockService == nil {
		return nil, fmt.Errorf("block service not enabled on this volume server")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name is required")
	}
	if req.NewSizeBytes == 0 {
		return nil, fmt.Errorf("new_size_bytes must be > 0")
	}

	actualSize, err := vs.blockService.ExpandBlockVol(req.Name, req.NewSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("expand block volume %q: %w", req.Name, err)
	}

	return &volume_server_pb.ExpandBlockVolumeResponse{
		CapacityBytes: actualSize,
	}, nil
}
