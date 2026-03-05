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

	path, iqn, iscsiAddr, err := vs.blockService.CreateBlockVol(req.Name, req.SizeBytes, req.DiskType)
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
