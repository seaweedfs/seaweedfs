package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

// GetState returns a volume server's state flags.
func (vs *VolumeServer) GetState(ctx context.Context, req *volume_server_pb.GetStateRequest) (*volume_server_pb.GetStateResponse, error) {
	resp := &volume_server_pb.GetStateResponse{
		State:        vs.store.State.Pb,
		StateVersion: vs.store.State.Version,
	}

	return resp, nil
}

// SetState updates state flags for volume servers.
func (vs *VolumeServer) SetState(ctx context.Context, req *volume_server_pb.SetStateRequest) (*volume_server_pb.SetStateResponse, error) {
	err := vs.store.State.Update(req.GetState(), req.GetStateVersion())
	resp := &volume_server_pb.SetStateResponse{
		State:        vs.store.State.Pb,
		StateVersion: vs.store.State.Version,
	}

	return resp, err
}
