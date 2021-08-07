package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

func (vs *VolumeServer) ReadNeedleBlob(ctx context.Context, req *volume_server_pb.ReadNeedleBlobRequest) (resp *volume_server_pb.ReadNeedleBlobResponse, err error) {
	resp = &volume_server_pb.ReadNeedleBlobResponse{}
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	resp.NeedleBlob, err = v.ReadNeedleBlob(req.Offset, types.Size(req.Size))
	if err != nil {
		return nil, fmt.Errorf("read needle blob offset %d size %d: %v", req.Offset, req.Size, err)
	}

	return resp, nil
}

func (vs *VolumeServer) WriteNeedleBlob(ctx context.Context, req *volume_server_pb.WriteNeedleBlobRequest) (resp *volume_server_pb.WriteNeedleBlobResponse, err error) {
	resp = &volume_server_pb.WriteNeedleBlobResponse{}
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	if err = v.WriteNeedleBlob(types.NeedleId(req.NeedleId), req.NeedleBlob, types.Size(req.Size)); err != nil {
		return nil, fmt.Errorf("write blob needle %d size %d: %v", req.NeedleId, req.Size, err)
	}

	return resp, nil
}
