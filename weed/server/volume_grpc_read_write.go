package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
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

func (vs *VolumeServer) ReadNeedleMeta(ctx context.Context, req *volume_server_pb.ReadNeedleMetaRequest) (resp *volume_server_pb.ReadNeedleMetaResponse, err error) {
	resp = &volume_server_pb.ReadNeedleMetaResponse{}
	volumeId := needle.VolumeId(req.VolumeId)

	n := &needle.Needle{
		Id:    types.NeedleId(req.NeedleId),
		Flags: 0x08,
	}
	size := req.Size
	offset := req.Offset

	hasVolume := vs.store.HasVolume(volumeId)
	if !hasVolume {
		return nil, fmt.Errorf("not found volume id %d and read needle metadata at ec shards is not supported", req.VolumeId)
	}
	err = vs.store.ReadVolumeNeedleMetaAt(volumeId, n, offset, size)
	if err != nil {
		return nil, err
	}

	resp.Cookie = uint32(n.Cookie)
	resp.LastModified = n.LastModified
	resp.Crc = n.Checksum.Value()
	if n.HasTtl() {
		resp.Ttl = n.Ttl.String()
	}
	resp.AppendAtNs = n.AppendAtNs
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
