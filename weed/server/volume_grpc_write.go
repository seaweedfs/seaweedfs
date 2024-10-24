package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// VolumeTierMoveDatToRemote copy dat file to a remote tier
func (vs *VolumeServer) WriteBlob(ctx context.Context, req *volume_server_pb.WriteBlobRequest) (res *volume_server_pb.WriteBlobResponse, err error) {
	res = &volume_server_pb.WriteBlobResponse{}

	volumeId := needle.VolumeId(req.VolumeId)
	needleId, cookie, _ := needle.ParseNeedleIdCookie(req.FileId)

	n, contentMd5 := needle.CreateNeedleSimple(volumeId, needleId, cookie, req.Data)

	params := topology.ReplicatedWriteParams{
		VolumeId: volumeId,
		Needle: n,
		Jwt: "",
		Replicate: false,
		Fsync: false,
		ContentMd5: contentMd5,
	}
	_, writeError := topology.ReplicatedWrite(vs.GetMaster, vs.grpcDialOption, vs.store, params)
	if writeError != nil {
		err = writeError
		return nil, err
	}

	res.Size = uint32(n.Size)

	return
}
