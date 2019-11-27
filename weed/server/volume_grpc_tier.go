package weed_server

import (
	"context"
	"fmt"
	"os"

	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

// VolumeTierCopyDatToRemote copy dat file to a remote tier
func (vs *VolumeServer) VolumeTierCopyDatToRemote(ctx context.Context, req *volume_server_pb.VolumeTierCopyDatToRemoteRequest) (*volume_server_pb.VolumeTierCopyDatToRemoteResponse, error) {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}

	if v.Collection != req.Collection {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	diskFile, ok := v.DataBackend.(*backend.DiskFile)
	if !ok {
		return nil, fmt.Errorf("volume %d is not on local disk", req.VolumeId)
	}
	err := uploadFileToRemote(ctx, req, diskFile.File)

	return &volume_server_pb.VolumeTierCopyDatToRemoteResponse{}, err
}

func uploadFileToRemote(ctx context.Context, req *volume_server_pb.VolumeTierCopyDatToRemoteRequest, f *os.File) error {
	println("copying dat file of", f.Name(), "to remote")

	return nil
}