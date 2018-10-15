package weed_server

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func (vs *VolumeServer) VolumeSyncStatus(ctx context.Context, req *volume_server_pb.VolumeSyncStatusRequest) (*volume_server_pb.VolumeSyncStatusResponse, error) {

	v := vs.store.GetVolume(storage.VolumeId(req.VolumdId))
	if v == nil {
		return nil, fmt.Errorf("Not Found Volume Id %d", req.VolumdId)
	}

	resp := v.GetVolumeSyncStatus()

	glog.V(2).Infof("volume sync status %d", req.VolumdId)

	return resp, nil

}
