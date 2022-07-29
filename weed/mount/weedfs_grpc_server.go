package mount

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_pb"
)

func (wfs *WFS) Configure(ctx context.Context, request *mount_pb.ConfigureRequest) (*mount_pb.ConfigureResponse, error) {
	if wfs.option.Collection == "" {
		return nil, fmt.Errorf("mount quota only works when mounted to a new folder with a collection")
	}
	glog.V(0).Infof("quota changed from %d to %d", wfs.option.Quota, request.CollectionCapacity)
	wfs.option.Quota = request.GetCollectionCapacity()
	return &mount_pb.ConfigureResponse{}, nil
}
