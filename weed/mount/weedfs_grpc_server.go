package mount

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/mount_pb"
)

func (wfs *WFS) Configure(ctx context.Context, request *mount_pb.ConfigureRequest) (*mount_pb.ConfigureResponse, error) {
	glog.V(0).Infof("quota changed from %d to %d", wfs.option.Quota, request.CollectionCapacity)
	wfs.option.Quota = request.GetCollectionCapacity()
	return &mount_pb.ConfigureResponse{}, nil
}
