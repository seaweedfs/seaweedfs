package filesys

import (
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

var _ = filer_pb.FilerClient(&WFS{})

func (wfs *WFS) WithFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {

	err := util.Retry("filer grpc "+wfs.option.FilerGrpcAddress, func() error {
		return pb.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
			client := filer_pb.NewSeaweedFilerClient(grpcConnection)
			return fn(client)
		}, wfs.option.FilerGrpcAddress, wfs.option.GrpcDialOption)
	})

	if err == nil {
		return nil
	}
	return err

}

func (wfs *WFS) AdjustedUrl(location *filer_pb.Location) string {
	if wfs.option.OutsideContainerClusterMode {
		return location.PublicUrl
	}
	return location.Url
}
