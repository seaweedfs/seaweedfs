package filesys

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

var _ = filer_pb.FilerClient(&WFS{})

func (wfs *WFS) WithFilerClient(fn func(filer_pb.SeaweedFilerClient) error) (err error) {

	return util.Retry("filer grpc", func() error {

		i := wfs.option.filerIndex
		n := len(wfs.option.FilerGrpcAddresses)
		for x := 0; x < n; x++ {

			filerGrpcAddress := wfs.option.FilerGrpcAddresses[i]
			err = pb.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
				client := filer_pb.NewSeaweedFilerClient(grpcConnection)
				return fn(client)
			}, filerGrpcAddress, wfs.option.GrpcDialOption)

			if err != nil {
				glog.V(0).Infof("WithFilerClient %d %v: %v", x, filerGrpcAddress, err)
			} else {
				wfs.option.filerIndex = i
				return nil
			}

			i++
			if i >= n {
				i = 0
			}

		}
		return err
	})

}

func (wfs *WFS) AdjustedUrl(location *filer_pb.Location) string {
	if wfs.option.VolumeServerAccess == "publicUrl" {
		return location.PublicUrl
	}
	return location.Url
}
