package filesys

import (
	"fmt"
	"strings"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

var _ = filer_pb.FilerClient(&WFS{})

func (wfs *WFS) WithFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {

	err := pb.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, wfs.option.FilerGrpcAddress, wfs.option.GrpcDialOption)

	if err == nil {
		return nil
	}
	return err

}

func (wfs *WFS) AdjustedUrl(hostAndPort string) string {
	if !wfs.option.OutsideContainerClusterMode {
		return hostAndPort
	}
	commaIndex := strings.Index(hostAndPort, ":")
	if commaIndex < 0 {
		return hostAndPort
	}
	filerCommaIndex := strings.Index(wfs.option.FilerGrpcAddress, ":")
	return fmt.Sprintf("%s:%s", wfs.option.FilerGrpcAddress[:filerCommaIndex], hostAndPort[commaIndex+1:])

}
