package operation

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
)

func WithVolumeServerClient(volumeServer string, grpcDialOption grpc.DialOption, fn func(volume_server_pb.VolumeServerClient) error) error {

	grpcAddress, err := toVolumeServerGrpcAddress(volumeServer)
	if err != nil {
		return fmt.Errorf("failed to parse volume server %v: %v", volumeServer, err)
	}

	return pb.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := volume_server_pb.NewVolumeServerClient(grpcConnection)
		return fn(client)
	}, grpcAddress, grpcDialOption)

}

func toVolumeServerGrpcAddress(volumeServer string) (grpcAddress string, err error) {
	sepIndex := strings.LastIndex(volumeServer, ":")
	port, err := strconv.Atoi(volumeServer[sepIndex+1:])
	if err != nil {
		glog.Errorf("failed to parse volume server address: %v", volumeServer)
		return "", err
	}
	return fmt.Sprintf("%s:%d", volumeServer[0:sepIndex], port+10000), nil
}

func WithMasterServerClient(masterServer string, grpcDialOption grpc.DialOption, fn func(masterClient master_pb.SeaweedClient) error) error {

	masterGrpcAddress, parseErr := pb.ParseServerToGrpcAddress(masterServer)
	if parseErr != nil {
		return fmt.Errorf("failed to parse master %v: %v", masterServer, parseErr)
	}

	return pb.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := master_pb.NewSeaweedClient(grpcConnection)
		return fn(client)
	}, masterGrpcAddress, grpcDialOption)

}

func WithFilerServerClient(filerServer string, grpcDialOption grpc.DialOption, fn func(masterClient filer_pb.SeaweedFilerClient) error) error {

	filerGrpcAddress, parseErr := pb.ParseServerToGrpcAddress(filerServer)
	if parseErr != nil {
		return fmt.Errorf("failed to parse filer %v: %v", filerGrpcAddress, parseErr)
	}

	return pb.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, filerGrpcAddress, grpcDialOption)

}
