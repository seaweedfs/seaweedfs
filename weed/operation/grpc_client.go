package operation

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"strconv"
	"strings"
)

func WithVolumeServerClient(volumeServer string, grpcDialOption grpc.DialOption, fn func(volume_server_pb.VolumeServerClient) error) error {

	grpcAddress, err := toVolumeServerGrpcAddress(volumeServer)
	if err != nil {
		return err
	}

	return util.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
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

func withMasterServerClient(masterServer string, grpcDialOption grpc.DialOption, fn func(masterClient master_pb.SeaweedClient) error) error {

	masterGrpcAddress, parseErr := util.ParseServerToGrpcAddress(masterServer, 0)
	if parseErr != nil {
		return fmt.Errorf("failed to parse master grpc %v", masterServer)
	}

	return util.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := master_pb.NewSeaweedClient(grpcConnection)
		return fn(client)
	}, masterGrpcAddress, grpcDialOption)

}
