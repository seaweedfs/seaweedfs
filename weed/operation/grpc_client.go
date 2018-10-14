package operation

import (
	"fmt"
	"strings"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func withVolumeServerClient(volumeServer string, fn func(volume_server_pb.VolumeServerClient) error) error {

	grpcAddress, err := toVolumeServerGrpcAddress(volumeServer)
	if err != nil {
		return err
	}

	grpcConnection, err := util.GrpcDial(grpcAddress)
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", grpcAddress, err)
	}
	defer grpcConnection.Close()

	client := volume_server_pb.NewVolumeServerClient(grpcConnection)

	return fn(client)
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

func withMasterServerClient(masterServer string, fn func(masterClient master_pb.SeaweedClient) error) error {

	grpcConnection, err := util.GrpcDial(masterServer)
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", masterServer, err)
	}
	defer grpcConnection.Close()

	client := master_pb.NewSeaweedClient(grpcConnection)

	return fn(client)
}
