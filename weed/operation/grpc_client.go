package operation

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

var (
	grpcClients     = make(map[string]*grpc.ClientConn)
	grpcClientsLock sync.Mutex
)

func WithVolumeServerClient(volumeServer string, fn func(volume_server_pb.VolumeServerClient) error) error {

	grpcAddress, err := toVolumeServerGrpcAddress(volumeServer)
	if err != nil {
		return err
	}

	grpcClientsLock.Lock()

	existingConnection, found := grpcClients[grpcAddress]
	if found {
		grpcClientsLock.Unlock()
		client := volume_server_pb.NewVolumeServerClient(existingConnection)
		return fn(client)
	}

	grpcConnection, err := util.GrpcDial(grpcAddress)
	if err != nil {
		grpcClientsLock.Unlock()
		return fmt.Errorf("fail to dial %s: %v", grpcAddress, err)
	}

	grpcClients[grpcAddress] = grpcConnection
	grpcClientsLock.Unlock()

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

	grpcClientsLock.Lock()

	existingConnection, found := grpcClients[masterServer]
	if found {
		grpcClientsLock.Unlock()
		client := master_pb.NewSeaweedClient(existingConnection)
		return fn(client)
	}

	grpcConnection, err := util.GrpcDial(masterServer)
	if err != nil {
		grpcClientsLock.Unlock()
		return fmt.Errorf("fail to dial %s: %v", masterServer, err)
	}

	grpcClients[masterServer] = grpcConnection
	grpcClientsLock.Unlock()

	client := master_pb.NewSeaweedClient(grpcConnection)

	return fn(client)
}
