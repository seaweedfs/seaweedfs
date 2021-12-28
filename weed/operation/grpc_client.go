package operation

import (
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
)

func WithVolumeServerClient(streamingMode bool, volumeServer pb.ServerAddress, grpcDialOption grpc.DialOption, fn func(volume_server_pb.VolumeServerClient) error) error {

	return pb.WithGrpcClient(streamingMode, func(grpcConnection *grpc.ClientConn) error {
		client := volume_server_pb.NewVolumeServerClient(grpcConnection)
		return fn(client)
	}, volumeServer.ToGrpcAddress(), grpcDialOption)

}

func WithMasterServerClient(streamingMode bool, masterServer pb.ServerAddress, grpcDialOption grpc.DialOption, fn func(masterClient master_pb.SeaweedClient) error) error {

	return pb.WithGrpcClient(streamingMode, func(grpcConnection *grpc.ClientConn) error {
		client := master_pb.NewSeaweedClient(grpcConnection)
		return fn(client)
	}, masterServer.ToGrpcAddress(), grpcDialOption)

}
