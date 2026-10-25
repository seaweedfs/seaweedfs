package operation

import (
	"context"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func WithVolumeServerClient(streamingMode bool, volumeServer pb.ServerAddress, grpcDialOption grpc.DialOption, fn func(volume_server_pb.VolumeServerClient) error) error {
	return WithVolumeServerClientOptions(streamingMode, volumeServer, fn, grpcDialOption)
}

// WithVolumeServerClientOptions is WithVolumeServerClient with extra dial
// options appended after the TLS option, so a caller dialing an untrusted
// source address can pin the validated endpoint at connect time.
func WithVolumeServerClientOptions(streamingMode bool, volumeServer pb.ServerAddress, fn func(volume_server_pb.VolumeServerClient) error, grpcDialOptions ...grpc.DialOption) error {

	return pb.WithGrpcClient(context.Background(), streamingMode, 0, func(grpcConnection *grpc.ClientConn) error {
		client := volume_server_pb.NewVolumeServerClient(grpcConnection)
		return fn(client)
	}, volumeServer.ToGrpcAddress(), false, grpcDialOptions...)

}

// WithMasterServerClient threads the caller's per-request context into the
// connection-invalidation decision, so a Canceled/DeadlineExceeded from the
// caller's own timeout does not invalidate the shared cached master connection.
// Pass context.Background() when there is no per-request deadline to honor.
func WithMasterServerClient(ctx context.Context, streamingMode bool, masterServer pb.ServerAddress, grpcDialOption grpc.DialOption, fn func(masterClient master_pb.SeaweedClient) error) error {
	return pb.WithMasterClient(ctx, streamingMode, masterServer, grpcDialOption, false, fn)
}
