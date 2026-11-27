package pb

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
)

// WithGrpcFilerClientAuth selects credentials for this logical client, not for
// the shared connection. source is called at each RPC invocation or stream open
// inside fn, including calls whose contexts fn constructs itself.
func WithGrpcFilerClientAuth(streamingMode bool, signature int32, filerAddress ServerAddress, grpcDialOption grpc.DialOption, source security.FilerTokenSource, fn func(filer_pb.SeaweedFilerClient) error) error {
	return WithGrpcClient(context.Background(), streamingMode, signature, func(conn *grpc.ClientConn) error {
		return fn(filer_pb.NewSeaweedFilerClient(security.WithFilerClientAuth(conn, source)))
	}, filerAddress.ToGrpcAddress(), false, grpcDialOption)
}
