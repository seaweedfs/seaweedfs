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

// WithOneOfGrpcFilerClientsAuth tries each filer in order, preserving the
// existing failover behavior while applying source to each logical client's
// calls. The shared pooled connections remain unchanged; source is resolved
// by the wrapped client for every RPC attempt.
func WithOneOfGrpcFilerClientsAuth(streamingMode bool, filerAddresses []ServerAddress, grpcDialOption grpc.DialOption, source security.FilerTokenSource, fn func(filer_pb.SeaweedFilerClient) error) (err error) {
	for _, filerAddress := range filerAddresses {
		err = WithGrpcFilerClientAuth(streamingMode, 0, filerAddress, grpcDialOption, source, fn)
		if err == nil {
			return nil
		}
	}
	return err
}
