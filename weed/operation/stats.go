package operation

import (
	"context"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func Statistics(server string, grpcDialOption grpc.DialOption, req *master_pb.StatisticsRequest) (resp *master_pb.StatisticsResponse, err error) {

	err = WithMasterServerClient(server, grpcDialOption, func(ctx context.Context, masterClient master_pb.SeaweedClient) error {

		grpcResponse, grpcErr := masterClient.Statistics(ctx, req)
		if grpcErr != nil {
			return grpcErr
		}

		resp = grpcResponse

		return nil

	})

	return
}
