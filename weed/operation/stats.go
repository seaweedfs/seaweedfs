package operation

import (
	"context"

	"google.golang.org/grpc"

	"github.com/HZ89/seaweedfs/weed/pb/master_pb"
)

func Statistics(server string, grpcDialOption grpc.DialOption, req *master_pb.StatisticsRequest) (resp *master_pb.StatisticsResponse, err error) {

	err = withMasterServerClient(server, grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

		grpcResponse, grpcErr := masterClient.Statistics(context.Background(), req)
		if grpcErr != nil {
			return grpcErr
		}

		resp = grpcResponse

		return nil

	})

	return
}
