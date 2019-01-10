package operation

import (
	"context"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func Statistics(server string, req *master_pb.StatisticsRequest) (resp *master_pb.StatisticsResponse, err error) {

	err = withMasterServerClient(server, func(masterClient master_pb.SeaweedClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5*time.Second))
		defer cancel()

		grpcResponse, grpcErr := masterClient.Statistics(ctx, req)
		if grpcErr != nil {
			return grpcErr
		}

		resp = grpcResponse

		return nil

	})

	return
}
