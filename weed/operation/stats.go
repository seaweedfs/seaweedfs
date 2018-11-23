package operation

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func Statistics(server string, req *master_pb.StatisticsRequest) (resp *master_pb.StatisticsResponse, err error) {

	err = withMasterServerClient(server, func(masterClient master_pb.SeaweedClient) error {
		grpcResponse, grpcErr := masterClient.Statistics(context.Background(), req)
		if grpcErr != nil {
			return grpcErr
		}

		resp = grpcResponse

		return nil

	})

	return
}
