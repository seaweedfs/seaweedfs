package wdclient

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func (mc *MasterClient) CollectionDelete(ctx context.Context, collection string) error {
	return withMasterClient(ctx, mc.currentMaster, mc.grpcDialOption, func(ctx context.Context, client master_pb.SeaweedClient) error {
		_, err := client.CollectionDelete(ctx, &master_pb.CollectionDeleteRequest{
			Name: collection,
		})
		return err
	})
}

func (mc *MasterClient) CollectionList(ctx context.Context) (resp *master_pb.CollectionListResponse, err error) {
	err = withMasterClient(ctx, mc.currentMaster, mc.grpcDialOption, func(ctx context.Context, client master_pb.SeaweedClient) error {
		resp, err = client.CollectionList(ctx, &master_pb.CollectionListRequest{})
		return err
	})
	return
}
