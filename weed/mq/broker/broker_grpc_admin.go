package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (b *MessageQueueBroker) FindBrokerLeader(c context.Context, request *mq_pb.FindBrokerLeaderRequest) (*mq_pb.FindBrokerLeaderResponse, error) {
	ret := &mq_pb.FindBrokerLeaderResponse{}
	err := b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.FindLockOwner(context.Background(), &filer_pb.FindLockOwnerRequest{
			Name: pub_balancer.LockBrokerBalancer,
		})
		if err != nil {
			return err
		}
		ret.Broker = resp.Owner
		return nil
	})
	return ret, err
}
