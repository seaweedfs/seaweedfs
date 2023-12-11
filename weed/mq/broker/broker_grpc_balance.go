package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (b *MessageQueueBroker) BalanceTopics(ctx context.Context, request *mq_pb.BalanceTopicsRequest) (resp *mq_pb.BalanceTopicsResponse, err error) {
	if b.currentBalancer == "" {
		return nil, status.Errorf(codes.Unavailable, "no balancer")
	}
	if !b.lockAsBalancer.IsLocked() {
		proxyErr := b.withBrokerClient(false, b.currentBalancer, func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.BalanceTopics(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	ret := &mq_pb.BalanceTopicsResponse{}

	actions := b.Balancer.BalancePublishers()
	err = b.Balancer.ExecuteBalanceAction(actions, b.grpcDialOption)

	return ret, err
}
