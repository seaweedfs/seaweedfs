package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (broker *MessageQueueBroker) BalanceTopics(ctx context.Context, request *mq_pb.BalanceTopicsRequest) (resp *mq_pb.BalanceTopicsResponse, err error) {
	if broker.currentBalancer == "" {
		return nil, status.Errorf(codes.Unavailable, "no balancer")
	}
	if !broker.lockAsBalancer.IsLocked() {
		proxyErr := broker.withBrokerClient(false, broker.currentBalancer, func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.BalanceTopics(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	ret := &mq_pb.BalanceTopicsResponse{}

	actions := broker.Balancer.BalancePublishers()
	err = broker.Balancer.ExecuteBalanceAction(actions, broker.grpcDialOption)

	return ret, err
}
