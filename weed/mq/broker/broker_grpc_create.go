package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateTopic Runs on any broker, but proxied to the balancer if not the balancer
func (broker *MessageQueueBroker) CreateTopic(ctx context.Context, request *mq_pb.CreateTopicRequest) (resp *mq_pb.CreateTopicResponse, err error) {
	if broker.currentBalancer == "" {
		return nil, status.Errorf(codes.Unavailable, "no balancer")
	}
	if !broker.lockAsBalancer.IsLocked() {
		proxyErr := broker.withBrokerClient(false, broker.currentBalancer, func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.CreateTopic(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	ret := &mq_pb.CreateTopicResponse{}
	ret.BrokerPartitionAssignments, err = broker.Balancer.LookupOrAllocateTopicPartitions(request.Topic, true, request.PartitionCount)

	for _, bpa := range ret.BrokerPartitionAssignments {
		if doCreateErr := broker.withBrokerClient(false, pb.ServerAddress(bpa.LeaderBroker), func(client mq_pb.SeaweedMessagingClient) error {
			_, doCreateErr := client.DoCreateTopic(ctx, &mq_pb.DoCreateTopicRequest{
				Topic:     request.Topic,
				Partition: bpa.Partition,
			})
			return doCreateErr
		}); doCreateErr != nil {
			return nil, doCreateErr
		}
	}

	return ret, err
}

func (broker *MessageQueueBroker) DoCreateTopic(ctx context.Context, req *mq_pb.DoCreateTopicRequest) (resp *mq_pb.DoCreateTopicResponse, err error) {
	ret := &mq_pb.DoCreateTopicResponse{}
	t, p := topic.FromPbTopic(req.Topic), topic.FromPbPartition(req.Partition)
	localTopicPartition := broker.localTopicManager.GetTopicPartition(t, p)
	if localTopicPartition == nil {
		localTopicPartition = topic.NewLocalPartition(t, p, true, nil)
		broker.localTopicManager.AddTopicPartition(t, localTopicPartition)
	}

	return ret, err
}
