package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConfigureTopic Runs on any broker, but proxied to the balancer if not the balancer
// It generates an assignments based on existing allocations,
// and then assign the partitions to the brokers.
func (broker *MessageQueueBroker) ConfigureTopic(ctx context.Context, request *mq_pb.ConfigureTopicRequest) (resp *mq_pb.ConfigureTopicResponse, err error) {
	if broker.currentBalancer == "" {
		return nil, status.Errorf(codes.Unavailable, "no balancer")
	}
	if !broker.lockAsBalancer.IsLocked() {
		proxyErr := broker.withBrokerClient(false, broker.currentBalancer, func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.ConfigureTopic(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	ret := &mq_pb.ConfigureTopicResponse{}
	ret.BrokerPartitionAssignments, err = broker.Balancer.LookupOrAllocateTopicPartitions(request.Topic, true, request.PartitionCount)

	for _, bpa := range ret.BrokerPartitionAssignments {
		// fmt.Printf("create topic %s on %s\n", request.Topic, bpa.LeaderBroker)
		if doCreateErr := broker.withBrokerClient(false, pb.ServerAddress(bpa.LeaderBroker), func(client mq_pb.SeaweedMessagingClient) error {
			_, doCreateErr := client.AssignTopicPartitions(ctx, &mq_pb.AssignTopicPartitionsRequest{
				Topic: request.Topic,
				BrokerPartitionAssignments: []*mq_pb.BrokerPartitionAssignment{
					{
						Partition: bpa.Partition,
					},
				},
				IsLeader:   true,
				IsDraining: false,
			})
			if doCreateErr != nil {
				return fmt.Errorf("do create topic %s on %s: %v", request.Topic, bpa.LeaderBroker, doCreateErr)
			}
			brokerStats, found := broker.Balancer.Brokers.Get(bpa.LeaderBroker)
			if !found {
				brokerStats = balancer.NewBrokerStats()
				if !broker.Balancer.Brokers.SetIfAbsent(bpa.LeaderBroker, brokerStats) {
					brokerStats, _ = broker.Balancer.Brokers.Get(bpa.LeaderBroker)
				}
			}
			brokerStats.RegisterAssignment(request.Topic, bpa.Partition)
			return nil
		}); doCreateErr != nil {
			return nil, doCreateErr
		}
	}

	// TODO revert if some error happens in the middle of the assignments

	return ret, err
}

// AssignTopicPartitions Runs on the assigned broker, to execute the topic partition assignment
func (broker *MessageQueueBroker) AssignTopicPartitions(c context.Context, request *mq_pb.AssignTopicPartitionsRequest) (*mq_pb.AssignTopicPartitionsResponse, error) {
	ret := &mq_pb.AssignTopicPartitionsResponse{}
	self := pb.ServerAddress(fmt.Sprintf("%s:%d", broker.option.Ip, broker.option.Port))

	// drain existing topic partition subscriptions
	for _, brokerPartition := range request.BrokerPartitionAssignments {
		localPartition := topic.FromPbBrokerPartitionAssignment(self, brokerPartition)
		if request.IsDraining {
			// TODO drain existing topic partition subscriptions

			broker.localTopicManager.RemoveTopicPartition(
				topic.FromPbTopic(request.Topic),
				localPartition.Partition)
		} else {
			broker.localTopicManager.AddTopicPartition(
				topic.FromPbTopic(request.Topic),
				localPartition)
		}
	}

	// if is leader, notify the followers to drain existing topic partition subscriptions
	if request.IsLeader {
		for _, brokerPartition := range request.BrokerPartitionAssignments {
			for _, follower := range brokerPartition.FollowerBrokers {
				err := pb.WithBrokerGrpcClient(false, follower, broker.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
					_, err := client.AssignTopicPartitions(context.Background(), request)
					return err
				})
				if err != nil {
					return ret, err
				}
			}
		}
	}

	return ret, nil
}
