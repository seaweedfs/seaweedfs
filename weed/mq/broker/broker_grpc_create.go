package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConfigureTopic Runs on any broker, but proxied to the balancer if not the balancer
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
