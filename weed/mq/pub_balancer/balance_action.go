package pub_balancer

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
)

// PubBalancer <= PublisherToPubBalancer() <= Broker <=> Publish()
// ExecuteBalanceActionMove from PubBalancer => AssignTopicPartitions() => Broker => Publish()

func (balancer *PubBalancer) ExecuteBalanceActionMove(move *BalanceActionMove, grpcDialOption grpc.DialOption) error {
	if _, found := balancer.Brokers.Get(move.SourceBroker); !found {
		return fmt.Errorf("source broker %s not found", move.SourceBroker)
	}
	if _, found := balancer.Brokers.Get(move.TargetBroker); !found {
		return fmt.Errorf("target broker %s not found", move.TargetBroker)
	}

	err := pb.WithBrokerGrpcClient(false, move.TargetBroker, grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		_, err := client.AssignTopicPartitions(context.Background(), &mq_pb.AssignTopicPartitionsRequest{
			Topic: move.TopicPartition.Topic.ToPbTopic(),
			BrokerPartitionAssignments: []*mq_pb.BrokerPartitionAssignment{
				{
					Partition: move.TopicPartition.ToPbPartition(),
				},
			},
			IsLeader:   true,
			IsDraining: false,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("assign topic partition %v to %s: %v", move.TopicPartition, move.TargetBroker, err)
	}

	err = pb.WithBrokerGrpcClient(false, move.SourceBroker, grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		_, err := client.AssignTopicPartitions(context.Background(), &mq_pb.AssignTopicPartitionsRequest{
			Topic: move.TopicPartition.Topic.ToPbTopic(),
			BrokerPartitionAssignments: []*mq_pb.BrokerPartitionAssignment{
				{
					Partition: move.TopicPartition.ToPbPartition(),
				},
			},
			IsLeader:   true,
			IsDraining: true,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("assign topic partition %v to %s: %v", move.TopicPartition, move.SourceBroker, err)
	}

	return nil

}
