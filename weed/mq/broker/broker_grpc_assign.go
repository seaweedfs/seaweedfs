package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

// AssignTopicPartitions Runs on the assigned broker, to execute the topic partition assignment
func (b *MessageQueueBroker) AssignTopicPartitions(c context.Context, request *mq_pb.AssignTopicPartitionsRequest) (*mq_pb.AssignTopicPartitionsResponse, error) {
	ret := &mq_pb.AssignTopicPartitionsResponse{}
	self := pb.ServerAddress(fmt.Sprintf("%s:%d", b.option.Ip, b.option.Port))

	// drain existing topic partition subscriptions
	for _, assignment := range request.BrokerPartitionAssignments {
		t := topic.FromPbTopic(request.Topic)
		partition := topic.FromPbPartition(assignment.Partition)
		b.accessLock.Lock()
		if request.IsDraining {
			// TODO drain existing topic partition subscriptions
			b.localTopicManager.RemoveTopicPartition(t, partition)
		} else {
			var localPartition *topic.LocalPartition
			if localPartition = b.localTopicManager.GetTopicPartition(t, partition); localPartition == nil {
				localPartition = topic.FromPbBrokerPartitionAssignment(self, partition, assignment, b.genLogFlushFunc(t, assignment.Partition), b.genLogOnDiskReadFunc(t, assignment.Partition))
				b.localTopicManager.AddTopicPartition(t, localPartition)
			}
		}
		b.accessLock.Unlock()
	}

	// if is leader, notify the followers to drain existing topic partition subscriptions
	if request.IsLeader {
		for _, brokerPartition := range request.BrokerPartitionAssignments {
			for _, follower := range brokerPartition.FollowerBrokers {
				err := pb.WithBrokerGrpcClient(false, follower, b.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
					_, err := client.AssignTopicPartitions(context.Background(), request)
					return err
				})
				if err != nil {
					return ret, err
				}
			}
		}
	}

	glog.V(0).Infof("AssignTopicPartitions: topic %s partition assignments: %v", request.Topic, request.BrokerPartitionAssignments)
	return ret, nil
}
