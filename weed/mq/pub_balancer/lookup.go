package pub_balancer

import (
	"errors"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

var (
	ErrNoBroker = errors.New("no broker")
)

func (balancer *Balancer) LookupOrAllocateTopicPartitions(topic *mq_pb.Topic, publish bool, partitionCount int32) (assignments []*mq_pb.BrokerPartitionAssignment, err error) {
	if partitionCount == 0 {
		partitionCount = 6
	}
	// find existing topic partition assignments
	for brokerStatsItem := range balancer.Brokers.IterBuffered() {
		broker, brokerStats := brokerStatsItem.Key, brokerStatsItem.Val
		for topicPartitionStatsItem := range brokerStats.TopicPartitionStats.IterBuffered() {
			topicPartitionStat := topicPartitionStatsItem.Val
			if topicPartitionStat.TopicPartition.Namespace == topic.Namespace &&
				topicPartitionStat.TopicPartition.Name == topic.Name {
				assignment := &mq_pb.BrokerPartitionAssignment{
					Partition: &mq_pb.Partition{
						RingSize:   MaxPartitionCount,
						RangeStart: topicPartitionStat.RangeStart,
						RangeStop:  topicPartitionStat.RangeStop,
					},
				}
				// TODO fix follower setting
				assignment.LeaderBroker = broker
				assignments = append(assignments, assignment)
			}
		}
	}
	if len(assignments) > 0 && len(assignments) == int(partitionCount) || !publish {
		glog.V(0).Infof("existing topic partitions %d: %v", len(assignments), assignments)
		return assignments, nil
	}

	// find the topic partitions on the filer
	// if the topic is not found
	//   if the request is_for_publish
	//     create the topic
	//   if the request is_for_subscribe
	//     return error not found
	// t := topic.FromPbTopic(request.Topic)
	if balancer.Brokers.IsEmpty() {
		return nil, ErrNoBroker
	}
	return allocateTopicPartitions(balancer.Brokers, partitionCount), nil
}
