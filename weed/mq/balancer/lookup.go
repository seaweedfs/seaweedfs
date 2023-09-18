package balancer

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (b *Balancer) LookupOrAllocateTopicPartitions(topic *mq_pb.Topic, publish bool) (assignments []*mq_pb.BrokerPartitionAssignment, err error) {
	// find existing topic partition assignments
	for brokerStatsItem := range b.Brokers.IterBuffered() {
		broker, brokerStats := brokerStatsItem.Key, brokerStatsItem.Val
		for topicPartitionStatsItem := range brokerStats.Stats.IterBuffered() {
			topicPartitionStat := topicPartitionStatsItem.Val
			if topicPartitionStat.TopicPartition.Namespace == topic.Namespace &&
				topicPartitionStat.TopicPartition.Topic == topic.Name {
				assignment := &mq_pb.BrokerPartitionAssignment{
					Partition: &mq_pb.Partition{
						RingSize:   MaxPartitionCount,
						RangeStart: topicPartitionStat.RangeStart,
						RangeStop:  topicPartitionStat.RangeStop,
					},
				}
				if topicPartitionStat.IsLeader {
					assignment.LeaderBroker = broker
				} else {
					assignment.FollowerBrokers = append(assignment.FollowerBrokers, broker)
				}
				assignments = append(assignments, assignment)
			}
		}
	}
	if len(assignments) > 0 {
		return assignments, nil
	}

	// find the topic partitions on the filer
	// if the topic is not found
	//   if the request is_for_publish
	//     create the topic
	//   if the request is_for_subscribe
	//     return error not found
	// t := topic.FromPbTopic(request.Topic)
	return allocateTopicPartitions(b.Brokers, 6), nil
}
