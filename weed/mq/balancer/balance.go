package balancer

import "github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"

func (b *Balancer) LookupOrAllocateTopicPartitions(topic *mq_pb.Topic, publish bool) ([]*mq_pb.BrokerPartitionAssignment, error) {
	// TODO lock the topic

	// find the topic partitions on the filer
	// if the topic is not found
	//   if the request is_for_publish
	//     create the topic
	//   if the request is_for_subscribe
	//     return error not found
	// t := topic.FromPbTopic(request.Topic)
	return []*mq_pb.BrokerPartitionAssignment{
		{
			LeaderBroker:    "localhost:17777",
			FollowerBrokers: []string{"localhost:17777"},
			Partition: &mq_pb.Partition{
				RingSize:   MaxPartitionCount,
				RangeStart: 0,
				RangeStop:  MaxPartitionCount,
			},
		},
	}, nil
}
