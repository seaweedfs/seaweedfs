package balancer

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func allocateTopicPartitions(brokers cmap.ConcurrentMap[string, *BrokerStats], partitionCount int) (assignments []*mq_pb.BrokerPartitionAssignment) {
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
	}
}
