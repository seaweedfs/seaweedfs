package balancer

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"math/rand"
)

func allocateTopicPartitions(brokers cmap.ConcurrentMap[string, *BrokerStats], partitionCount int32) (assignments []*mq_pb.BrokerPartitionAssignment) {
	// divide the ring into partitions
	rangeSize := MaxPartitionCount / partitionCount
	for i := int32(0); i < partitionCount; i++ {
		assignment := &mq_pb.BrokerPartitionAssignment{
			Partition: &mq_pb.Partition{
				RingSize:   MaxPartitionCount,
				RangeStart: int32(i * rangeSize),
				RangeStop:  int32((i + 1) * rangeSize),
			},
		}
		if i == partitionCount-1 {
			assignment.Partition.RangeStop = MaxPartitionCount
		}
		assignments = append(assignments, assignment)
	}

	// pick the brokers
	pickedBrokers := pickBrokers(brokers, partitionCount)

	// assign the partitions to brokers
	for i, assignment := range assignments {
		assignment.LeaderBroker = pickedBrokers[i]
	}
	return
}

// for now: randomly pick brokers
// TODO pick brokers based on the broker stats
func pickBrokers(brokers cmap.ConcurrentMap[string, *BrokerStats], count int32) []string {
	candidates := make([]string, 0, brokers.Count())
	for brokerStatsItem := range brokers.IterBuffered() {
		candidates = append(candidates, brokerStatsItem.Key)
	}
	pickedBrokers := make([]string, 0, count)
	for i := int32(0); i < count; i++ {
		p := rand.Int() % len(candidates)
		if p < 0 {
			p = -p
		}
		pickedBrokers = append(pickedBrokers, candidates[p])
	}
	return pickedBrokers
}
