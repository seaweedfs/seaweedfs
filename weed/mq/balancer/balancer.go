package balancer

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

const (
	MaxPartitionCount = 8 * 9 * 5 * 7 //2520
)

type Balancer struct {
	Brokers cmap.ConcurrentMap[string, *BrokerStats]
}

type BrokerStats struct {
	TopicPartitionCount int32
	ConsumerCount       int32
	CpuUsagePercent     int32
	Stats               cmap.ConcurrentMap[string, *TopicPartitionStats]
}

func (bs *BrokerStats) UpdateStats(stats *mq_pb.BrokerStats) {
	bs.TopicPartitionCount = int32(len(stats.Stats))
	bs.CpuUsagePercent = stats.CpuUsagePercent

	var consumerCount int32
	currentTopicPartitions := bs.Stats.Items()
	for _, topicPartitionStats := range stats.Stats {
		tps := &TopicPartitionStats{
			TopicPartition: TopicPartition{
				Namespace:  topicPartitionStats.Topic.Namespace,
				Topic:      topicPartitionStats.Topic.Name,
				RangeStart: topicPartitionStats.Partition.RangeStart,
				RangeStop:  topicPartitionStats.Partition.RangeStop,
			},
			ConsumerCount: topicPartitionStats.ConsumerCount,
			IsLeader:      topicPartitionStats.IsLeader,
		}
		consumerCount += topicPartitionStats.ConsumerCount
		key := tps.TopicPartition.String()
		bs.Stats.Set(key, tps)
		delete(currentTopicPartitions, key)
	}
	// remove the topic partitions that are not in the stats
	for key := range currentTopicPartitions {
		bs.Stats.Remove(key)
	}
	bs.ConsumerCount = consumerCount

}

type TopicPartition struct {
	Namespace  string
	Topic      string
	RangeStart int32
	RangeStop  int32
}

type TopicPartitionStats struct {
	TopicPartition
	ConsumerCount int32
	IsLeader      bool
}

func NewBalancer() *Balancer {
	return &Balancer{
		Brokers: cmap.New[*BrokerStats](),
	}
}

func NewBrokerStats() *BrokerStats {
	return &BrokerStats{
		Stats: cmap.New[*TopicPartitionStats](),
	}
}

func (tp *TopicPartition) String() string {
	return fmt.Sprintf("%v.%v-%04d-%04d", tp.Namespace, tp.Topic, tp.RangeStart, tp.RangeStop)
}
