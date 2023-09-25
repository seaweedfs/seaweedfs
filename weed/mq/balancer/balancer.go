package balancer

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

const (
	MaxPartitionCount  = 8 * 9 * 5 * 7 //2520
	LockBrokerBalancer = "broker_balancer"
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
			TopicPartition: topic.TopicPartition{
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

func (bs *BrokerStats) RegisterAssignment(t *mq_pb.Topic, partition *mq_pb.Partition) {
	tps := &TopicPartitionStats{
		TopicPartition: topic.TopicPartition{
			Namespace:  t.Namespace,
			Topic:      t.Name,
			RangeStart: partition.RangeStart,
			RangeStop:  partition.RangeStop,
		},
		ConsumerCount: 0,
		IsLeader:      true,
	}
	key := tps.TopicPartition.String()
	bs.Stats.Set(key, tps)
}

func (bs *BrokerStats) String() string {
	return fmt.Sprintf("BrokerStats{TopicPartitionCount:%d, ConsumerCount:%d, CpuUsagePercent:%d, Stats:%+v}",
		bs.TopicPartitionCount, bs.ConsumerCount, bs.CpuUsagePercent, bs.Stats.Items())
}

type TopicPartitionStats struct {
	topic.TopicPartition
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
