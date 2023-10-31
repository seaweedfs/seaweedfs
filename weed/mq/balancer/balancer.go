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

// Balancer collects stats from all brokers.
//
//	When publishers wants to create topics, it picks brokers to assign the topic partitions.
//	When consumers wants to subscribe topics, it tells which brokers are serving the topic partitions.
//
// When a partition needs to be split or merged, or a partition needs to be moved to another broker,
// the balancer will let the broker tell the consumer instance to stop processing the partition.
// The existing consumer instance will flush the internal state, and then stop processing.
// Then the balancer will tell the brokers to start sending new messages in the new/moved partition to the consumer instances.
//
// Failover to standby consumer instances:
//
//	A consumer group can have min and max number of consumer instances.
//	For consumer instances joined after the max number, they will be in standby mode.
//
//	When a consumer instance is down, the broker will notice this and inform the balancer.
//	The balancer will then tell the broker to send the partition to another standby consumer instance.
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
				Topic:     topic.Topic{Namespace: topicPartitionStats.Topic.Namespace, Name: topicPartitionStats.Topic.Name},
				Partition: topic.Partition{RangeStart: topicPartitionStats.Partition.RangeStart, RangeStop: topicPartitionStats.Partition.RangeStop, RingSize: topicPartitionStats.Partition.RingSize},
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
			Topic:     topic.Topic{Namespace: t.Namespace, Name: t.Name},
			Partition: topic.Partition{RangeStart: partition.RangeStart, RangeStop: partition.RangeStop},
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
