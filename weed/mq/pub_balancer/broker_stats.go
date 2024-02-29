package pub_balancer

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

type BrokerStats struct {
	TopicPartitionCount int32
	ConsumerCount       int32
	CpuUsagePercent     int32
	TopicPartitionStats cmap.ConcurrentMap[string, *TopicPartitionStats] // key: topic_partition
	Topics              []topic.Topic
}
type TopicPartitionStats struct {
	topic.TopicPartition
	ConsumerCount int32
	IsLeader      bool
}

func NewBrokerStats() *BrokerStats {
	return &BrokerStats{
		TopicPartitionStats: cmap.New[*TopicPartitionStats](),
	}
}
func (bs *BrokerStats) String() string {
	return fmt.Sprintf("BrokerStats{TopicPartitionCount:%d, ConsumerCount:%d, CpuUsagePercent:%d, Stats:%+v}",
		bs.TopicPartitionCount, bs.ConsumerCount, bs.CpuUsagePercent, bs.TopicPartitionStats.Items())
}

func (bs *BrokerStats) UpdateStats(stats *mq_pb.BrokerStats) {
	bs.TopicPartitionCount = int32(len(stats.Stats))
	bs.CpuUsagePercent = stats.CpuUsagePercent

	var consumerCount int32
	currentTopicPartitions := bs.TopicPartitionStats.Items()
	for _, topicPartitionStats := range stats.Stats {
		tps := &TopicPartitionStats{
			TopicPartition: topic.TopicPartition{
				Topic: topic.Topic{Namespace: topicPartitionStats.Topic.Namespace, Name: topicPartitionStats.Topic.Name},
				Partition: topic.Partition{
					RangeStart: topicPartitionStats.Partition.RangeStart,
					RangeStop:  topicPartitionStats.Partition.RangeStop,
					RingSize:   topicPartitionStats.Partition.RingSize,
					UnixTimeNs: topicPartitionStats.Partition.UnixTimeNs,
				},
			},
			ConsumerCount: topicPartitionStats.ConsumerCount,
			IsLeader:      topicPartitionStats.IsLeader,
		}
		consumerCount += topicPartitionStats.ConsumerCount
		key := tps.TopicPartition.String()
		bs.TopicPartitionStats.Set(key, tps)
		delete(currentTopicPartitions, key)
	}
	// remove the topic partitions that are not in the stats
	for key := range currentTopicPartitions {
		bs.TopicPartitionStats.Remove(key)
	}
	bs.ConsumerCount = consumerCount

}

func (bs *BrokerStats) RegisterAssignment(t *mq_pb.Topic, partition *mq_pb.Partition, isAdd bool) {
	tps := &TopicPartitionStats{
		TopicPartition: topic.TopicPartition{
			Topic: topic.Topic{Namespace: t.Namespace, Name: t.Name},
			Partition: topic.Partition{
				RangeStart: partition.RangeStart,
				RangeStop:  partition.RangeStop,
				RingSize:   partition.RingSize,
				UnixTimeNs: partition.UnixTimeNs,
			},
		},
		ConsumerCount: 0,
		IsLeader:      true,
	}
	key := tps.TopicPartition.String()
	if isAdd {
		bs.TopicPartitionStats.SetIfAbsent(key, tps)
	} else {
		bs.TopicPartitionStats.Remove(key)
	}
}
