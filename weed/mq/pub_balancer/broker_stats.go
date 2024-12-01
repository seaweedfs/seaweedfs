package pub_balancer

import (
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

type BrokerStats struct {
	TopicPartitionCount int32
	PublisherCount      int32
	SubscriberCount     int32
	CpuUsagePercent     int32
	TopicPartitionStats cmap.ConcurrentMap[string, *TopicPartitionStats] // key: topic_partition
	Topics              []topic.Topic
}
type TopicPartitionStats struct {
	topic.TopicPartition
	PublisherCount  int32
	SubscriberCount int32
}

func NewBrokerStats() *BrokerStats {
	return &BrokerStats{
		TopicPartitionStats: cmap.New[*TopicPartitionStats](),
	}
}
func (bs *BrokerStats) String() string {
	return fmt.Sprintf("BrokerStats{TopicPartitionCount:%d, Publishers:%d, Subscribers:%d CpuUsagePercent:%d, Stats:%+v}",
		bs.TopicPartitionCount, bs.PublisherCount, bs.SubscriberCount, bs.CpuUsagePercent, bs.TopicPartitionStats.Items())
}

func (bs *BrokerStats) UpdateStats(stats *mq_pb.BrokerStats) {
	bs.TopicPartitionCount = int32(len(stats.Stats))
	bs.CpuUsagePercent = stats.CpuUsagePercent

	var publisherCount, subscriberCount int32
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
			PublisherCount:  topicPartitionStats.PublisherCount,
			SubscriberCount: topicPartitionStats.SubscriberCount,
		}
		publisherCount += topicPartitionStats.PublisherCount
		subscriberCount += topicPartitionStats.SubscriberCount
		key := tps.TopicPartition.TopicPartitionId()
		bs.TopicPartitionStats.Set(key, tps)
		delete(currentTopicPartitions, key)
	}
	// remove the topic partitions that are not in the stats
	for key := range currentTopicPartitions {
		bs.TopicPartitionStats.Remove(key)
	}
	bs.PublisherCount = publisherCount
	bs.SubscriberCount = subscriberCount
}

func (bs *BrokerStats) RegisterAssignment(t *schema_pb.Topic, partition *schema_pb.Partition, isAdd bool) {
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
		PublisherCount:  0,
		SubscriberCount: 0,
	}
	key := tps.TopicPartition.TopicPartitionId()
	if isAdd {
		bs.TopicPartitionStats.SetIfAbsent(key, tps)
	} else {
		bs.TopicPartitionStats.Remove(key)
	}
}
