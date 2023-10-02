package coordinator

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
)

type ConsumerGroupInstance struct {
	ClientId string
	// the consumer group instance may not have an active partition
	Partition *topic.Partition
	// processed message count
	ProcessedMessageCount int64
}
type ConsumerGroup struct {
	// map a client id to a consumer group instance
	ConsumerGroupInstances cmap.ConcurrentMap[string, *ConsumerGroupInstance]
	MinimumActiveInstances int32
	MaximumActiveInstances int32
}
type TopicConsumerGroups struct {
	// map a consumer group name to a consumer group
	ConsumerGroups cmap.ConcurrentMap[string, *ConsumerGroup]
}

// Coordinator coordinates the instances in the consumer group for one topic.
// It is responsible for:
// 1. Assigning partitions to consumer instances.
// 2. Reassigning partitions when a consumer instance is down.
// 3. Reassigning partitions when a consumer instance is up.
type Coordinator struct {
	// map client id to subscriber
	Subscribers cmap.ConcurrentMap[string, *ConsumerGroupInstance]
	// map topic name to consumer groups
	TopicSubscribers cmap.ConcurrentMap[string, map[string]TopicConsumerGroups]
}
