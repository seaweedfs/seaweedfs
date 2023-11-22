package sub_coordinator

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
)

type ConsumerGroupInstance struct {
	InstanceId string
	// the consumer group instance may not have an active partition
	Partition *topic.Partition
	// processed message count
	ProcessedMessageCount int64
}
type ConsumerGroup struct {
	// map a consumer group instance id to a consumer group instance
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
// 2. Reassigning partitions when a consumer instance is up/down.
// The coordinator manages for each consumer:
// 1. The consumer group
// 2. The consumer group instance
// 3. The partitions that the consumer is consuming

type Coordinator struct {
	// map client id to subscriber
	Subscribers cmap.ConcurrentMap[string, *ConsumerGroupInstance]
	// map topic name to consumer groups
	TopicSubscribers cmap.ConcurrentMap[string, *TopicConsumerGroups]
}
