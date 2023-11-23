package sub_coordinator

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

type ConsumerGroupInstance struct {
	InstanceId string
	// the consumer group instance may not have an active partition
	Partitions []*topic.Partition
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
	// map topic name to consumer groups
	TopicSubscribers cmap.ConcurrentMap[string, *TopicConsumerGroups]
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		TopicSubscribers: cmap.New[*TopicConsumerGroups](),
	}
}

func (c *Coordinator) GetTopicConsumerGroups(topic *mq_pb.Topic) *TopicConsumerGroups {
	topicName := toTopicName(topic)
	tcg, _ := c.TopicSubscribers.Get(topicName)
	if tcg == nil {
		tcg = &TopicConsumerGroups{
			ConsumerGroups: cmap.New[*ConsumerGroup](),
		}
		c.TopicSubscribers.Set(topicName, tcg)
	}
	return tcg
}
func (c *Coordinator) RemoveTopic(topic *mq_pb.Topic) {
	topicName := toTopicName(topic)
	c.TopicSubscribers.Remove(topicName)
}

func toTopicName(topic *mq_pb.Topic) string {
	topicName := topic.Namespace + "." + topic.Name
	return topicName
}

func (c *Coordinator) AddSubscriber(consumerGroup, consumerGroupInstance string, topic *mq_pb.Topic) {
	tcg := c.GetTopicConsumerGroups(topic)
	cg, _ := tcg.ConsumerGroups.Get(consumerGroup)
	if cg == nil {
		cg = &ConsumerGroup{
			ConsumerGroupInstances: cmap.New[*ConsumerGroupInstance](),
		}
		tcg.ConsumerGroups.Set(consumerGroup, cg)
	}
	cgi, _ := cg.ConsumerGroupInstances.Get(consumerGroupInstance)
	if cgi == nil {
		cgi = &ConsumerGroupInstance{
			InstanceId: consumerGroupInstance,
		}
		cg.ConsumerGroupInstances.Set(consumerGroupInstance, cgi)
	}
}

func (c *Coordinator) RemoveSubscriber(consumerGroup, consumerGroupInstance string, topic *mq_pb.Topic) {
	tcg, _ := c.TopicSubscribers.Get(toTopicName(topic))
	if tcg == nil {
		return
	}
	cg, _ := tcg.ConsumerGroups.Get(consumerGroup)
	if cg == nil {
		return
	}
	cg.ConsumerGroupInstances.Remove(consumerGroupInstance)
	if cg.ConsumerGroupInstances.Count() == 0 {
		tcg.ConsumerGroups.Remove(consumerGroup)
	}
	if tcg.ConsumerGroups.Count() == 0 {
		c.RemoveTopic(topic)
	}
}
