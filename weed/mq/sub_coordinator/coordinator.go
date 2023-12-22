package sub_coordinator

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

type TopicConsumerGroups struct {
	// map a consumer group name to a consumer group
	ConsumerGroups cmap.ConcurrentMap[string, *ConsumerGroup]
}

// Coordinator coordinates the instances in the consumer group for one topic.
// It is responsible for:
// 1. (Maybe) assigning partitions when a consumer instance is up/down.

type Coordinator struct {
	// map topic name to consumer groups
	TopicSubscribers cmap.ConcurrentMap[string, *TopicConsumerGroups]
	balancer         *pub_balancer.Balancer
}

func NewCoordinator(balancer *pub_balancer.Balancer) *Coordinator {
	return &Coordinator{
		TopicSubscribers: cmap.New[*TopicConsumerGroups](),
		balancer:         balancer,
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

func (c *Coordinator) AddSubscriber(consumerGroup, consumerGroupInstance string, topic *mq_pb.Topic) *ConsumerGroupInstance {
	tcg := c.GetTopicConsumerGroups(topic)
	cg, _ := tcg.ConsumerGroups.Get(consumerGroup)
	if cg == nil {
		cg = NewConsumerGroup()
		tcg.ConsumerGroups.Set(consumerGroup, cg)
	}
	cgi, _ := cg.ConsumerGroupInstances.Get(consumerGroupInstance)
	if cgi == nil {
		cgi = NewConsumerGroupInstance(consumerGroupInstance)
		cg.ConsumerGroupInstances.Set(consumerGroupInstance, cgi)
	}
	cg.OnAddConsumerGroupInstance(consumerGroupInstance, topic)
	return cgi
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
	cg.OnRemoveConsumerGroupInstance(consumerGroupInstance, topic)
	if cg.ConsumerGroupInstances.Count() == 0 {
		tcg.ConsumerGroups.Remove(consumerGroup)
	}
	if tcg.ConsumerGroups.Count() == 0 {
		c.RemoveTopic(topic)
	}
}
