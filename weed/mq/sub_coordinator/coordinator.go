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

func (c *Coordinator) GetTopicConsumerGroups(topic *mq_pb.Topic, createIfMissing bool) *TopicConsumerGroups {
	topicName := toTopicName(topic)
	tcg, _ := c.TopicSubscribers.Get(topicName)
	if tcg == nil && createIfMissing {
		tcg = &TopicConsumerGroups{
			ConsumerGroups: cmap.New[*ConsumerGroup](),
		}
		if !c.TopicSubscribers.SetIfAbsent(topicName, tcg) {
			tcg, _ = c.TopicSubscribers.Get(topicName)
		}
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
	tcg := c.GetTopicConsumerGroups(topic, true)
	cg, _ := tcg.ConsumerGroups.Get(consumerGroup)
	if cg == nil {
		cg = NewConsumerGroup(topic, c.balancer)
		if !tcg.ConsumerGroups.SetIfAbsent(consumerGroup, cg) {
			cg, _ = tcg.ConsumerGroups.Get(consumerGroup)
		}
	}
	cgi, _ := cg.ConsumerGroupInstances.Get(consumerGroupInstance)
	if cgi == nil {
		cgi = NewConsumerGroupInstance(consumerGroupInstance)
		if !cg.ConsumerGroupInstances.SetIfAbsent(consumerGroupInstance, cgi) {
			cgi, _ = cg.ConsumerGroupInstances.Get(consumerGroupInstance)
		}
	}
	cg.OnAddConsumerGroupInstance(consumerGroupInstance, topic)
	return cgi
}

func (c *Coordinator) RemoveSubscriber(consumerGroup, consumerGroupInstance string, topic *mq_pb.Topic) {
	tcg := c.GetTopicConsumerGroups(topic, false)
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

func (c *Coordinator) OnPartitionChange(topic *mq_pb.Topic, assignments []*mq_pb.BrokerPartitionAssignment) {
	tcg, _ := c.TopicSubscribers.Get(toTopicName(topic))
	if tcg == nil {
		return
	}
	for _, cg := range tcg.ConsumerGroups.Items() {
		cg.OnPartitionListChange(assignments)
	}
}

// OnSubAddBroker is called when a broker is added to the balancer
func (c *Coordinator) OnSubAddBroker(broker string, brokerStats *pub_balancer.BrokerStats) {

}

// OnSubRemoveBroker is called when a broker is removed from the balancer
func (c *Coordinator) OnSubRemoveBroker(broker string, brokerStats *pub_balancer.BrokerStats) {

}
