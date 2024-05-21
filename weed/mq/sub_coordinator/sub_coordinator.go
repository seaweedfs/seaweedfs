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

// SubCoordinator coordinates the instances in the consumer group for one topic.
// It is responsible for:
// 1. (Maybe) assigning partitions when a consumer instance is up/down.

type SubCoordinator struct {
	// map topic name to consumer groups
	TopicSubscribers    cmap.ConcurrentMap[string, *TopicConsumerGroups]
	balancer            *pub_balancer.PubBalancer
	FilerClientAccessor *FilerClientAccessor
}

func NewCoordinator(balancer *pub_balancer.PubBalancer) *SubCoordinator {
	return &SubCoordinator{
		TopicSubscribers: cmap.New[*TopicConsumerGroups](),
		balancer:         balancer,
	}
}

func (c *SubCoordinator) GetTopicConsumerGroups(topic *mq_pb.Topic, createIfMissing bool) *TopicConsumerGroups {
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
func (c *SubCoordinator) RemoveTopic(topic *mq_pb.Topic) {
	topicName := toTopicName(topic)
	c.TopicSubscribers.Remove(topicName)
}

func toTopicName(topic *mq_pb.Topic) string {
	topicName := topic.Namespace + "." + topic.Name
	return topicName
}

func (c *SubCoordinator) AddSubscriber(initMessage *mq_pb.SubscriberToSubCoordinatorRequest_InitMessage) *ConsumerGroupInstance {
	tcg := c.GetTopicConsumerGroups(initMessage.Topic, true)
	cg, _ := tcg.ConsumerGroups.Get(initMessage.ConsumerGroup)
	if cg == nil {
		cg = NewConsumerGroup(initMessage.Topic, c.balancer, c.FilerClientAccessor)
		if !tcg.ConsumerGroups.SetIfAbsent(initMessage.ConsumerGroup, cg) {
			cg, _ = tcg.ConsumerGroups.Get(initMessage.ConsumerGroup)
		}
	}
	cgi, _ := cg.ConsumerGroupInstances.Get(initMessage.ConsumerGroupInstanceId)
	if cgi == nil {
		cgi = NewConsumerGroupInstance(initMessage.ConsumerGroupInstanceId)
		if !cg.ConsumerGroupInstances.SetIfAbsent(initMessage.ConsumerGroupInstanceId, cgi) {
			cgi, _ = cg.ConsumerGroupInstances.Get(initMessage.ConsumerGroupInstanceId)
		}
	}
	cgi.MaxPartitionCount = initMessage.MaxPartitionCount
	cg.OnAddConsumerGroupInstance(initMessage.ConsumerGroupInstanceId, initMessage.Topic, initMessage.MaxPartitionCount, initMessage.RebalanceSeconds)
	return cgi
}

func (c *SubCoordinator) RemoveSubscriber(initMessage *mq_pb.SubscriberToSubCoordinatorRequest_InitMessage) {
	tcg := c.GetTopicConsumerGroups(initMessage.Topic, false)
	if tcg == nil {
		return
	}
	cg, _ := tcg.ConsumerGroups.Get(initMessage.ConsumerGroup)
	if cg == nil {
		return
	}
	cg.ConsumerGroupInstances.Remove(initMessage.ConsumerGroupInstanceId)
	cg.OnRemoveConsumerGroupInstance(initMessage.ConsumerGroupInstanceId, initMessage.Topic, initMessage.MaxPartitionCount, initMessage.RebalanceSeconds)
	if cg.ConsumerGroupInstances.Count() == 0 {
		tcg.ConsumerGroups.Remove(initMessage.ConsumerGroup)
	}
	if tcg.ConsumerGroups.Count() == 0 {
		c.RemoveTopic(initMessage.Topic)
	}
}

func (c *SubCoordinator) OnPartitionChange(topic *mq_pb.Topic, assignments []*mq_pb.BrokerPartitionAssignment) {
	tcg, _ := c.TopicSubscribers.Get(toTopicName(topic))
	if tcg == nil {
		return
	}
	for _, cg := range tcg.ConsumerGroups.Items() {
		cg.OnPartitionListChange(assignments)
	}
}

// OnSubAddBroker is called when a broker is added to the balancer
func (c *SubCoordinator) OnSubAddBroker(broker string, brokerStats *pub_balancer.BrokerStats) {

}

// OnSubRemoveBroker is called when a broker is removed from the balancer
func (c *SubCoordinator) OnSubRemoveBroker(broker string, brokerStats *pub_balancer.BrokerStats) {

}
