package sub_coordinator

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

type ConsumerGroupInstance struct {
	InstanceId string
	// the consumer group instance may not have an active partition
	Partitions   []*topic.Partition
	ResponseChan chan *mq_pb.SubscriberToSubCoordinatorResponse
}
type ConsumerGroup struct {
	// map a consumer group instance id to a consumer group instance
	ConsumerGroupInstances cmap.ConcurrentMap[string, *ConsumerGroupInstance]
	mapping                *PartitionConsumerMapping
}

func NewConsumerGroup() *ConsumerGroup {
	return &ConsumerGroup{
		ConsumerGroupInstances: cmap.New[*ConsumerGroupInstance](),
		mapping:                NewPartitionConsumerMapping(pub_balancer.MaxPartitionCount),
	}
}

func NewConsumerGroupInstance(instanceId string) *ConsumerGroupInstance {
	return &ConsumerGroupInstance{
		InstanceId:   instanceId,
		ResponseChan: make(chan *mq_pb.SubscriberToSubCoordinatorResponse, 1),
	}
}
func (cg *ConsumerGroup) OnAddConsumerGroupInstance(consumerGroupInstance string, topic *mq_pb.Topic) {
}
func (cg *ConsumerGroup) OnRemoveConsumerGroupInstance(consumerGroupInstance string, topic *mq_pb.Topic) {

}
func (cg *ConsumerGroup) OnPartitionListChange() {

}
