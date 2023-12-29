package sub_coordinator

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"time"
)

type ConsumerGroupInstance struct {
	InstanceId string
	// the consumer group instance may not have an active partition
	Partitions   []*topic.Partition
	ResponseChan chan *mq_pb.SubscriberToSubCoordinatorResponse
}
type ConsumerGroup struct {
	topic         topic.Topic
	// map a consumer group instance id to a consumer group instance
	ConsumerGroupInstances cmap.ConcurrentMap[string, *ConsumerGroupInstance]
	mapping        *PartitionConsumerMapping
	reBalanceTimer *time.Timer
	pubBalancer    *pub_balancer.Balancer
}

func NewConsumerGroup(t *mq_pb.Topic, pubBalancer *pub_balancer.Balancer) *ConsumerGroup {
	return &ConsumerGroup{
		topic:                  topic.FromPbTopic(t),
		ConsumerGroupInstances: cmap.New[*ConsumerGroupInstance](),
		mapping:                NewPartitionConsumerMapping(pub_balancer.MaxPartitionCount),
		pubBalancer:            pubBalancer,
	}
}

func NewConsumerGroupInstance(instanceId string) *ConsumerGroupInstance {
	return &ConsumerGroupInstance{
		InstanceId:   instanceId,
		ResponseChan: make(chan *mq_pb.SubscriberToSubCoordinatorResponse, 1),
	}
}
func (cg *ConsumerGroup) OnAddConsumerGroupInstance(consumerGroupInstance string, topic *mq_pb.Topic) {
	cg.onConsumerGroupInstanceChange()
}
func (cg *ConsumerGroup) OnRemoveConsumerGroupInstance(consumerGroupInstance string, topic *mq_pb.Topic) {
	cg.onConsumerGroupInstanceChange()
}

func (cg *ConsumerGroup) onConsumerGroupInstanceChange(){
	if cg.reBalanceTimer != nil {
		cg.reBalanceTimer.Stop()
		cg.reBalanceTimer = nil
	}
	cg.reBalanceTimer = time.AfterFunc(5*time.Second, func() {
		cg.RebalanceConsumberGroupInstances()
		cg.reBalanceTimer = nil
	})
}
func (cg *ConsumerGroup) OnPartitionListChange() {
	if cg.reBalanceTimer != nil {
		cg.reBalanceTimer.Stop()
		cg.reBalanceTimer = nil
	}
	cg.RebalanceConsumberGroupInstances()
}

func (cg *ConsumerGroup) RebalanceConsumberGroupInstances() {
	println("rebalance...")

	now := time.Now().UnixNano()

	// collect current topic partitions
	partitionSlotToBrokerList, found := cg.pubBalancer.TopicToBrokers.Get(cg.topic.String())
	if !found {
		glog.V(0).Infof("topic %s not found in balancer", cg.topic.String())
		return
	}
	partitions := make([]*topic.Partition, 0)
	for _, partitionSlot := range partitionSlotToBrokerList.PartitionSlots {
		partitions = append(partitions, topic.NewPartition(partitionSlot.RangeStart, partitionSlot.RangeStop, partitionSlotToBrokerList.RingSize, now))
	}

	// collect current consumer group instance ids
	consumerInstanceIds := make([]string, 0)
	for _, consumerGroupInstance := range cg.ConsumerGroupInstances.Items() {
		consumerInstanceIds = append(consumerInstanceIds, consumerGroupInstance.InstanceId)
	}

	cg.mapping.BalanceToConsumerInstanceIds(partitions, consumerInstanceIds)

	// convert cg.mapping currentMapping to map of consumer group instance id to partition slots
	consumerInstanceToPartitionSlots := make(map[string][]*PartitionSlotToConsumerInstance)
	for _, partitionSlot := range cg.mapping.currentMapping.PartitionSlots {
		consumerInstanceToPartitionSlots[partitionSlot.AssignedInstanceId] = append(consumerInstanceToPartitionSlots[partitionSlot.AssignedInstanceId], partitionSlot)
	}

	// notify consumer group instances
	for _, consumerGroupInstance := range cg.ConsumerGroupInstances.Items() {
		partitionSlots, found := consumerInstanceToPartitionSlots[consumerGroupInstance.InstanceId]
		if !found {
			partitionSlots = make([]*PartitionSlotToConsumerInstance, 0)
		}
		consumerGroupInstance.Partitions = ToPartitions(partitionSlotToBrokerList.RingSize, partitionSlots, now)
		assignedPartitions := make([]*mq_pb.SubscriberToSubCoordinatorResponse_AssignedPartition, len(partitionSlots))
		for i, partitionSlot := range partitionSlots {
			assignedPartitions[i] = &mq_pb.SubscriberToSubCoordinatorResponse_AssignedPartition{
				Partition: &mq_pb.Partition{
					RangeStop: partitionSlot.RangeStop,
					RangeStart: partitionSlot.RangeStart,
					RingSize: partitionSlotToBrokerList.RingSize,
					UnixTimeNs: now,
				},
			}
		}
		response := &mq_pb.SubscriberToSubCoordinatorResponse{
			Message: &mq_pb.SubscriberToSubCoordinatorResponse_Assignment_{
				Assignment: &mq_pb.SubscriberToSubCoordinatorResponse_Assignment{
					AssignedPartitions: assignedPartitions,
				},
			},
		}
		consumerGroupInstance.ResponseChan <- response
	}


}
