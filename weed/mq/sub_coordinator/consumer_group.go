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
	topic topic.Topic
	// map a consumer group instance id to a consumer group instance
	ConsumerGroupInstances cmap.ConcurrentMap[string, *ConsumerGroupInstance]
	mapping                *PartitionConsumerMapping
	reBalanceTimer         *time.Timer
	pubBalancer            *pub_balancer.Balancer
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
	cg.onConsumerGroupInstanceChange("add consumer instance " + consumerGroupInstance)
}
func (cg *ConsumerGroup) OnRemoveConsumerGroupInstance(consumerGroupInstance string, topic *mq_pb.Topic) {
	cg.onConsumerGroupInstanceChange("remove consumer instance " + consumerGroupInstance)
}

func (cg *ConsumerGroup) onConsumerGroupInstanceChange(reason string) {
	if cg.reBalanceTimer != nil {
		cg.reBalanceTimer.Stop()
		cg.reBalanceTimer = nil
	}
	cg.reBalanceTimer = time.AfterFunc(5*time.Second, func() {
		cg.RebalanceConsumberGroupInstances(nil, reason)
		cg.reBalanceTimer = nil
	})
}
func (cg *ConsumerGroup) OnPartitionListChange(assignments []*mq_pb.BrokerPartitionAssignment) {
	if cg.reBalanceTimer != nil {
		cg.reBalanceTimer.Stop()
		cg.reBalanceTimer = nil
	}
	partitionSlotToBrokerList := pub_balancer.NewPartitionSlotToBrokerList(pub_balancer.MaxPartitionCount)
	for _, assignment := range assignments {
		partitionSlotToBrokerList.AddBroker(assignment.Partition, assignment.LeaderBroker)
	}
	cg.RebalanceConsumberGroupInstances(partitionSlotToBrokerList, "partition list change")
}

func (cg *ConsumerGroup) RebalanceConsumberGroupInstances(knownPartitionSlotToBrokerList *pub_balancer.PartitionSlotToBrokerList, reason string) {
	glog.V(0).Infof("rebalance consumer group %s due to %s", cg.topic.String(), reason)

	// collect current topic partitions
	partitionSlotToBrokerList := knownPartitionSlotToBrokerList
	if partitionSlotToBrokerList == nil {
		var found bool
		partitionSlotToBrokerList, found = cg.pubBalancer.TopicToBrokers.Get(cg.topic.String())
		if !found {
			glog.V(0).Infof("topic %s not found in balancer", cg.topic.String())
			return
		}
	}

	// collect current consumer group instance ids
	var consumerInstanceIds []string
	for _, consumerGroupInstance := range cg.ConsumerGroupInstances.Items() {
		consumerInstanceIds = append(consumerInstanceIds, consumerGroupInstance.InstanceId)
	}

	cg.mapping.BalanceToConsumerInstanceIds(partitionSlotToBrokerList, consumerInstanceIds)

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
		consumerGroupInstance.Partitions = ToPartitions(partitionSlotToBrokerList.RingSize, partitionSlots)
		assignedPartitions := make([]*mq_pb.BrokerPartitionAssignment, len(partitionSlots))
		for i, partitionSlot := range partitionSlots {
			assignedPartitions[i] = &mq_pb.BrokerPartitionAssignment{
				Partition: &mq_pb.Partition{
					RangeStop:  partitionSlot.RangeStop,
					RangeStart: partitionSlot.RangeStart,
					RingSize:   partitionSlotToBrokerList.RingSize,
					UnixTimeNs: partitionSlot.UnixTimeNs,
				},
				LeaderBroker: partitionSlot.Broker,
			}
		}
		response := &mq_pb.SubscriberToSubCoordinatorResponse{
			Message: &mq_pb.SubscriberToSubCoordinatorResponse_Assignment_{
				Assignment: &mq_pb.SubscriberToSubCoordinatorResponse_Assignment{
					PartitionAssignments: assignedPartitions,
				},
			},
		}
		println("sending response to", consumerGroupInstance.InstanceId, "...")
		consumerGroupInstance.ResponseChan <- response
	}

}
