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
	Partitions        []*topic.Partition
	ResponseChan      chan *mq_pb.SubscriberToSubCoordinatorResponse
	MaxPartitionCount int32
}
type ConsumerGroup struct {
	topic topic.Topic
	// map a consumer group instance id to a consumer group instance
	ConsumerGroupInstances cmap.ConcurrentMap[string, *ConsumerGroupInstance]
	mapping                *PartitionConsumerMapping
	reBalanceTimer         *time.Timer
	pubBalancer            *pub_balancer.Balancer
	filerClientAccessor    *FilerClientAccessor
}

func NewConsumerGroup(t *mq_pb.Topic, pubBalancer *pub_balancer.Balancer, filerClientAccessor *FilerClientAccessor) *ConsumerGroup {
	return &ConsumerGroup{
		topic:                  topic.FromPbTopic(t),
		ConsumerGroupInstances: cmap.New[*ConsumerGroupInstance](),
		mapping:                NewPartitionConsumerMapping(pub_balancer.MaxPartitionCount),
		pubBalancer:            pubBalancer,
		filerClientAccessor:    filerClientAccessor,
	}
}

func NewConsumerGroupInstance(instanceId string) *ConsumerGroupInstance {
	return &ConsumerGroupInstance{
		InstanceId:   instanceId,
		ResponseChan: make(chan *mq_pb.SubscriberToSubCoordinatorResponse, 1),
	}
}
func (cg *ConsumerGroup) OnAddConsumerGroupInstance(consumerGroupInstance string, topic *mq_pb.Topic, maxPartitionCount, rebalanceSeconds int32) {
	cg.onConsumerGroupInstanceChange(true, "add consumer instance "+consumerGroupInstance, maxPartitionCount, rebalanceSeconds)
}
func (cg *ConsumerGroup) OnRemoveConsumerGroupInstance(consumerGroupInstance string, topic *mq_pb.Topic, maxPartitionCount, rebalanceSeconds int32) {
	cg.onConsumerGroupInstanceChange(false, "remove consumer instance "+consumerGroupInstance, maxPartitionCount, rebalanceSeconds)
}

func (cg *ConsumerGroup) onConsumerGroupInstanceChange(isAdd bool, reason string, maxPartitionCount, rebalanceSeconds int32) {
	if cg.reBalanceTimer != nil {
		cg.reBalanceTimer.Stop()
		cg.reBalanceTimer = nil
	}
	if maxPartitionCount == 0 {
		maxPartitionCount = 1
	}
	if rebalanceSeconds == 0 {
		rebalanceSeconds = 10
	}
	if isAdd {
		if conf, err := cg.filerClientAccessor.ReadTopicConfFromFiler(cg.topic); err == nil {
			var sumMaxPartitionCount int32
			for _, cgi := range cg.ConsumerGroupInstances.Items() {
				sumMaxPartitionCount += cgi.MaxPartitionCount
			}
			if sumMaxPartitionCount < int32(len(conf.BrokerPartitionAssignments)) && sumMaxPartitionCount+maxPartitionCount >= int32(len(conf.BrokerPartitionAssignments)) {
				partitionSlotToBrokerList := pub_balancer.NewPartitionSlotToBrokerList(pub_balancer.MaxPartitionCount)
				for _, assignment := range conf.BrokerPartitionAssignments {
					partitionSlotToBrokerList.AddBroker(assignment.Partition, assignment.LeaderBroker, assignment.FollowerBroker)
				}
				cg.BalanceConsumerGroupInstances(partitionSlotToBrokerList, reason)
				return
			}
		}
	}
	cg.reBalanceTimer = time.AfterFunc(time.Duration(rebalanceSeconds)*time.Second, func() {
		cg.BalanceConsumerGroupInstances(nil, reason)
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
		partitionSlotToBrokerList.AddBroker(assignment.Partition, assignment.LeaderBroker, assignment.FollowerBroker)
	}
	cg.BalanceConsumerGroupInstances(partitionSlotToBrokerList, "partition list change")
}

func (cg *ConsumerGroup) BalanceConsumerGroupInstances(knownPartitionSlotToBrokerList *pub_balancer.PartitionSlotToBrokerList, reason string) {
	glog.V(0).Infof("rebalance consumer group %s due to %s", cg.topic.String(), reason)

	// collect current topic partitions
	partitionSlotToBrokerList := knownPartitionSlotToBrokerList
	if partitionSlotToBrokerList == nil {
		if conf, err := cg.filerClientAccessor.ReadTopicConfFromFiler(cg.topic); err == nil {
			partitionSlotToBrokerList = pub_balancer.NewPartitionSlotToBrokerList(pub_balancer.MaxPartitionCount)
			for _, assignment := range conf.BrokerPartitionAssignments {
				partitionSlotToBrokerList.AddBroker(assignment.Partition, assignment.LeaderBroker, assignment.FollowerBroker)
			}
		} else {
			glog.V(0).Infof("fail to read topic conf from filer: %v", err)
			return
		}
	}

	// collect current consumer group instance ids
	var consumerInstances []*ConsumerGroupInstance
	for _, consumerGroupInstance := range cg.ConsumerGroupInstances.Items() {
		consumerInstances = append(consumerInstances, consumerGroupInstance)
	}

	cg.mapping.BalanceToConsumerInstances(partitionSlotToBrokerList, consumerInstances)

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
		for _, partitionSlot := range partitionSlots {
			consumerGroupInstance.ResponseChan <- &mq_pb.SubscriberToSubCoordinatorResponse{
				Message: &mq_pb.SubscriberToSubCoordinatorResponse_Assignment_{
					Assignment: &mq_pb.SubscriberToSubCoordinatorResponse_Assignment{
						PartitionAssignment: &mq_pb.BrokerPartitionAssignment{
							Partition: &mq_pb.Partition{
								RangeStop:  partitionSlot.RangeStop,
								RangeStart: partitionSlot.RangeStart,
								RingSize:   partitionSlotToBrokerList.RingSize,
								UnixTimeNs: partitionSlot.UnixTimeNs,
							},
							LeaderBroker:   partitionSlot.Broker,
							FollowerBroker: partitionSlot.FollowerBroker,
						},
					},
				},
			}
		}
	}

}
