package sub_coordinator

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"time"
)

type PartitionConsumerMapping struct {
	currentMapping *PartitionSlotToConsumerInstanceList
	prevMappings   []*PartitionSlotToConsumerInstanceList
}

// Balance goal:
// 1. max processing power utilization
// 2. allow one consumer instance to be down unexpectedly
//    without affecting the processing power utilization

func (pcm *PartitionConsumerMapping) BalanceToConsumerInstances(partitionSlotToBrokerList *pub_balancer.PartitionSlotToBrokerList, consumerInstances []*ConsumerGroupInstance) {
	if len(partitionSlotToBrokerList.PartitionSlots) == 0 || len(consumerInstances) == 0 {
		return
	}
	newMapping := NewPartitionSlotToConsumerInstanceList(partitionSlotToBrokerList.RingSize, time.Now())
	var prevMapping *PartitionSlotToConsumerInstanceList
	if len(pcm.prevMappings) > 0 {
		prevMapping = pcm.prevMappings[len(pcm.prevMappings)-1]
	} else {
		prevMapping = nil
	}
	newMapping.PartitionSlots = doBalanceSticky(partitionSlotToBrokerList.PartitionSlots, consumerInstances, prevMapping)
	if pcm.currentMapping != nil {
		pcm.prevMappings = append(pcm.prevMappings, pcm.currentMapping)
		if len(pcm.prevMappings) > 10 {
			pcm.prevMappings = pcm.prevMappings[1:]
		}
	}
	pcm.currentMapping = newMapping
}

func doBalanceSticky(partitions []*pub_balancer.PartitionSlotToBroker, consumerInstances []*ConsumerGroupInstance, prevMapping *PartitionSlotToConsumerInstanceList) (partitionSlots []*PartitionSlotToConsumerInstance) {
	// collect previous consumer instance ids
	prevConsumerInstanceIds := make(map[ConsumerGroupInstanceId]struct{})
	if prevMapping != nil {
		for _, prevPartitionSlot := range prevMapping.PartitionSlots {
			if prevPartitionSlot.AssignedInstanceId != "" {
				prevConsumerInstanceIds[prevPartitionSlot.AssignedInstanceId] = struct{}{}
			}
		}
	}
	// collect current consumer instance ids
	currConsumerInstanceIds := make(map[ConsumerGroupInstanceId]struct{})
	for _, consumerInstance := range consumerInstances {
		currConsumerInstanceIds[consumerInstance.InstanceId] = struct{}{}
	}

	// check deleted consumer instances
	deletedConsumerInstanceIds := make(map[ConsumerGroupInstanceId]struct{})
	for consumerInstanceId := range prevConsumerInstanceIds {
		if _, ok := currConsumerInstanceIds[consumerInstanceId]; !ok {
			deletedConsumerInstanceIds[consumerInstanceId] = struct{}{}
		}
	}

	// convert partition slots from list to a map
	prevPartitionSlotMap := make(map[string]*PartitionSlotToConsumerInstance)
	if prevMapping != nil {
		for _, partitionSlot := range prevMapping.PartitionSlots {
			key := fmt.Sprintf("%d-%d", partitionSlot.RangeStart, partitionSlot.RangeStop)
			prevPartitionSlotMap[key] = partitionSlot
		}
	}

	// make a copy of old mapping, skipping the deleted consumer instances
	newPartitionSlots := make([]*PartitionSlotToConsumerInstance, 0, len(partitions))
	for _, partition := range partitions {
		newPartitionSlots = append(newPartitionSlots, &PartitionSlotToConsumerInstance{
			RangeStart:     partition.RangeStart,
			RangeStop:      partition.RangeStop,
			UnixTimeNs:     partition.UnixTimeNs,
			Broker:         partition.AssignedBroker,
			FollowerBroker: partition.FollowerBroker,
		})
	}
	for _, newPartitionSlot := range newPartitionSlots {
		key := fmt.Sprintf("%d-%d", newPartitionSlot.RangeStart, newPartitionSlot.RangeStop)
		if prevPartitionSlot, ok := prevPartitionSlotMap[key]; ok {
			if _, ok := deletedConsumerInstanceIds[prevPartitionSlot.AssignedInstanceId]; !ok {
				newPartitionSlot.AssignedInstanceId = prevPartitionSlot.AssignedInstanceId
			}
		}
	}

	// for all consumer instances, count the average number of partitions
	// that are assigned to them
	consumerInstancePartitionCount := make(map[ConsumerGroupInstanceId]int)
	for _, newPartitionSlot := range newPartitionSlots {
		if newPartitionSlot.AssignedInstanceId != "" {
			consumerInstancePartitionCount[newPartitionSlot.AssignedInstanceId]++
		}
	}
	// average number of partitions that are assigned to each consumer instance
	averageConsumerInstanceLoad := float32(len(partitions)) / float32(len(consumerInstances))

	// assign unassigned partition slots to consumer instances that is underloaded
	consumerInstanceIdsIndex := 0
	for _, newPartitionSlot := range newPartitionSlots {
		if newPartitionSlot.AssignedInstanceId == "" {
			for avoidDeadLoop := len(consumerInstances); avoidDeadLoop > 0; avoidDeadLoop-- {
				consumerInstance := consumerInstances[consumerInstanceIdsIndex]
				if float32(consumerInstancePartitionCount[consumerInstance.InstanceId]) < averageConsumerInstanceLoad {
					newPartitionSlot.AssignedInstanceId = consumerInstance.InstanceId
					consumerInstancePartitionCount[consumerInstance.InstanceId]++
					consumerInstanceIdsIndex++
					if consumerInstanceIdsIndex >= len(consumerInstances) {
						consumerInstanceIdsIndex = 0
					}
					break
				} else {
					consumerInstanceIdsIndex++
					if consumerInstanceIdsIndex >= len(consumerInstances) {
						consumerInstanceIdsIndex = 0
					}
				}
			}
		}
	}

	return newPartitionSlots
}
