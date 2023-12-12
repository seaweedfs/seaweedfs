package sub_coordinator

import "github.com/seaweedfs/seaweedfs/weed/mq/topic"

type PartitionSlotToConsumerInstance struct {
	RangeStart         int32
	RangeStop          int32
	AssignedInstanceId string
}

type PartitionSlotToConsumerInstanceList struct {
	PartitionSlots []*PartitionSlotToConsumerInstance
	RingSize       int32
	Version        int64
}

func NewPartitionSlotToConsumerInstanceList(ringSize int32, version int64) *PartitionSlotToConsumerInstanceList {
	return &PartitionSlotToConsumerInstanceList{
		RingSize: ringSize,
		Version:  version,
	}
}

func ToPartitionSlots(partitions []*topic.Partition) (partitionSlots []*PartitionSlotToConsumerInstance) {
	for _, partition := range partitions {
		partitionSlots = append(partitionSlots, &PartitionSlotToConsumerInstance{
			RangeStart: partition.RangeStart,
			RangeStop:  partition.RangeStop,
		})
	}
	return
}
