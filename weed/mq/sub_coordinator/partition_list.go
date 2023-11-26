package sub_coordinator

import "github.com/seaweedfs/seaweedfs/weed/mq/topic"

type PartitionSlot struct {
	RangeStart         int32
	RangeStop          int32
	AssignedInstanceId string
}

type PartitionSlotList struct {
	PartitionSlots []*PartitionSlot
	RingSize       int32
	Version        int64
}

func NewPartitionSlotList(ringSize int32, version int64) *PartitionSlotList {
	return &PartitionSlotList{
		RingSize: ringSize,
		Version:  version,
	}
}

func ToPartitionSlots(partitions []*topic.Partition) (partitionSlots []*PartitionSlot) {
	for _, partition := range partitions {
		partitionSlots = append(partitionSlots, &PartitionSlot{
			RangeStart: partition.RangeStart,
			RangeStop:  partition.RangeStop,
		})
	}
	return
}
