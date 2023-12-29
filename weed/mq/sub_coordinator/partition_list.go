package sub_coordinator

import "github.com/seaweedfs/seaweedfs/weed/mq/topic"

type PartitionSlotToConsumerInstance struct {
	RangeStart         int32
	RangeStop          int32
	Broker             string
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

func ToPartitions(ringSize int32, slots []*PartitionSlotToConsumerInstance, unixTimeNs int64) []*topic.Partition {
	partitions := make([]*topic.Partition, 0, len(slots))
	for _, slot := range slots {
		partitions = append(partitions, topic.NewPartition(slot.RangeStart, slot.RangeStop, ringSize, unixTimeNs))
	}
	return partitions
}
