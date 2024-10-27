package sub_coordinator

import "time"

type PartitionSlotToConsumerInstance struct {
	RangeStart         int32
	RangeStop          int32
	UnixTimeNs         int64
	Broker             string
	AssignedInstanceId ConsumerGroupInstanceId
	FollowerBroker     string
}

type PartitionSlotToConsumerInstanceList struct {
	PartitionSlots []*PartitionSlotToConsumerInstance
	RingSize       int32
	Version        int64
}

func NewPartitionSlotToConsumerInstanceList(ringSize int32, version time.Time) *PartitionSlotToConsumerInstanceList {
	return &PartitionSlotToConsumerInstanceList{
		RingSize: ringSize,
		Version:  version.UnixNano(),
	}
}
