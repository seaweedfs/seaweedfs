package sub_coordinator

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
