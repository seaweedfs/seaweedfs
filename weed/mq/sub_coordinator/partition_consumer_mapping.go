package sub_coordinator

type PartitionConsumerMapping struct {
	currentMapping *PartitionSlotToConsumerInstanceList
	prevMappings   []*PartitionSlotToConsumerInstanceList
}

