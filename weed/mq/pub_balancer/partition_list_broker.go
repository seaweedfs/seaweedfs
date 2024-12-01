package pub_balancer

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

type PartitionSlotToBroker struct {
	RangeStart     int32
	RangeStop      int32
	UnixTimeNs     int64
	AssignedBroker string
	FollowerBroker string
}

type PartitionSlotToBrokerList struct {
	PartitionSlots []*PartitionSlotToBroker
	RingSize       int32
}

func NewPartitionSlotToBrokerList(ringSize int32) *PartitionSlotToBrokerList {
	return &PartitionSlotToBrokerList{
		RingSize: ringSize,
	}
}

func (ps *PartitionSlotToBrokerList) AddBroker(partition *schema_pb.Partition, broker string, follower string) {
	for _, partitionSlot := range ps.PartitionSlots {
		if partitionSlot.RangeStart == partition.RangeStart && partitionSlot.RangeStop == partition.RangeStop {
			if partitionSlot.AssignedBroker != "" && partitionSlot.AssignedBroker != broker {
				glog.V(0).Infof("partition %s broker change: %s => %s", partition, partitionSlot.AssignedBroker, broker)
				partitionSlot.AssignedBroker = broker
			}
			if partitionSlot.FollowerBroker != "" && partitionSlot.FollowerBroker != follower {
				glog.V(0).Infof("partition %s follower change: %s => %s", partition, partitionSlot.FollowerBroker, follower)
				partitionSlot.FollowerBroker = follower
			}

			return
		}
	}
	ps.PartitionSlots = append(ps.PartitionSlots, &PartitionSlotToBroker{
		RangeStart:     partition.RangeStart,
		RangeStop:      partition.RangeStop,
		UnixTimeNs:     partition.UnixTimeNs,
		AssignedBroker: broker,
		FollowerBroker: follower,
	})
}
func (ps *PartitionSlotToBrokerList) RemoveBroker(broker string) {
	ps.ReplaceBroker(broker, "")
}

func (ps *PartitionSlotToBrokerList) ReplaceBroker(oldBroker string, newBroker string) {
	for _, partitionSlot := range ps.PartitionSlots {
		if partitionSlot.AssignedBroker == oldBroker {
			partitionSlot.AssignedBroker = newBroker
		}
	}
}
