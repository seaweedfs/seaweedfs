package pub_balancer

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

type PartitionSlotToBroker struct {
	RangeStart     int32
	RangeStop      int32
	AssignedBroker string
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

func (ps *PartitionSlotToBrokerList) AddBroker(partition *mq_pb.Partition, broker string) {
	for _, partitionSlot := range ps.PartitionSlots {
		if partitionSlot.RangeStart == partition.RangeStart && partitionSlot.RangeStop == partition.RangeStop {
			if partitionSlot.AssignedBroker == broker {
				return
			}
			if partitionSlot.AssignedBroker != "" {
				glog.V(0).Infof("partition %s broker change: %s => %s", partition, partitionSlot.AssignedBroker, broker)
			}
			partitionSlot.AssignedBroker = broker
			return
		}
	}
	ps.PartitionSlots = append(ps.PartitionSlots, &PartitionSlotToBroker{
		RangeStart:     partition.RangeStart,
		RangeStop:      partition.RangeStop,
		AssignedBroker: broker,
	})
}
func (ps *PartitionSlotToBrokerList) RemoveBroker(broker string) {
	for _, partitionSlot := range ps.PartitionSlots {
		if partitionSlot.AssignedBroker == broker {
			partitionSlot.AssignedBroker = ""
		}
	}
}
