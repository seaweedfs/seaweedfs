package topic

import "github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"

const PartitionCount = 4096

type Partition struct {
	RangeStart int32
	RangeStop  int32 // exclusive
	RingSize   int32
}

func (partition Partition) Equals(other Partition) bool {
	if partition.RangeStart != other.RangeStart {
		return false
	}
	if partition.RangeStop != other.RangeStop {
		return false
	}
	if partition.RingSize != other.RingSize {
		return false
	}
	return true
}

func FromPbPartition(partition *mq_pb.Partition) Partition {
	return Partition{
		RangeStart: partition.RangeStart,
		RangeStop:  partition.RangeStop,
		RingSize:   partition.RingSize,
	}
}
