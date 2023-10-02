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

func SplitPartitions(targetCount int32) []*Partition {
	partitions := make([]*Partition, 0, targetCount)
	partitionSize := PartitionCount / targetCount
	for i := int32(0); i < targetCount; i++ {
		partitionStop := (i + 1) * partitionSize
		if i == targetCount-1 {
			partitionStop = PartitionCount
		}
		partitions = append(partitions, &Partition{
			RangeStart: i * partitionSize,
			RangeStop:  partitionStop,
			RingSize:   PartitionCount,
		})
	}
	return partitions
}
