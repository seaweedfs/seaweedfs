package topic

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"time"
)

const PartitionCount = 4096

type Partition struct {
	RangeStart int32
	RangeStop  int32 // exclusive
	RingSize   int32
	UnixTimeNs int64 // in nanoseconds
}

func NewPartition(rangeStart, rangeStop, ringSize int32, unixTimeNs int64) *Partition {
	return &Partition{
		RangeStart: rangeStart,
		RangeStop:  rangeStop,
		RingSize:   ringSize,
		UnixTimeNs: unixTimeNs,
	}
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
	if partition.UnixTimeNs != other.UnixTimeNs {
		return false
	}
	return true
}

func FromPbPartition(partition *schema_pb.Partition) Partition {
	return Partition{
		RangeStart: partition.RangeStart,
		RangeStop:  partition.RangeStop,
		RingSize:   partition.RingSize,
		UnixTimeNs: partition.UnixTimeNs,
	}
}

func SplitPartitions(targetCount int32, ts int64) []*Partition {
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
			UnixTimeNs: ts,
		})
	}
	return partitions
}

func (partition Partition) ToPbPartition() *schema_pb.Partition {
	return &schema_pb.Partition{
		RangeStart: partition.RangeStart,
		RangeStop:  partition.RangeStop,
		RingSize:   partition.RingSize,
		UnixTimeNs: partition.UnixTimeNs,
	}
}

func (partition Partition) Overlaps(partition2 Partition) bool {
	if partition.RangeStart >= partition2.RangeStop {
		return false
	}
	if partition.RangeStop <= partition2.RangeStart {
		return false
	}
	return true
}

func (partition Partition) String() string {
	return fmt.Sprintf("%04d-%04d", partition.RangeStart, partition.RangeStop)
}

func ParseTopicVersion(name string) (t time.Time, err error) {
	return time.Parse(PartitionGenerationFormat, name)
}

func ParsePartitionBoundary(name string) (start, stop int32) {
	_, err := fmt.Sscanf(name, "%04d-%04d", &start, &stop)
	if err != nil {
		return 0, 0
	}
	return start, stop
}

func PartitionDir(t Topic, p Partition) string {
	partitionGeneration := time.Unix(0, p.UnixTimeNs).UTC().Format(PartitionGenerationFormat)
	return fmt.Sprintf("%s/%s/%04d-%04d", t.Dir(), partitionGeneration, p.RangeStart, p.RangeStop)
}
