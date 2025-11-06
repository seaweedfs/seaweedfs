package kafka

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// Convenience functions for partition mapping used by production code
// The full PartitionMapper implementation is in partition_mapping_test.go for testing

// MapKafkaPartitionToSMQRange maps a Kafka partition to SeaweedMQ ring range
func MapKafkaPartitionToSMQRange(kafkaPartition int32) (rangeStart, rangeStop int32) {
	// Use a range size that divides evenly into MaxPartitionCount (2520)
	// Range size 35 gives us exactly 72 Kafka partitions: 2520 / 35 = 72
	rangeSize := int32(35)
	rangeStart = kafkaPartition * rangeSize
	rangeStop = rangeStart + rangeSize - 1
	return rangeStart, rangeStop
}

// CreateSMQPartition creates a SeaweedMQ partition from a Kafka partition
func CreateSMQPartition(kafkaPartition int32, unixTimeNs int64) *schema_pb.Partition {
	rangeStart, rangeStop := MapKafkaPartitionToSMQRange(kafkaPartition)

	return &schema_pb.Partition{
		RingSize:   pub_balancer.MaxPartitionCount,
		RangeStart: rangeStart,
		RangeStop:  rangeStop,
		UnixTimeNs: unixTimeNs,
	}
}

// ExtractKafkaPartitionFromSMQRange extracts the Kafka partition from SeaweedMQ range
func ExtractKafkaPartitionFromSMQRange(rangeStart int32) int32 {
	rangeSize := int32(35)
	return rangeStart / rangeSize
}

// ValidateKafkaPartition validates that a Kafka partition is within supported range
func ValidateKafkaPartition(kafkaPartition int32) bool {
	maxPartitions := int32(pub_balancer.MaxPartitionCount) / 35 // 72 partitions
	return kafkaPartition >= 0 && kafkaPartition < maxPartitions
}

// GetRangeSize returns the range size used for partition mapping
func GetRangeSize() int32 {
	return 35
}

// GetMaxKafkaPartitions returns the maximum number of Kafka partitions supported
func GetMaxKafkaPartitions() int32 {
	return int32(pub_balancer.MaxPartitionCount) / 35 // 72 partitions
}
