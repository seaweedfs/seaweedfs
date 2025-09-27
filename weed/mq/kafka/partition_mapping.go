package kafka

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// PartitionMapper provides consistent Kafka partition to SeaweedMQ ring mapping
type PartitionMapper struct{}

// NewPartitionMapper creates a new partition mapper
func NewPartitionMapper() *PartitionMapper {
	return &PartitionMapper{}
}

// GetRangeSize returns the consistent range size for Kafka partition mapping
// This ensures all components use the same calculation
func (pm *PartitionMapper) GetRangeSize() int32 {
	// Use a fixed range size that divides evenly into MaxPartitionCount
	// 2520 / 32 = 78.75, which causes issues
	// Instead, use a range size that divides evenly: 2520 / 35 = 72
	// Or better yet, use a power-of-2 friendly division: 2520 / 40 = 63
	// For maximum compatibility, let's use 32 as the standard range size
	// and adjust the ring utilization accordingly
	return 32
}

// GetMaxKafkaPartitions returns the maximum number of Kafka partitions supported
func (pm *PartitionMapper) GetMaxKafkaPartitions() int32 {
	// With range size 32, we can support: 2520 / 32 = 78 Kafka partitions
	return int32(pub_balancer.MaxPartitionCount) / pm.GetRangeSize()
}

// MapKafkaPartitionToSMQRange maps a Kafka partition to SeaweedMQ ring range
func (pm *PartitionMapper) MapKafkaPartitionToSMQRange(kafkaPartition int32) (rangeStart, rangeStop int32) {
	rangeSize := pm.GetRangeSize()
	rangeStart = kafkaPartition * rangeSize
	rangeStop = rangeStart + rangeSize - 1
	return rangeStart, rangeStop
}

// CreateSMQPartition creates a SeaweedMQ partition from a Kafka partition
func (pm *PartitionMapper) CreateSMQPartition(kafkaPartition int32, unixTimeNs int64) *schema_pb.Partition {
	rangeStart, rangeStop := pm.MapKafkaPartitionToSMQRange(kafkaPartition)

	return &schema_pb.Partition{
		RingSize:   pub_balancer.MaxPartitionCount,
		RangeStart: rangeStart,
		RangeStop:  rangeStop,
		UnixTimeNs: unixTimeNs,
	}
}

// ExtractKafkaPartitionFromSMQRange extracts the Kafka partition from SeaweedMQ range
func (pm *PartitionMapper) ExtractKafkaPartitionFromSMQRange(rangeStart int32) int32 {
	rangeSize := pm.GetRangeSize()
	return rangeStart / rangeSize
}

// ValidateKafkaPartition validates that a Kafka partition is within supported range
func (pm *PartitionMapper) ValidateKafkaPartition(kafkaPartition int32) bool {
	return kafkaPartition >= 0 && kafkaPartition < pm.GetMaxKafkaPartitions()
}

// GetPartitionMappingInfo returns debug information about the partition mapping
func (pm *PartitionMapper) GetPartitionMappingInfo() map[string]interface{} {
	return map[string]interface{}{
		"ring_size":            pub_balancer.MaxPartitionCount,
		"range_size":           pm.GetRangeSize(),
		"max_kafka_partitions": pm.GetMaxKafkaPartitions(),
		"ring_utilization":     float64(pm.GetMaxKafkaPartitions()*pm.GetRangeSize()) / float64(pub_balancer.MaxPartitionCount),
	}
}

// Global instance for consistent usage across the codebase
var DefaultPartitionMapper = NewPartitionMapper()

// Convenience functions that use the default mapper
func MapKafkaPartitionToSMQRange(kafkaPartition int32) (rangeStart, rangeStop int32) {
	return DefaultPartitionMapper.MapKafkaPartitionToSMQRange(kafkaPartition)
}

func CreateSMQPartition(kafkaPartition int32, unixTimeNs int64) *schema_pb.Partition {
	return DefaultPartitionMapper.CreateSMQPartition(kafkaPartition, unixTimeNs)
}

func ExtractKafkaPartitionFromSMQRange(rangeStart int32) int32 {
	return DefaultPartitionMapper.ExtractKafkaPartitionFromSMQRange(rangeStart)
}

func ValidateKafkaPartition(kafkaPartition int32) bool {
	return DefaultPartitionMapper.ValidateKafkaPartition(kafkaPartition)
}

func GetRangeSize() int32 {
	return DefaultPartitionMapper.GetRangeSize()
}

func GetMaxKafkaPartitions() int32 {
	return DefaultPartitionMapper.GetMaxKafkaPartitions()
}
