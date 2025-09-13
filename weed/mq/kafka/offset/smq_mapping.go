package offset

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// KafkaToSMQMapper handles the conversion between Kafka offsets and SMQ PartitionOffset
type KafkaToSMQMapper struct {
	ledger *Ledger
}

// NewKafkaToSMQMapper creates a new mapper with the given ledger
func NewKafkaToSMQMapper(ledger *Ledger) *KafkaToSMQMapper {
	return &KafkaToSMQMapper{
		ledger: ledger,
	}
}

// KafkaOffsetToSMQPartitionOffset converts a Kafka offset to SMQ PartitionOffset
// This is the core mapping function that bridges Kafka and SMQ semantics
func (m *KafkaToSMQMapper) KafkaOffsetToSMQPartitionOffset(
	kafkaOffset int64,
	topic string,
	kafkaPartition int32,
) (*schema_pb.PartitionOffset, error) {

	// Step 1: Look up the SMQ timestamp for this Kafka offset
	smqTimestamp, _, err := m.ledger.GetRecord(kafkaOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to find SMQ timestamp for Kafka offset %d: %w", kafkaOffset, err)
	}

	// Step 2: Create SMQ Partition
	// SMQ uses a ring-based partitioning scheme
	rangeSize := int32(pub_balancer.MaxPartitionCount / 32) // Calculate dynamic range size
	smqPartition := &schema_pb.Partition{
		RingSize:   pub_balancer.MaxPartitionCount,          // Standard ring size
		RangeStart: int32(kafkaPartition) * rangeSize,       // Map Kafka partition to ring range
		RangeStop:  (int32(kafkaPartition)+1)*rangeSize - 1, // Each Kafka partition gets calculated slots
		UnixTimeNs: smqTimestamp,                            // When this partition mapping was created
	}

	// Step 3: Create PartitionOffset with the mapped timestamp
	partitionOffset := &schema_pb.PartitionOffset{
		Partition: smqPartition,
		StartTsNs: smqTimestamp, // This is the key mapping: Kafka offset → SMQ timestamp
	}

	return partitionOffset, nil
}

// SMQPartitionOffsetToKafkaOffset converts SMQ PartitionOffset back to Kafka offset
// This is used during Fetch operations to convert SMQ data back to Kafka semantics
func (m *KafkaToSMQMapper) SMQPartitionOffsetToKafkaOffset(
	partitionOffset *schema_pb.PartitionOffset,
) (int64, error) {

	smqTimestamp := partitionOffset.StartTsNs

	// Binary search through the ledger to find the Kafka offset for this timestamp
	entries := m.ledger.entries
	for _, entry := range entries {
		if entry.Timestamp == smqTimestamp {
			return entry.KafkaOffset, nil
		}
	}

	return -1, fmt.Errorf("no Kafka offset found for SMQ timestamp %d", smqTimestamp)
}

// CreateSMQSubscriptionRequest creates a proper SMQ subscription request for a Kafka fetch
func (m *KafkaToSMQMapper) CreateSMQSubscriptionRequest(
	topic string,
	kafkaPartition int32,
	startKafkaOffset int64,
	consumerGroup string,
) (*schema_pb.PartitionOffset, schema_pb.OffsetType, error) {

	var startTimestamp int64
	var offsetType schema_pb.OffsetType

	// Handle special Kafka offset values
	switch startKafkaOffset {
	case -2: // EARLIEST
		startTimestamp = m.ledger.earliestTime
		offsetType = schema_pb.OffsetType_RESET_TO_EARLIEST

	case -1: // LATEST
		startTimestamp = m.ledger.latestTime
		offsetType = schema_pb.OffsetType_RESET_TO_LATEST

	default: // Specific offset
		if startKafkaOffset < 0 {
			return nil, 0, fmt.Errorf("invalid Kafka offset: %d", startKafkaOffset)
		}

		// Look up the SMQ timestamp for this Kafka offset
		timestamp, _, err := m.ledger.GetRecord(startKafkaOffset)
		if err != nil {
			// If exact offset not found, use the next available timestamp
			if startKafkaOffset >= m.ledger.GetHighWaterMark() {
				startTimestamp = time.Now().UnixNano() // Start from now for future messages
				offsetType = schema_pb.OffsetType_EXACT_TS_NS
			} else {
				return nil, 0, fmt.Errorf("Kafka offset %d not found in ledger", startKafkaOffset)
			}
		} else {
			startTimestamp = timestamp
			offsetType = schema_pb.OffsetType_EXACT_TS_NS
		}
	}

	// Create SMQ partition mapping
	rangeSize := int32(pub_balancer.MaxPartitionCount / 32) // Calculate dynamic range size
	smqPartition := &schema_pb.Partition{
		RingSize:   pub_balancer.MaxPartitionCount,
		RangeStart: int32(kafkaPartition) * rangeSize,
		RangeStop:  (int32(kafkaPartition)+1)*rangeSize - 1,
		UnixTimeNs: time.Now().UnixNano(),
	}

	partitionOffset := &schema_pb.PartitionOffset{
		Partition: smqPartition,
		StartTsNs: startTimestamp,
	}

	return partitionOffset, offsetType, nil
}

// ExtractKafkaPartitionFromSMQPartition extracts the Kafka partition number from SMQ Partition
func ExtractKafkaPartitionFromSMQPartition(smqPartition *schema_pb.Partition) int32 {
	// Reverse the mapping: SMQ range → Kafka partition
	rangeSize := int32(pub_balancer.MaxPartitionCount / 32)
	return smqPartition.RangeStart / rangeSize
}

// OffsetMappingInfo provides debugging information about the mapping
type OffsetMappingInfo struct {
	KafkaOffset    int64
	SMQTimestamp   int64
	KafkaPartition int32
	SMQRangeStart  int32
	SMQRangeStop   int32
	MessageSize    int32
}

// GetMappingInfo returns detailed mapping information for debugging
func (m *KafkaToSMQMapper) GetMappingInfo(kafkaOffset int64, kafkaPartition int32) (*OffsetMappingInfo, error) {
	timestamp, size, err := m.ledger.GetRecord(kafkaOffset)
	if err != nil {
		return nil, err
	}

	return &OffsetMappingInfo{
		KafkaOffset:    kafkaOffset,
		SMQTimestamp:   timestamp,
		KafkaPartition: kafkaPartition,
		SMQRangeStart:  kafkaPartition * int32(pub_balancer.MaxPartitionCount/32),
		SMQRangeStop:   (kafkaPartition+1)*int32(pub_balancer.MaxPartitionCount/32) - 1,
		MessageSize:    size,
	}, nil
}

// ValidateMapping checks if the Kafka-SMQ mapping is consistent
func (m *KafkaToSMQMapper) ValidateMapping(topic string, kafkaPartition int32) error {
	// Check that offsets are sequential
	entries := m.ledger.entries
	for i := 1; i < len(entries); i++ {
		if entries[i].KafkaOffset != entries[i-1].KafkaOffset+1 {
			return fmt.Errorf("non-sequential Kafka offsets: %d -> %d",
				entries[i-1].KafkaOffset, entries[i].KafkaOffset)
		}
	}

	// Check that timestamps are monotonically increasing
	for i := 1; i < len(entries); i++ {
		if entries[i].Timestamp <= entries[i-1].Timestamp {
			return fmt.Errorf("non-monotonic SMQ timestamps: %d -> %d",
				entries[i-1].Timestamp, entries[i].Timestamp)
		}
	}

	return nil
}

// GetOffsetRange returns the Kafka offset range for a given SMQ time range
func (m *KafkaToSMQMapper) GetOffsetRange(startTime, endTime int64) (startOffset, endOffset int64, err error) {
	startOffset = -1
	endOffset = -1

	entries := m.ledger.entries
	for _, entry := range entries {
		if entry.Timestamp >= startTime && startOffset == -1 {
			startOffset = entry.KafkaOffset
		}
		if entry.Timestamp <= endTime {
			endOffset = entry.KafkaOffset
		}
	}

	if startOffset == -1 {
		return 0, 0, fmt.Errorf("no offsets found in time range [%d, %d]", startTime, endTime)
	}

	return startOffset, endOffset, nil
}

// CreatePartitionOffsetForTimeRange creates a PartitionOffset for a specific time range
func (m *KafkaToSMQMapper) CreatePartitionOffsetForTimeRange(
	kafkaPartition int32,
	startTime int64,
) *schema_pb.PartitionOffset {

	rangeSize := int32(pub_balancer.MaxPartitionCount / 32) // Calculate dynamic range size
	smqPartition := &schema_pb.Partition{
		RingSize:   pub_balancer.MaxPartitionCount,
		RangeStart: kafkaPartition * rangeSize,
		RangeStop:  (kafkaPartition+1)*rangeSize - 1,
		UnixTimeNs: time.Now().UnixNano(),
	}

	return &schema_pb.PartitionOffset{
		Partition: smqPartition,
		StartTsNs: startTime,
	}
}
