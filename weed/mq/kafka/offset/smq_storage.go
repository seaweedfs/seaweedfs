package offset

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// SMQOffsetStorage implements LedgerStorage using SMQ's native offset persistence
// This reuses the same filer locations and file format that SMQ brokers use
type SMQOffsetStorage struct {
	filerClientAccessor *filer_client.FilerClientAccessor
}

// NewSMQOffsetStorage creates a storage backend that uses SMQ's native offset files
func NewSMQOffsetStorage(filerClientAccessor *filer_client.FilerClientAccessor) *SMQOffsetStorage {
	return &SMQOffsetStorage{
		filerClientAccessor: filerClientAccessor,
	}
}

// SaveConsumerOffset saves the committed offset for a consumer group
// Uses the same file format and location as SMQ brokers:
// Path: <topic-dir>/<partition-dir>/<consumerGroup>.offset
// Content: 8-byte big-endian offset
func (s *SMQOffsetStorage) SaveConsumerOffset(key ConsumerOffsetKey, kafkaOffset, smqTimestamp int64, size int32) error {
	t := topic.Topic{
		Namespace: "kafka", // Use kafka namespace for Kafka topics
		Name:      key.Topic,
	}

	// Use consistent timestamp for consumer offset operations to ensure same partition path
	// Hash the consumer group name to get a consistent timestamp for this group's offset storage
	consistentTimestamp := int64(0) // Use epoch time for consistent consumer offset paths
	smqPartition := kafka.CreateSMQPartition(key.Partition, consistentTimestamp)
	p := topic.Partition{
		RingSize:   smqPartition.RingSize,
		RangeStart: smqPartition.RangeStart,
		RangeStop:  smqPartition.RangeStop,
		UnixTimeNs: smqPartition.UnixTimeNs,
	}

	partitionDir := topic.PartitionDir(t, p)
	consumersDir := fmt.Sprintf("%s/consumers", partitionDir)
	offsetFileName := fmt.Sprintf("%s.offset", key.ConsumerGroup)

	// Use SMQ's 8-byte offset format
	offsetBytes := make([]byte, 8)
	util.Uint64toBytes(offsetBytes, uint64(kafkaOffset))

	return s.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, consumersDir, offsetFileName, offsetBytes)
	})
}

// LoadConsumerOffsets loads the committed offset for a consumer group
// Returns empty slice since we only track the committed offset, not the mapping history
func (s *SMQOffsetStorage) LoadConsumerOffsets(key ConsumerOffsetKey) ([]OffsetEntry, error) {
	offset, err := s.getCommittedOffset(key)
	if err != nil {
		return []OffsetEntry{}, nil // No committed offset found
	}

	if offset < 0 {
		return []OffsetEntry{}, nil // No valid offset
	}

	// Return single entry representing the committed offset
	return []OffsetEntry{
		{
			KafkaOffset: offset,
			Timestamp:   0, // SMQ doesn't store timestamp mapping
			Size:        0, // SMQ doesn't store size mapping
		},
	}, nil
}

// GetConsumerHighWaterMark returns the next offset after the committed offset
func (s *SMQOffsetStorage) GetConsumerHighWaterMark(key ConsumerOffsetKey) (int64, error) {
	offset, err := s.getCommittedOffset(key)
	if err != nil {
		return 0, nil // Start from beginning if no committed offset
	}

	if offset < 0 {
		return 0, nil // Start from beginning
	}

	return offset + 1, nil // Next offset after committed
}

// getCommittedOffset reads the committed offset from SMQ's filer location
func (s *SMQOffsetStorage) getCommittedOffset(key ConsumerOffsetKey) (int64, error) {
	t := topic.Topic{
		Namespace: "kafka",
		Name:      key.Topic,
	}

	// Use consistent timestamp for consumer offset operations to ensure same partition path
	// Hash the consumer group name to get a consistent timestamp for this group's offset storage
	consistentTimestamp := int64(0) // Use epoch time for consistent consumer offset paths
	smqPartition := kafka.CreateSMQPartition(key.Partition, consistentTimestamp)
	p := topic.Partition{
		RingSize:   smqPartition.RingSize,
		RangeStart: smqPartition.RangeStart,
		RangeStop:  smqPartition.RangeStop,
		UnixTimeNs: smqPartition.UnixTimeNs,
	}

	partitionDir := topic.PartitionDir(t, p)
	consumersDir := fmt.Sprintf("%s/consumers", partitionDir)
	offsetFileName := fmt.Sprintf("%s.offset", key.ConsumerGroup)

	var offset int64 = -1
	err := s.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, consumersDir, offsetFileName)
		if err != nil {
			return err
		}
		if len(data) != 8 {
			return fmt.Errorf("invalid offset file format")
		}
		offset = int64(util.BytesToUint64(data))
		return nil
	})

	if err != nil {
		return -1, err
	}

	return offset, nil
}

// Legacy methods for backward compatibility

func (s *SMQOffsetStorage) SaveOffsetMapping(topicPartition string, kafkaOffset, smqTimestamp int64, size int32) error {
	key, err := parseTopicPartitionKey(topicPartition)
	if err != nil {
		return err
	}
	return s.SaveConsumerOffset(key, kafkaOffset, smqTimestamp, size)
}

func (s *SMQOffsetStorage) LoadOffsetMappings(topicPartition string) ([]OffsetEntry, error) {
	key, err := parseTopicPartitionKey(topicPartition)
	if err != nil {
		return nil, err
	}
	return s.LoadConsumerOffsets(key)
}

func (s *SMQOffsetStorage) GetHighWaterMark(topicPartition string) (int64, error) {
	key, err := parseTopicPartitionKey(topicPartition)
	if err != nil {
		return 0, err
	}
	return s.GetConsumerHighWaterMark(key)
}

// Close is a no-op for SMQ storage
func (s *SMQOffsetStorage) Close() error {
	return nil
}

// parseTopicPartitionKey parses legacy "topic:partition" format into ConsumerOffsetKey
func parseTopicPartitionKey(topicPartition string) (ConsumerOffsetKey, error) {
	lastColonIndex := strings.LastIndex(topicPartition, ":")
	if lastColonIndex <= 0 || lastColonIndex == len(topicPartition)-1 {
		return ConsumerOffsetKey{}, fmt.Errorf("invalid legacy format: expected 'topic:partition', got '%s'", topicPartition)
	}

	topic := topicPartition[:lastColonIndex]
	if topic == "" {
		return ConsumerOffsetKey{}, fmt.Errorf("empty topic in legacy format: '%s'", topicPartition)
	}

	partitionStr := topicPartition[lastColonIndex+1:]
	partition, err := strconv.ParseInt(partitionStr, 10, 32)
	if err != nil {
		return ConsumerOffsetKey{}, fmt.Errorf("invalid partition number in legacy format '%s': %w", topicPartition, err)
	}

	if partition < 0 {
		return ConsumerOffsetKey{}, fmt.Errorf("negative partition number in legacy format: %d", partition)
	}

	// Return a ConsumerOffsetKey with empty group ID since legacy format doesn't include it
	return ConsumerOffsetKey{
		ConsumerGroup: "", // Legacy format doesn't specify group
		Topic:         topic,
		Partition:     int32(partition),
	}, nil
}
