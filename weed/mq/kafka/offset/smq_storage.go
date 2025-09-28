package offset

import (
	"context"
	"encoding/json"
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
// Uses a JSON payload to preserve Kafka offset metadata (offset, timestamp, size)
// Path: <topic-dir>/<partition-dir>/<consumerGroup>.offset
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

	// For persistent ledger, store each entry separately to allow loading all entries
	if key.ConsumerGroup == "__persistent_ledger__" {
		ledgerDir := fmt.Sprintf("%s/ledger", consumersDir)
		entryFileName := fmt.Sprintf("offset-%d.json", kafkaOffset)

		entry := OffsetEntry{
			KafkaOffset: kafkaOffset,
			Timestamp:   smqTimestamp,
			Size:        size,
		}

		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}

		return s.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer.SaveInsideFiler(client, ledgerDir, entryFileName, data)
		})
	}

	// For regular consumer groups, store just the latest offset
	offsetFileName := fmt.Sprintf("%s.offset", key.ConsumerGroup)

	entry := OffsetEntry{
		KafkaOffset: kafkaOffset,
		Timestamp:   smqTimestamp,
		Size:        size,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	return s.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, consumersDir, offsetFileName, data)
	})
}

// LoadConsumerOffsets loads ALL offset entries for a consumer group
// This is used for ledger persistence, so we need to return all entries, not just the latest
func (s *SMQOffsetStorage) LoadConsumerOffsets(key ConsumerOffsetKey) ([]OffsetEntry, error) {
	// For ledger persistence, we need to load all entries from the ledger directory
	// The current implementation only stores the latest entry, which breaks ledger rebuilding

	// Check if this is a persistent ledger request (consumer group = "__persistent_ledger__")
	if key.ConsumerGroup == "__persistent_ledger__" {
		return s.loadAllLedgerEntries(key)
	}

	// For regular consumer groups, return just the latest committed offset
	latest, err := s.getCommittedEntry(key)
	if err != nil || latest.KafkaOffset < 0 {
		return []OffsetEntry{}, nil
	}

	return []OffsetEntry{latest}, nil
}

// GetConsumerHighWaterMark returns the next offset after the committed offset
func (s *SMQOffsetStorage) GetConsumerHighWaterMark(key ConsumerOffsetKey) (int64, error) {
	entry, err := s.getCommittedEntry(key)
	if err != nil || entry.KafkaOffset < 0 {
		return 0, nil
	}

	return entry.KafkaOffset + 1, nil
}

// getCommittedEntry reads the committed offset entry from SMQ's filer location
func (s *SMQOffsetStorage) getCommittedEntry(key ConsumerOffsetKey) (OffsetEntry, error) {
	result := OffsetEntry{KafkaOffset: -1}

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

	err := s.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, consumersDir, offsetFileName)
		if err != nil {
			return err
		}

		// Try JSON format first
		var entry OffsetEntry
		if jsonErr := json.Unmarshal(data, &entry); jsonErr == nil {
			result = entry
			return nil
		}

		// Fallback to legacy 8-byte format (offset only)
		if len(data) == 8 {
			offset := int64(util.BytesToUint64(data))
			result = OffsetEntry{KafkaOffset: offset}
			return nil
		}

		return fmt.Errorf("invalid offset file format")
	})

	if err != nil {
		return OffsetEntry{KafkaOffset: -1}, err
	}

	return result, nil
}

// loadAllLedgerEntries loads all offset entries for a persistent ledger
func (s *SMQOffsetStorage) loadAllLedgerEntries(key ConsumerOffsetKey) ([]OffsetEntry, error) {
	t := topic.Topic{
		Namespace: "kafka",
		Name:      key.Topic,
	}

	consistentTimestamp := int64(0)
	smqPartition := kafka.CreateSMQPartition(key.Partition, consistentTimestamp)
	p := topic.Partition{
		RingSize:   smqPartition.RingSize,
		RangeStart: smqPartition.RangeStart,
		RangeStop:  smqPartition.RangeStop,
		UnixTimeNs: smqPartition.UnixTimeNs,
	}

	partitionDir := topic.PartitionDir(t, p)
	ledgerDir := fmt.Sprintf("%s/consumers/ledger", partitionDir)

	var entries []OffsetEntry

	err := s.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Use streaming list to get all entries in the ledger directory
		stream, err := client.ListEntries(context.TODO(), &filer_pb.ListEntriesRequest{
			Directory: ledgerDir,
		})
		if err != nil {
			// Directory doesn't exist yet, return empty
			return nil
		}

		// Read all entries from the stream
		for {
			resp, err := stream.Recv()
			if err != nil {
				if err.Error() == "EOF" {
					break // End of stream
				}
				return nil // Ignore other errors, return empty
			}

			entry := resp.Entry
			if !strings.HasPrefix(entry.Name, "offset-") || !strings.HasSuffix(entry.Name, ".json") {
				continue
			}

			data, readErr := filer.ReadInsideFiler(client, ledgerDir, entry.Name)
			if readErr != nil {
				continue // Skip corrupted files
			}

			var offsetEntry OffsetEntry
			if jsonErr := json.Unmarshal(data, &offsetEntry); jsonErr == nil {
				entries = append(entries, offsetEntry)
			}
		}

		return nil
	})

	if err != nil {
		return []OffsetEntry{}, nil
	}

	// Sort entries by KafkaOffset to ensure correct order
	// Use a simple bubble sort since we expect small numbers of entries
	for i := 0; i < len(entries)-1; i++ {
		for j := 0; j < len(entries)-i-1; j++ {
			if entries[j].KafkaOffset > entries[j+1].KafkaOffset {
				entries[j], entries[j+1] = entries[j+1], entries[j]
			}
		}
	}

	return entries, nil
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
