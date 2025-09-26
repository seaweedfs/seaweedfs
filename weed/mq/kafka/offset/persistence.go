package offset

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// LedgerStorage interface for consumer offset persistence
type LedgerStorage interface {
	SaveConsumerOffset(key ConsumerOffsetKey, kafkaOffset, smqTimestamp int64, size int32) error
	LoadConsumerOffsets(key ConsumerOffsetKey) ([]OffsetEntry, error)
	GetConsumerHighWaterMark(key ConsumerOffsetKey) (int64, error)
	Close() error
}

// ConsumerOffsetKey represents the full key for consumer offsets
type ConsumerOffsetKey struct {
	Topic                 string `json:"topic"`
	Partition             int32  `json:"partition"`
	ConsumerGroup         string `json:"consumer_group"`
	ConsumerGroupInstance string `json:"consumer_group_instance,omitempty"` // Optional static membership ID
}

func (k ConsumerOffsetKey) String() string {
	if k.ConsumerGroupInstance != "" {
		return fmt.Sprintf("%s:%d:%s:%s", k.Topic, k.Partition, k.ConsumerGroup, k.ConsumerGroupInstance)
	}
	return fmt.Sprintf("%s:%d:%s", k.Topic, k.Partition, k.ConsumerGroup)
}

// OffsetEntry is already defined in ledger.go

// SeaweedMQ storage implementation using SeaweedMQ's ledgers
type SeaweedMQStorage struct {
	ledgersMu sync.RWMutex
	ledgers   map[string]*Ledger // key: ConsumerOffsetKey.String()
}

// NewSeaweedMQStorage creates a SeaweedMQ-compatible storage backend
func NewSeaweedMQStorage() *SeaweedMQStorage {
	return &SeaweedMQStorage{
		ledgers: make(map[string]*Ledger),
	}
}

func (s *SeaweedMQStorage) SaveConsumerOffset(key ConsumerOffsetKey, kafkaOffset, smqTimestamp int64, size int32) error {
	s.ledgersMu.Lock()
	defer s.ledgersMu.Unlock()

	keyStr := key.String()
	ledger, exists := s.ledgers[keyStr]
	if !exists {
		ledger = NewLedger()
		s.ledgers[keyStr] = ledger
	}

	return ledger.AppendRecord(kafkaOffset, smqTimestamp, size)
}

func (s *SeaweedMQStorage) LoadConsumerOffsets(key ConsumerOffsetKey) ([]OffsetEntry, error) {
	s.ledgersMu.RLock()
	defer s.ledgersMu.RUnlock()

	keyStr := key.String()
	ledger, exists := s.ledgers[keyStr]
	if !exists {
		return []OffsetEntry{}, nil
	}

	entries := ledger.GetEntries()
	result := make([]OffsetEntry, len(entries))
	for i, entry := range entries {
		result[i] = OffsetEntry{
			KafkaOffset: entry.KafkaOffset,
			Timestamp:   entry.Timestamp,
			Size:        entry.Size,
		}
	}

	return result, nil
}

func (s *SeaweedMQStorage) GetConsumerHighWaterMark(key ConsumerOffsetKey) (int64, error) {
	s.ledgersMu.RLock()
	defer s.ledgersMu.RUnlock()

	keyStr := key.String()
	ledger, exists := s.ledgers[keyStr]
	if !exists {
		return 0, nil
	}

	return ledger.GetHighWaterMark(), nil
}

func (s *SeaweedMQStorage) Close() error {
	s.ledgersMu.Lock()
	defer s.ledgersMu.Unlock()

	// Ledgers don't need explicit closing in this implementation
	s.ledgers = make(map[string]*Ledger)

	return nil
}

// parseTopicPartitionToConsumerKey parses a "topic-partition" string to ConsumerOffsetKey
func parseTopicPartitionToConsumerKey(topicPartition string) ConsumerOffsetKey {
	// Default parsing logic for "topic-partition" format
	// Find the last dash to separate topic from partition
	lastDash := strings.LastIndex(topicPartition, "-")
	if lastDash == -1 {
		// No dash found, assume entire string is topic with partition 0
		return ConsumerOffsetKey{
			Topic:         topicPartition,
			Partition:     0,
			ConsumerGroup: "__persistent_ledger__", // Special consumer group for PersistentLedger
		}
	}

	topic := topicPartition[:lastDash]
	partitionStr := topicPartition[lastDash+1:]

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		// If partition parsing fails, assume partition 0
		return ConsumerOffsetKey{
			Topic:         topicPartition,
			Partition:     0,
			ConsumerGroup: "__persistent_ledger__",
		}
	}

	return ConsumerOffsetKey{
		Topic:         topic,
		Partition:     int32(partition),
		ConsumerGroup: "__persistent_ledger__", // Special consumer group for PersistentLedger
	}
}

// PersistentLedger wraps a Ledger with SeaweedMQ persistence
type PersistentLedger struct {
	Ledger         *Ledger
	TopicPartition string
	ConsumerKey    ConsumerOffsetKey
	Storage        LedgerStorage
}

// NewPersistentLedger creates a new persistent ledger
func NewPersistentLedger(topicPartition string, storage LedgerStorage) *PersistentLedger {
	// Parse topicPartition string to extract topic and partition
	// Format: "topic-partition" (e.g., "my-topic-0")
	consumerKey := parseTopicPartitionToConsumerKey(topicPartition)

	pl := &PersistentLedger{
		Ledger:         NewLedger(),
		TopicPartition: topicPartition,
		ConsumerKey:    consumerKey,
		Storage:        storage,
	}

	pl.RebuildInMemoryLedger()

	return pl
}

// RebuildInMemoryLedger reloads entries from persistent storage into the in-memory ledger.
// This is safe to call multiple times; the underlying entries slice is reset first.
func (pl *PersistentLedger) RebuildInMemoryLedger() {
	pl.Ledger.mu.Lock()
	// Reset ledger state
	pl.Ledger.entries = pl.Ledger.entries[:0]
	pl.Ledger.nextOffset = 0
	pl.Ledger.earliestTime = 0
	pl.Ledger.latestTime = 0
	pl.Ledger.mu.Unlock()

	entries, err := pl.Storage.LoadConsumerOffsets(pl.ConsumerKey)
	if err != nil {
		return
	}

	pl.Ledger.mu.Lock()
	for _, entry := range entries {
		// Ensure entries are in order; skip invalid ones
		if entry.KafkaOffset < 0 {
			continue
		}
		// Append respecting sequential offsets
		if entry.KafkaOffset >= pl.Ledger.nextOffset {
			pl.Ledger.nextOffset = entry.KafkaOffset + 1
		}
		pl.Ledger.entries = append(pl.Ledger.entries, OffsetEntry{
			KafkaOffset: entry.KafkaOffset,
			Timestamp:   entry.Timestamp,
			Size:        entry.Size,
		})
		if pl.Ledger.earliestTime == 0 || entry.Timestamp < pl.Ledger.earliestTime {
			pl.Ledger.earliestTime = entry.Timestamp
		}
		if entry.Timestamp > pl.Ledger.latestTime {
			pl.Ledger.latestTime = entry.Timestamp
		}
	}
	pl.Ledger.mu.Unlock()
}

// AddEntry adds an offset mapping and persists it
func (pl *PersistentLedger) AddEntry(kafkaOffset, smqTimestamp int64, size int32) error {
	// Add to memory ledger
	if err := pl.Ledger.AppendRecord(kafkaOffset, smqTimestamp, size); err != nil {
		return err
	}

	// Persist to storage using new consumer offset method
	return pl.Storage.SaveConsumerOffset(pl.ConsumerKey, kafkaOffset, smqTimestamp, size)
}

// GetEntries returns all entries from the ledger
func (pl *PersistentLedger) GetEntries() []OffsetEntry {
	return pl.Ledger.GetEntries()
}

// AssignOffsets reserves a range of consecutive Kafka offsets
func (pl *PersistentLedger) AssignOffsets(count int64) int64 {
	return pl.Ledger.AssignOffsets(count)
}

// AppendRecord adds a record to the ledger (compatibility method)
func (pl *PersistentLedger) AppendRecord(kafkaOffset, timestamp int64, size int32) error {
	return pl.AddEntry(kafkaOffset, timestamp, size)
}

// GetHighWaterMark returns the next offset to be assigned
func (pl *PersistentLedger) GetHighWaterMark() int64 {
	return pl.Ledger.GetHighWaterMark()
}

// GetEarliestOffset returns the earliest offset in the ledger
func (pl *PersistentLedger) GetEarliestOffset() int64 {
	return pl.Ledger.GetEarliestOffset()
}

// GetLatestOffset returns the latest offset in the ledger
func (pl *PersistentLedger) GetLatestOffset() int64 {
	return pl.Ledger.GetLatestOffset()
}

// GetStats returns statistics about the ledger
func (pl *PersistentLedger) GetStats() (count int, earliestTime, latestTime int64) {
	count, _, earliestTime, latestTime = pl.Ledger.GetStats()
	return count, earliestTime, latestTime
}
