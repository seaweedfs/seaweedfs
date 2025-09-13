package offset

import (
	"fmt"
	"sync"
)

// LedgerStorage interface for consumer offset persistence
type LedgerStorage interface {
	SaveConsumerOffset(key ConsumerOffsetKey, kafkaOffset, smqTimestamp int64, size int32) error
	LoadConsumerOffsets(key ConsumerOffsetKey) ([]OffsetEntry, error)
	GetConsumerHighWaterMark(key ConsumerOffsetKey) (int64, error)
	Close() error

	// Legacy methods for backward compatibility
	SaveOffsetMapping(topicPartition string, kafkaOffset, smqTimestamp int64, size int32) error
	LoadOffsetMappings(topicPartition string) ([]OffsetEntry, error)
	GetHighWaterMark(topicPartition string) (int64, error)
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

// Legacy storage implementation using SeaweedMQ's ledgers
// This is kept for backward compatibility but should not be used for new deployments
type SeaweedMQStorage struct {
	ledgersMu sync.RWMutex
	ledgers   map[string]*Ledger // key: topic-partition OR ConsumerOffsetKey.String()
}

// NewSeaweedMQStorage creates a SeaweedMQ-compatible storage backend
func NewSeaweedMQStorage() *SeaweedMQStorage {
	return &SeaweedMQStorage{
		ledgers: make(map[string]*Ledger),
	}
}

func (s *SeaweedMQStorage) SaveConsumerOffset(key ConsumerOffsetKey, kafkaOffset, smqTimestamp int64, size int32) error {
	keyStr := key.String()
	return s.SaveOffsetMapping(keyStr, kafkaOffset, smqTimestamp, size)
}

func (s *SeaweedMQStorage) LoadConsumerOffsets(key ConsumerOffsetKey) ([]OffsetEntry, error) {
	keyStr := key.String()
	return s.LoadOffsetMappings(keyStr)
}

func (s *SeaweedMQStorage) GetConsumerHighWaterMark(key ConsumerOffsetKey) (int64, error) {
	keyStr := key.String()
	return s.GetHighWaterMark(keyStr)
}

func (s *SeaweedMQStorage) SaveOffsetMapping(topicPartition string, kafkaOffset, smqTimestamp int64, size int32) error {
	s.ledgersMu.Lock()
	defer s.ledgersMu.Unlock()

	ledger, exists := s.ledgers[topicPartition]
	if !exists {
		ledger = NewLedger()
		s.ledgers[topicPartition] = ledger
	}

	return ledger.AppendRecord(kafkaOffset, smqTimestamp, size)
}

func (s *SeaweedMQStorage) LoadOffsetMappings(topicPartition string) ([]OffsetEntry, error) {
	s.ledgersMu.RLock()
	defer s.ledgersMu.RUnlock()

	ledger, exists := s.ledgers[topicPartition]
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

func (s *SeaweedMQStorage) GetHighWaterMark(topicPartition string) (int64, error) {
	s.ledgersMu.RLock()
	defer s.ledgersMu.RUnlock()

	ledger, exists := s.ledgers[topicPartition]
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

// PersistentLedger wraps a Ledger with SeaweedMQ persistence
type PersistentLedger struct {
	Ledger         *Ledger
	TopicPartition string
	Storage        LedgerStorage
}

// NewPersistentLedger creates a new persistent ledger
func NewPersistentLedger(topicPartition string, storage LedgerStorage) *PersistentLedger {
	pl := &PersistentLedger{
		Ledger:         NewLedger(),
		TopicPartition: topicPartition,
		Storage:        storage,
	}

	// Load existing mappings
	if entries, err := storage.LoadOffsetMappings(topicPartition); err == nil {
		for _, entry := range entries {
			pl.Ledger.AppendRecord(entry.KafkaOffset, entry.Timestamp, entry.Size)
		}
	}

	return pl
}

// AddEntry adds an offset mapping and persists it
func (pl *PersistentLedger) AddEntry(kafkaOffset, smqTimestamp int64, size int32) error {
	// Add to memory ledger
	if err := pl.Ledger.AppendRecord(kafkaOffset, smqTimestamp, size); err != nil {
		return err
	}

	// Persist to storage
	return pl.Storage.SaveOffsetMapping(pl.TopicPartition, kafkaOffset, smqTimestamp, size)
}

// GetEntries returns all entries from the ledger
func (pl *PersistentLedger) GetEntries() []OffsetEntry {
	return pl.Ledger.GetEntries()
}

// AssignOffsets reserves a range of consecutive Kafka offsets
func (pl *PersistentLedger) AssignOffsets(count int64) int64 {
	return pl.Ledger.AssignOffsets(count)
}

// AppendRecord adds a record to the ledger (legacy compatibility method)
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
