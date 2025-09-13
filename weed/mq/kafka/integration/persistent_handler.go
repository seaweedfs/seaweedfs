package integration

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// PersistentKafkaHandler integrates Kafka protocol with persistent SMQ storage
type PersistentKafkaHandler struct {
	brokers []string

	// SMQ integration components
	publisher  *SMQPublisher
	subscriber *SMQSubscriber

	// Offset storage
	offsetStorage *offset.SMQOffsetStorage

	// Topic registry
	topicsMu sync.RWMutex
	topics   map[string]*TopicInfo

	// Ledgers for offset tracking (persistent)
	ledgersMu sync.RWMutex
	ledgers   map[string]*offset.PersistentLedger // key: topic-partition
}

// TopicInfo holds information about a Kafka topic
type TopicInfo struct {
	Name       string
	Partitions int32
	CreatedAt  int64
	RecordType *schema_pb.RecordType
}

// NewPersistentKafkaHandler creates a new handler with full SMQ integration
func NewPersistentKafkaHandler(brokers []string) (*PersistentKafkaHandler, error) {
	// Create SMQ publisher
	publisher, err := NewSMQPublisher(brokers)
	if err != nil {
		return nil, fmt.Errorf("failed to create SMQ publisher: %w", err)
	}

	// Create SMQ subscriber
	subscriber, err := NewSMQSubscriber(brokers)
	if err != nil {
		publisher.Close()
		return nil, fmt.Errorf("failed to create SMQ subscriber: %w", err)
	}

	// Create offset storage
	// Use first broker as filer address for offset storage
	filerAddress := brokers[0]
	offsetStorage, err := offset.NewSMQOffsetStorage(filerAddress)
	if err != nil {
		publisher.Close()
		subscriber.Close()
		return nil, fmt.Errorf("failed to create offset storage: %w", err)
	}

	return &PersistentKafkaHandler{
		brokers:       brokers,
		publisher:     publisher,
		subscriber:    subscriber,
		offsetStorage: offsetStorage,
		topics:        make(map[string]*TopicInfo),
		ledgers:       make(map[string]*offset.PersistentLedger),
	}, nil
}

// ProduceMessage handles Kafka produce requests with persistent offset tracking
func (h *PersistentKafkaHandler) ProduceMessage(
	topic string,
	partition int32,
	key []byte,
	value *schema_pb.RecordValue,
	recordType *schema_pb.RecordType,
) (int64, error) {

	// Ensure topic exists
	if err := h.ensureTopicExists(topic, recordType); err != nil {
		return -1, fmt.Errorf("failed to ensure topic exists: %w", err)
	}

	// Publish to SMQ with offset tracking
	kafkaOffset, err := h.publisher.PublishMessage(topic, partition, key, value, recordType)
	if err != nil {
		return -1, fmt.Errorf("failed to publish message: %w", err)
	}

	return kafkaOffset, nil
}

// FetchMessages handles Kafka fetch requests with SMQ subscription
func (h *PersistentKafkaHandler) FetchMessages(
	topic string,
	partition int32,
	fetchOffset int64,
	maxBytes int32,
	consumerGroup string,
) ([]*KafkaMessage, error) {

	// Fetch messages from SMQ subscriber
	messages, err := h.subscriber.FetchMessages(topic, partition, fetchOffset, maxBytes, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch messages: %w", err)
	}

	return messages, nil
}

// GetOrCreateLedger returns a persistent ledger for the topic-partition
func (h *PersistentKafkaHandler) GetOrCreateLedger(topic string, partition int32) (*offset.PersistentLedger, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	h.ledgersMu.RLock()
	if ledger, exists := h.ledgers[key]; exists {
		h.ledgersMu.RUnlock()
		return ledger, nil
	}
	h.ledgersMu.RUnlock()

	h.ledgersMu.Lock()
	defer h.ledgersMu.Unlock()

	// Double-check after acquiring write lock
	if ledger, exists := h.ledgers[key]; exists {
		return ledger, nil
	}

	// Create persistent ledger
	ledger := offset.NewPersistentLedger(key, h.offsetStorage)

	h.ledgers[key] = ledger
	return ledger, nil
}

// GetLedger returns the ledger for a topic-partition (may be nil)
func (h *PersistentKafkaHandler) GetLedger(topic string, partition int32) *offset.PersistentLedger {
	key := fmt.Sprintf("%s-%d", topic, partition)

	h.ledgersMu.RLock()
	defer h.ledgersMu.RUnlock()

	return h.ledgers[key]
}

// CreateTopic creates a new Kafka topic
func (h *PersistentKafkaHandler) CreateTopic(name string, partitions int32, recordType *schema_pb.RecordType) error {
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()

	if _, exists := h.topics[name]; exists {
		return nil // Topic already exists
	}

	h.topics[name] = &TopicInfo{
		Name:       name,
		Partitions: partitions,
		CreatedAt:  getCurrentTimeNanos(),
		RecordType: recordType,
	}

	return nil
}

// TopicExists checks if a topic exists
func (h *PersistentKafkaHandler) TopicExists(name string) bool {
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()

	_, exists := h.topics[name]
	return exists
}

// GetTopicInfo returns information about a topic
func (h *PersistentKafkaHandler) GetTopicInfo(name string) *TopicInfo {
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()

	return h.topics[name]
}

// ListTopics returns all topic names
func (h *PersistentKafkaHandler) ListTopics() []string {
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()

	topics := make([]string, 0, len(h.topics))
	for name := range h.topics {
		topics = append(topics, name)
	}
	return topics
}

// GetHighWaterMark returns the high water mark for a topic-partition
func (h *PersistentKafkaHandler) GetHighWaterMark(topic string, partition int32) (int64, error) {
	ledger, err := h.GetOrCreateLedger(topic, partition)
	if err != nil {
		return 0, err
	}
	return ledger.GetHighWaterMark(), nil
}

// GetEarliestOffset returns the earliest offset for a topic-partition
func (h *PersistentKafkaHandler) GetEarliestOffset(topic string, partition int32) (int64, error) {
	ledger, err := h.GetOrCreateLedger(topic, partition)
	if err != nil {
		return 0, err
	}
	return ledger.GetEarliestOffset(), nil
}

// GetLatestOffset returns the latest offset for a topic-partition
func (h *PersistentKafkaHandler) GetLatestOffset(topic string, partition int32) (int64, error) {
	ledger, err := h.GetOrCreateLedger(topic, partition)
	if err != nil {
		return 0, err
	}
	return ledger.GetLatestOffset(), nil
}

// CommitOffset commits a consumer group offset
func (h *PersistentKafkaHandler) CommitOffset(
	topic string,
	partition int32,
	offset int64,
	consumerGroup string,
) error {
	return h.subscriber.CommitOffset(topic, partition, offset, consumerGroup)
}

// FetchOffset retrieves a committed consumer group offset
func (h *PersistentKafkaHandler) FetchOffset(
	topic string,
	partition int32,
	consumerGroup string,
) (int64, error) {
	// For now, return -1 (no committed offset)
	// In a full implementation, this would query SMQ for the committed offset
	return -1, nil
}

// GetStats returns comprehensive statistics about the handler
func (h *PersistentKafkaHandler) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})

	// Topic stats
	h.topicsMu.RLock()
	topicStats := make(map[string]interface{})
	for name, info := range h.topics {
		topicStats[name] = map[string]interface{}{
			"partitions": info.Partitions,
			"created_at": info.CreatedAt,
		}
	}
	h.topicsMu.RUnlock()

	stats["topics"] = topicStats
	stats["topic_count"] = len(topicStats)

	// Ledger stats
	h.ledgersMu.RLock()
	ledgerStats := make(map[string]interface{})
	for key, ledger := range h.ledgers {
		entryCount, earliestTime, latestTime := ledger.GetStats()
		nextOffset := ledger.GetHighWaterMark()
		ledgerStats[key] = map[string]interface{}{
			"entry_count":     entryCount,
			"earliest_time":   earliestTime,
			"latest_time":     latestTime,
			"next_offset":     nextOffset,
			"high_water_mark": ledger.GetHighWaterMark(),
		}
	}
	h.ledgersMu.RUnlock()

	stats["ledgers"] = ledgerStats
	stats["ledger_count"] = len(ledgerStats)

	return stats
}

// Close shuts down the handler and all connections
func (h *PersistentKafkaHandler) Close() error {
	var lastErr error

	if err := h.publisher.Close(); err != nil {
		lastErr = err
	}

	if err := h.subscriber.Close(); err != nil {
		lastErr = err
	}

	if err := h.offsetStorage.Close(); err != nil {
		lastErr = err
	}

	return lastErr
}

// ensureTopicExists creates a topic if it doesn't exist
func (h *PersistentKafkaHandler) ensureTopicExists(name string, recordType *schema_pb.RecordType) error {
	if h.TopicExists(name) {
		return nil
	}

	return h.CreateTopic(name, 1, recordType) // Default to 1 partition
}

// getCurrentTimeNanos returns current time in nanoseconds
func getCurrentTimeNanos() int64 {
	return time.Now().UnixNano()
}

// RestoreAllLedgers restores all ledgers from persistent storage on startup
func (h *PersistentKafkaHandler) RestoreAllLedgers() error {
	// This would scan SMQ for all topic-partitions and restore their ledgers
	// For now, ledgers are created on-demand
	return nil
}
