package integration

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// SeaweedMQHandler integrates Kafka protocol handlers with real SeaweedMQ storage
type SeaweedMQHandler struct {
	agentClient *AgentClient

	// Topic registry - still keep track of Kafka topics
	topicsMu sync.RWMutex
	topics   map[string]*KafkaTopicInfo

	// Offset ledgers for Kafka offset translation
	ledgersMu sync.RWMutex
	ledgers   map[TopicPartitionKey]*offset.Ledger
}

// KafkaTopicInfo holds Kafka-specific topic information
type KafkaTopicInfo struct {
	Name       string
	Partitions int32
	CreatedAt  int64

	// SeaweedMQ integration
	SeaweedTopic *schema_pb.Topic
}

// TopicPartitionKey uniquely identifies a topic partition
type TopicPartitionKey struct {
	Topic     string
	Partition int32
}

// NewSeaweedMQHandler creates a new handler with SeaweedMQ integration
func NewSeaweedMQHandler(agentAddress string) (*SeaweedMQHandler, error) {
	agentClient, err := NewAgentClient(agentAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to create agent client: %v", err)
	}

	// Test the connection
	if err := agentClient.HealthCheck(); err != nil {
		agentClient.Close()
		return nil, fmt.Errorf("agent health check failed: %v", err)
	}

	return &SeaweedMQHandler{
		agentClient: agentClient,
		topics:      make(map[string]*KafkaTopicInfo),
		ledgers:     make(map[TopicPartitionKey]*offset.Ledger),
	}, nil
}

// Close shuts down the handler and all connections
func (h *SeaweedMQHandler) Close() error {
	return h.agentClient.Close()
}

// CreateTopic creates a new topic in both Kafka registry and SeaweedMQ
func (h *SeaweedMQHandler) CreateTopic(name string, partitions int32) error {
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()

	// Check if topic already exists
	if _, exists := h.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	// Create SeaweedMQ topic reference
	seaweedTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      name,
	}

	// Create Kafka topic info
	topicInfo := &KafkaTopicInfo{
		Name:         name,
		Partitions:   partitions,
		CreatedAt:    time.Now().UnixNano(),
		SeaweedTopic: seaweedTopic,
	}

	// Store in registry
	h.topics[name] = topicInfo

	// Initialize offset ledgers for all partitions
	for partitionID := int32(0); partitionID < partitions; partitionID++ {
		key := TopicPartitionKey{Topic: name, Partition: partitionID}
		h.ledgersMu.Lock()
		h.ledgers[key] = offset.NewLedger()
		h.ledgersMu.Unlock()
	}

	return nil
}

// DeleteTopic removes a topic from both Kafka registry and SeaweedMQ
func (h *SeaweedMQHandler) DeleteTopic(name string) error {
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()

	topicInfo, exists := h.topics[name]
	if !exists {
		return fmt.Errorf("topic %s does not exist", name)
	}

	// Close all publisher sessions for this topic
	for partitionID := int32(0); partitionID < topicInfo.Partitions; partitionID++ {
		h.agentClient.ClosePublisher(name, partitionID)
	}

	// Remove from registry
	delete(h.topics, name)

	// Clean up offset ledgers
	h.ledgersMu.Lock()
	for partitionID := int32(0); partitionID < topicInfo.Partitions; partitionID++ {
		key := TopicPartitionKey{Topic: name, Partition: partitionID}
		delete(h.ledgers, key)
	}
	h.ledgersMu.Unlock()

	return nil
}

// TopicExists checks if a topic exists
func (h *SeaweedMQHandler) TopicExists(name string) bool {
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()

	_, exists := h.topics[name]
	return exists
}

// GetTopicInfo returns information about a topic
func (h *SeaweedMQHandler) GetTopicInfo(name string) (*KafkaTopicInfo, bool) {
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()

	info, exists := h.topics[name]
	return info, exists
}

// ListTopics returns all topic names
func (h *SeaweedMQHandler) ListTopics() []string {
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()

	topics := make([]string, 0, len(h.topics))
	for name := range h.topics {
		topics = append(topics, name)
	}
	return topics
}

// ProduceRecord publishes a record to SeaweedMQ and updates Kafka offset tracking
func (h *SeaweedMQHandler) ProduceRecord(topic string, partition int32, key []byte, value []byte) (int64, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}

	// Get current timestamp
	timestamp := time.Now().UnixNano()

	// Publish to SeaweedMQ
	_, err := h.agentClient.PublishRecord(topic, partition, key, value, timestamp)
	if err != nil {
		return 0, fmt.Errorf("failed to publish to SeaweedMQ: %v", err)
	}

	// Update Kafka offset ledger
	ledger := h.GetOrCreateLedger(topic, partition)
	kafkaOffset := ledger.AssignOffsets(1) // Assign one Kafka offset

	// Map SeaweedMQ sequence to Kafka offset
	if err := ledger.AppendRecord(kafkaOffset, timestamp, int32(len(value))); err != nil {
		// Log the error but don't fail the produce operation
		fmt.Printf("Warning: failed to update offset ledger: %v\n", err)
	}

	return kafkaOffset, nil
}

// GetOrCreateLedger returns the offset ledger for a topic-partition
func (h *SeaweedMQHandler) GetOrCreateLedger(topic string, partition int32) *offset.Ledger {
	key := TopicPartitionKey{Topic: topic, Partition: partition}

	// Try to get existing ledger
	h.ledgersMu.RLock()
	ledger, exists := h.ledgers[key]
	h.ledgersMu.RUnlock()

	if exists {
		return ledger
	}

	// Create new ledger
	h.ledgersMu.Lock()
	defer h.ledgersMu.Unlock()

	// Double-check after acquiring write lock
	if ledger, exists := h.ledgers[key]; exists {
		return ledger
	}

	// Create and store new ledger
	ledger = offset.NewLedger()
	h.ledgers[key] = ledger
	return ledger
}

// GetLedger returns the offset ledger for a topic-partition, or nil if not found
func (h *SeaweedMQHandler) GetLedger(topic string, partition int32) *offset.Ledger {
	key := TopicPartitionKey{Topic: topic, Partition: partition}

	h.ledgersMu.RLock()
	defer h.ledgersMu.RUnlock()

	return h.ledgers[key]
}

// FetchRecords retrieves records from SeaweedMQ for a Kafka fetch request
func (h *SeaweedMQHandler) FetchRecords(topic string, partition int32, fetchOffset int64, maxBytes int32) ([]byte, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	ledger := h.GetLedger(topic, partition)
	if ledger == nil {
		// No messages yet, return empty record batch
		return []byte{}, nil
	}

	highWaterMark := ledger.GetHighWaterMark()

	// If fetch offset is at or beyond high water mark, no records to return
	if fetchOffset >= highWaterMark {
		return []byte{}, nil
	}

	// For Phase 2, we'll construct a simplified record batch
	// In a full implementation, this would read from SeaweedMQ subscriber
	return h.constructKafkaRecordBatch(ledger, fetchOffset, highWaterMark, maxBytes)
}

// constructKafkaRecordBatch creates a Kafka-compatible record batch
func (h *SeaweedMQHandler) constructKafkaRecordBatch(ledger *offset.Ledger, fetchOffset, highWaterMark int64, maxBytes int32) ([]byte, error) {
	recordsToFetch := highWaterMark - fetchOffset
	if recordsToFetch <= 0 {
		return []byte{}, nil
	}

	// Limit records to prevent overly large batches
	if recordsToFetch > 100 {
		recordsToFetch = 100
	}

	// For Phase 2, create a stub record batch with placeholder data
	// This represents what would come from SeaweedMQ subscriber
	batch := make([]byte, 0, 512)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset

	// Batch length (placeholder, will be filled at end)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch
	batch = append(batch, 2)          // magic byte (version 2)

	// CRC placeholder
	batch = append(batch, 0, 0, 0, 0)

	// Batch attributes
	batch = append(batch, 0, 0)

	// Last offset delta
	lastOffsetDelta := uint32(recordsToFetch - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// Timestamps
	currentTime := time.Now().UnixNano()
	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(currentTime))
	batch = append(batch, firstTimestampBytes...)

	maxTimestamp := currentTime + recordsToFetch*1000000 // 1ms apart
	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer info (simplified)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF) // producer ID (-1)
	batch = append(batch, 0xFF, 0xFF)                                     // producer epoch (-1)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)                         // base sequence (-1)

	// Record count
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(recordsToFetch))
	batch = append(batch, recordCountBytes...)

	// Add simple records (placeholders representing SeaweedMQ data)
	for i := int64(0); i < recordsToFetch; i++ {
		record := h.constructSingleRecord(i, fetchOffset+i)
		recordLength := byte(len(record))
		batch = append(batch, recordLength)
		batch = append(batch, record...)
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	return batch, nil
}

// constructSingleRecord creates a single Kafka record
func (h *SeaweedMQHandler) constructSingleRecord(index, offset int64) []byte {
	record := make([]byte, 0, 64)

	// Record attributes
	record = append(record, 0)

	// Timestamp delta (varint - simplified)
	record = append(record, byte(index))

	// Offset delta (varint - simplified)
	record = append(record, byte(index))

	// Key length (-1 = null key)
	record = append(record, 0xFF)

	// Value (represents data that would come from SeaweedMQ)
	value := fmt.Sprintf("seaweedmq-message-%d", offset)
	record = append(record, byte(len(value)))
	record = append(record, []byte(value)...)

	// Headers count (0)
	record = append(record, 0)

	return record
}
