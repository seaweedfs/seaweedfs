package protocol

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MessageRecord represents a stored message (TEST ONLY)
type MessageRecord struct {
	Key       []byte
	Value     []byte
	Timestamp int64
}

// basicSeaweedMQHandler is a minimal in-memory implementation for testing (TEST ONLY)
type basicSeaweedMQHandler struct {
	topics  map[string]bool
	ledgers map[string]*offset.Ledger
	// messages stores actual message content indexed by topic-partition-offset
	messages map[string]map[int32]map[int64]*MessageRecord // topic -> partition -> offset -> message
	mu       sync.RWMutex
}

// testSeaweedMQHandler is a minimal mock implementation for testing (TEST ONLY)
type testSeaweedMQHandler struct {
	topics  map[string]bool
	ledgers map[string]*offset.Ledger
	mu      sync.RWMutex
}

// NewTestHandler creates a handler for integration testing with real SMQ storage
// This should ONLY be used in tests - uses basicSeaweedMQHandler for message storage simulation
// and real SMQ offset storage for realistic offset persistence testing
func NewTestHandler() *Handler {
	// Create filer client accessor for test SMQ offset storage
	filerClientAccessor := filer_client.NewFilerClientAccessor(
		[]pb.ServerAddress{"127.0.0.1:8888"},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	smqOffsetStorage := offset.NewSMQOffsetStorage(filerClientAccessor)

	return &Handler{
		groupCoordinator: consumer.NewGroupCoordinator(),
		seaweedMQHandler: &basicSeaweedMQHandler{
			topics:   make(map[string]bool),
			ledgers:  make(map[string]*offset.Ledger),
			messages: make(map[string]map[int32]map[int64]*MessageRecord),
		},
		smqOffsetStorage: smqOffsetStorage,
	}
}

// NewSimpleTestHandler creates a minimal test handler without message storage
// This should ONLY be used for basic protocol tests that don't need message content
func NewSimpleTestHandler() *Handler {
	// Create filer client accessor for test SMQ offset storage
	filerClientAccessor := filer_client.NewFilerClientAccessor(
		[]pb.ServerAddress{"127.0.0.1:8888"},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	smqOffsetStorage := offset.NewSMQOffsetStorage(filerClientAccessor)

	return &Handler{
		groupCoordinator: consumer.NewGroupCoordinator(),
		smqOffsetStorage: smqOffsetStorage,
		seaweedMQHandler: &testSeaweedMQHandler{
			topics:  make(map[string]bool),
			ledgers: make(map[string]*offset.Ledger),
		},
	}
}

// ===== basicSeaweedMQHandler implementation (TEST ONLY) =====

func (b *basicSeaweedMQHandler) TopicExists(topic string) bool {
	return b.topics[topic]
}

func (b *basicSeaweedMQHandler) ListTopics() []string {
	topics := make([]string, 0, len(b.topics))
	for topic := range b.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (b *basicSeaweedMQHandler) CreateTopic(topic string, partitions int32) error {
	b.topics[topic] = true
	return nil
}

func (b *basicSeaweedMQHandler) CreateTopicWithSchemas(name string, partitions int32, valueRecordType *schema_pb.RecordType, keyRecordType *schema_pb.RecordType) error {
	// For test handler, just delegate to CreateTopic (ignore schemas)
	return b.CreateTopic(name, partitions)
}

func (b *basicSeaweedMQHandler) DeleteTopic(topic string) error {
	delete(b.topics, topic)
	return nil
}

func (b *basicSeaweedMQHandler) GetTopicInfo(topic string) (*integration.KafkaTopicInfo, bool) {
	if !b.topics[topic] {
		return nil, false
	}
	// For test handler, return basic info with 1 partition by default
	return &integration.KafkaTopicInfo{
		Name:       topic,
		Partitions: 1,
		CreatedAt:  time.Now().UnixNano(),
	}, true
}

func (b *basicSeaweedMQHandler) GetOrCreateLedger(topic string, partition int32) *offset.Ledger {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if ledger, exists := b.ledgers[key]; exists {
		return ledger
	}

	// Create new ledger
	ledger := offset.NewLedger()
	b.ledgers[key] = ledger

	// Also create the topic if it doesn't exist
	b.topics[topic] = true

	return ledger
}

// GetLedger method REMOVED - SMQ handles Kafka offsets natively

func (b *basicSeaweedMQHandler) ProduceRecord(topicName string, partitionID int32, key, value []byte) (int64, error) {
	// Get or create the ledger first (this will acquire and release the lock)
	ledger := b.GetOrCreateLedger(topicName, partitionID)

	// Now acquire the lock for the rest of the operation
	b.mu.Lock()
	defer b.mu.Unlock()

	// Assign an offset and append the record
	offset := ledger.AssignOffsets(1)
	timestamp := time.Now().UnixNano()
	size := int32(len(value))

	if err := ledger.AppendRecord(offset, timestamp, size); err != nil {
		return 0, fmt.Errorf("failed to append record: %w", err)
	}

	// Store the actual message content
	if b.messages[topicName] == nil {
		b.messages[topicName] = make(map[int32]map[int64]*MessageRecord)
	}
	if b.messages[topicName][partitionID] == nil {
		b.messages[topicName][partitionID] = make(map[int64]*MessageRecord)
	}

	// Make copies of key and value to avoid referencing the original slices
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	b.messages[topicName][partitionID][offset] = &MessageRecord{
		Key:       keyCopy,
		Value:     valueCopy,
		Timestamp: timestamp,
	}

	return offset, nil
}

func (b *basicSeaweedMQHandler) ProduceRecordValue(topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error) {
	// For testing, just delegate to ProduceRecord with the raw recordValueBytes
	return b.ProduceRecord(topicName, partitionID, key, recordValueBytes)
}

// GetStoredMessages retrieves stored messages for a topic-partition from a given offset (TEST ONLY)
func (b *basicSeaweedMQHandler) GetStoredMessages(topicName string, partitionID int32, fromOffset int64, maxMessages int) []*MessageRecord {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.messages[topicName] == nil || b.messages[topicName][partitionID] == nil {
		return nil
	}

	partitionMessages := b.messages[topicName][partitionID]
	var result []*MessageRecord

	// Collect messages starting from fromOffset
	for offset := fromOffset; offset < fromOffset+int64(maxMessages); offset++ {
		if msg, exists := partitionMessages[offset]; exists {
			result = append(result, msg)
		} else {
			// No more consecutive messages
			break
		}
	}

	return result
}

// BasicSMQRecord implements SMQRecord interface for basicSeaweedMQHandler (TEST ONLY)
type BasicSMQRecord struct {
	*MessageRecord
	offset int64
}

func (r *BasicSMQRecord) GetKey() []byte      { return r.Key }
func (r *BasicSMQRecord) GetValue() []byte    { return r.Value }
func (r *BasicSMQRecord) GetTimestamp() int64 { return r.Timestamp }
func (r *BasicSMQRecord) GetOffset() int64    { return r.offset }

// GetStoredRecords retrieves stored message records for basicSeaweedMQHandler (TEST ONLY)
func (b *basicSeaweedMQHandler) GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error) {
	messages := b.GetStoredMessages(topic, partition, fromOffset, maxRecords)
	if len(messages) == 0 {
		return nil, nil
	}

	records := make([]offset.SMQRecord, len(messages))
	for i, msg := range messages {
		records[i] = &BasicSMQRecord{
			MessageRecord: msg,
			offset:        fromOffset + int64(i),
		}
	}
	return records, nil
}

func (b *basicSeaweedMQHandler) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fmt.Errorf("test handler doesn't have filer access")
}

// GetBrokerAddresses returns the discovered SMQ broker addresses for Metadata responses
func (b *basicSeaweedMQHandler) GetBrokerAddresses() []string {
	return []string{"localhost:17777"} // Test broker address
}

func (b *basicSeaweedMQHandler) Close() error {
	return nil
}

// ===== testSeaweedMQHandler implementation (TEST ONLY) =====

func (t *testSeaweedMQHandler) TopicExists(topic string) bool {
	return t.topics[topic]
}

func (t *testSeaweedMQHandler) ListTopics() []string {
	var topics []string
	for topic := range t.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (t *testSeaweedMQHandler) CreateTopic(topic string, partitions int32) error {
	t.topics[topic] = true
	return nil
}

func (t *testSeaweedMQHandler) CreateTopicWithSchemas(name string, partitions int32, valueRecordType *schema_pb.RecordType, keyRecordType *schema_pb.RecordType) error {
	// For test handler, just delegate to CreateTopic (ignore schemas)
	return t.CreateTopic(name, partitions)
}

func (t *testSeaweedMQHandler) DeleteTopic(topic string) error {
	delete(t.topics, topic)
	return nil
}

func (t *testSeaweedMQHandler) GetTopicInfo(topic string) (*integration.KafkaTopicInfo, bool) {
	if !t.topics[topic] {
		return nil, false
	}
	// For test handler, return basic info with 1 partition by default
	return &integration.KafkaTopicInfo{
		Name:       topic,
		Partitions: 1,
		CreatedAt:  time.Now().UnixNano(),
	}, true
}

func (t *testSeaweedMQHandler) GetOrCreateLedger(topic string, partition int32) *offset.Ledger {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Mark topic as existing when creating ledger
	t.topics[topic] = true

	key := fmt.Sprintf("%s-%d", topic, partition)
	if ledger, exists := t.ledgers[key]; exists {
		return ledger
	}

	ledger := offset.NewLedger()
	t.ledgers[key] = ledger
	return ledger
}

func (t *testSeaweedMQHandler) GetLedger(topic string, partition int32) *offset.Ledger {
	t.mu.RLock()
	defer t.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if ledger, exists := t.ledgers[key]; exists {
		return ledger
	}

	// Return nil if ledger doesn't exist (topic doesn't exist)
	return nil
}

func (t *testSeaweedMQHandler) ProduceRecord(topicName string, partitionID int32, key, value []byte) (int64, error) {
	// For testing, actually store the record in the ledger
	ledger := t.GetOrCreateLedger(topicName, partitionID)

	// Assign an offset and append the record
	offset := ledger.AssignOffsets(1)
	timestamp := time.Now().UnixNano()
	size := int32(len(value))

	if err := ledger.AppendRecord(offset, timestamp, size); err != nil {
		return 0, fmt.Errorf("failed to append record: %w", err)
	}

	return offset, nil
}

func (t *testSeaweedMQHandler) ProduceRecordValue(topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error) {
	// For testing, just delegate to ProduceRecord with the raw recordValueBytes
	return t.ProduceRecord(topicName, partitionID, key, recordValueBytes)
}

// GetStoredRecords for testSeaweedMQHandler - returns empty (no storage simulation)
func (t *testSeaweedMQHandler) GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error) {
	// Test handler doesn't simulate message storage, return empty
	return nil, nil
}

func (t *testSeaweedMQHandler) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fmt.Errorf("test handler doesn't have filer access")
}

// GetBrokerAddresses returns the discovered SMQ broker addresses for Metadata responses
func (t *testSeaweedMQHandler) GetBrokerAddresses() []string {
	return []string{"localhost:17777"} // Test broker address
}

func (t *testSeaweedMQHandler) Close() error {
	return nil
}
