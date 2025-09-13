package kafka

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// MockSMQHandler provides a realistic SeaweedMQ simulation for testing
// It behaves like the real SeaweedMQ integration but uses in-memory storage
type MockSMQHandler struct {
	// Topic management
	topicsMu sync.RWMutex
	topics   map[string]*MockTopicInfo

	// Message storage - simulates SeaweedMQ's persistent storage
	messagesMu sync.RWMutex
	messages   map[string]map[int32][]*MockSMQRecord // topic -> partition -> []records

	// Offset management - simulates Kafka offset ledgers
	ledgersMu sync.RWMutex
	ledgers   map[string]*offset.Ledger // topic-partition -> ledger

	// Simulated SMQ timestamp tracking
	lastTimestamp map[string]map[int32]int64 // topic -> partition -> last_timestamp
}

// MockTopicInfo represents a Kafka topic in the mock SMQ environment
type MockTopicInfo struct {
	Name       string
	Partitions int32
	CreatedAt  int64
	Schema     *schema_pb.Topic
}

// MockSMQRecord represents a record in the mock SeaweedMQ storage
type MockSMQRecord struct {
	Key       []byte
	Value     []byte
	Timestamp int64 // SeaweedMQ timestamp (nanoseconds)
	Offset    int64 // Kafka offset for this partition
}

// Implement SMQRecord interface
func (r *MockSMQRecord) GetKey() []byte      { return r.Key }
func (r *MockSMQRecord) GetValue() []byte    { return r.Value }
func (r *MockSMQRecord) GetTimestamp() int64 { return r.Timestamp }
func (r *MockSMQRecord) GetOffset() int64    { return r.Offset }

// NewMockSMQHandler creates a new mock SeaweedMQ handler for testing
func NewMockSMQHandler() *MockSMQHandler {
	return &MockSMQHandler{
		topics:        make(map[string]*MockTopicInfo),
		messages:      make(map[string]map[int32][]*MockSMQRecord),
		ledgers:       make(map[string]*offset.Ledger),
		lastTimestamp: make(map[string]map[int32]int64),
	}
}

// TopicExists checks if a topic exists in the mock SMQ
func (m *MockSMQHandler) TopicExists(topic string) bool {
	m.topicsMu.RLock()
	defer m.topicsMu.RUnlock()
	_, exists := m.topics[topic]
	return exists
}

// ListTopics returns all topics in the mock SMQ
func (m *MockSMQHandler) ListTopics() []string {
	m.topicsMu.RLock()
	defer m.topicsMu.RUnlock()

	topics := make([]string, 0, len(m.topics))
	for name := range m.topics {
		topics = append(topics, name)
	}
	return topics
}

// CreateTopic creates a new topic in the mock SMQ
func (m *MockSMQHandler) CreateTopic(topic string, partitions int32) error {
	m.topicsMu.Lock()
	defer m.topicsMu.Unlock()

	if _, exists := m.topics[topic]; exists {
		return fmt.Errorf("topic %s already exists", topic)
	}

	// Create topic info
	m.topics[topic] = &MockTopicInfo{
		Name:       topic,
		Partitions: partitions,
		CreatedAt:  time.Now().UnixNano(),
		Schema: &schema_pb.Topic{
			Name: topic,
			// PartitionCount removed - not part of schema_pb.Topic
		},
	}

	// Initialize message storage for all partitions
	m.messagesMu.Lock()
	m.messages[topic] = make(map[int32][]*MockSMQRecord)
	for i := int32(0); i < partitions; i++ {
		m.messages[topic][i] = make([]*MockSMQRecord, 0, 1000)
	}
	m.messagesMu.Unlock()

	// Initialize timestamp tracking
	if m.lastTimestamp[topic] == nil {
		m.lastTimestamp[topic] = make(map[int32]int64)
	}
	for i := int32(0); i < partitions; i++ {
		m.lastTimestamp[topic][i] = 0
	}

	fmt.Printf("MOCK SMQ: Created topic %s with %d partitions\n", topic, partitions)
	return nil
}

// DeleteTopic removes a topic from the mock SMQ
func (m *MockSMQHandler) DeleteTopic(topic string) error {
	m.topicsMu.Lock()
	defer m.topicsMu.Unlock()

	if _, exists := m.topics[topic]; !exists {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	delete(m.topics, topic)

	// Clean up messages
	m.messagesMu.Lock()
	delete(m.messages, topic)
	m.messagesMu.Unlock()

	// Clean up ledgers
	m.ledgersMu.Lock()
	keysToDelete := make([]string, 0)
	for key := range m.ledgers {
		if key[:len(topic)+1] == topic+"-" {
			keysToDelete = append(keysToDelete, key)
		}
	}
	for _, key := range keysToDelete {
		delete(m.ledgers, key)
	}
	m.ledgersMu.Unlock()

	fmt.Printf("MOCK SMQ: Deleted topic %s\n", topic)
	return nil
}

// GetOrCreateLedger gets or creates a Kafka offset ledger for a topic-partition
func (m *MockSMQHandler) GetOrCreateLedger(topic string, partition int32) *offset.Ledger {
	key := fmt.Sprintf("%s-%d", topic, partition)

	m.ledgersMu.Lock()
	defer m.ledgersMu.Unlock()

	if ledger, exists := m.ledgers[key]; exists {
		return ledger
	}

	// Create new ledger
	ledger := offset.NewLedger()
	m.ledgers[key] = ledger

	// Ensure topic is created
	if !m.TopicExists(topic) {
		m.CreateTopic(topic, partition+1) // Ensure enough partitions
	}

	return ledger
}

// GetLedger retrieves an existing ledger or returns nil
func (m *MockSMQHandler) GetLedger(topic string, partition int32) *offset.Ledger {
	key := fmt.Sprintf("%s-%d", topic, partition)

	m.ledgersMu.RLock()
	defer m.ledgersMu.RUnlock()

	return m.ledgers[key]
}

// ProduceRecord publishes a record to the mock SeaweedMQ and updates Kafka offset tracking
func (m *MockSMQHandler) ProduceRecord(topic string, partition int32, key, value []byte) (int64, error) {
	// Verify topic exists
	if !m.TopicExists(topic) {
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}

	// Get current timestamp (simulate SeaweedMQ timestamp)
	timestamp := time.Now().UnixNano()

	// Get or create Kafka offset ledger
	ledger := m.GetOrCreateLedger(topic, partition)

	// Assign Kafka offset
	kafkaOffset := ledger.AssignOffsets(1)

	// Record in ledger (simulates Kafka offset -> SMQ timestamp mapping)
	messageSize := int32(len(value))
	if err := ledger.AppendRecord(kafkaOffset, timestamp, messageSize); err != nil {
		return 0, fmt.Errorf("failed to append to ledger: %w", err)
	}

	// Store message in mock SMQ storage (simulates persistent storage)
	m.messagesMu.Lock()
	defer m.messagesMu.Unlock()

	if m.messages[topic] == nil {
		m.messages[topic] = make(map[int32][]*MockSMQRecord)
	}
	if m.messages[topic][partition] == nil {
		m.messages[topic][partition] = make([]*MockSMQRecord, 0, 1000)
	}

	// Create message record (simulate SMQ storage)
	record := &MockSMQRecord{
		Key:       append([]byte(nil), key...),   // Deep copy key
		Value:     append([]byte(nil), value...), // Deep copy value
		Timestamp: timestamp,
		Offset:    kafkaOffset,
	}

	// Append to partition (simulate SMQ append-only log)
	m.messages[topic][partition] = append(m.messages[topic][partition], record)

	// Update last timestamp
	m.lastTimestamp[topic][partition] = timestamp

	fmt.Printf("MOCK SMQ: Stored record - topic:%s, partition:%d, kafka_offset:%d, smq_timestamp:%d, key:%s, value:%s\n",
		topic, partition, kafkaOffset, timestamp, string(key), string(value))

	return kafkaOffset, nil
}

// GetStoredRecords retrieves records from mock SMQ storage starting from a given Kafka offset
func (m *MockSMQHandler) GetStoredRecords(topic string, partition int32, fromKafkaOffset int64, maxRecords int) ([]offset.SMQRecord, error) {
	m.messagesMu.RLock()
	defer m.messagesMu.RUnlock()

	if m.messages[topic] == nil || m.messages[topic][partition] == nil {
		return nil, nil // No messages
	}

	records := m.messages[topic][partition]
	result := make([]offset.SMQRecord, 0, maxRecords)

	// Find records starting from the given Kafka offset
	for _, record := range records {
		if record.Offset >= fromKafkaOffset && len(result) < maxRecords {
			result = append(result, record)
		}
	}

	fmt.Printf("MOCK SMQ: Retrieved %d records from topic:%s, partition:%d, from_offset:%d\n",
		len(result), topic, partition, fromKafkaOffset)

	return result, nil
}

// Close shuts down the mock SMQ handler
func (m *MockSMQHandler) Close() error {
	fmt.Printf("MOCK SMQ: Handler closed\n")
	return nil
}

// GetPartitionCount returns the number of partitions for a topic
func (m *MockSMQHandler) GetPartitionCount(topic string) int32 {
	m.topicsMu.RLock()
	defer m.topicsMu.RUnlock()

	if topicInfo, exists := m.topics[topic]; exists {
		return topicInfo.Partitions
	}
	return 0
}

// GetMessageCount returns the total number of messages stored for a topic-partition
func (m *MockSMQHandler) GetMessageCount(topic string, partition int32) int {
	m.messagesMu.RLock()
	defer m.messagesMu.RUnlock()

	if m.messages[topic] == nil || m.messages[topic][partition] == nil {
		return 0
	}
	return len(m.messages[topic][partition])
}
