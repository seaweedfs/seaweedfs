package gateway

import (
	"context"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	schema_pb "github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// mockRecord implements the SMQRecord interface for testing
type mockRecord struct {
	key       []byte
	value     []byte
	timestamp int64
	offset    int64
}

func (r *mockRecord) GetKey() []byte      { return r.key }
func (r *mockRecord) GetValue() []byte    { return r.value }
func (r *mockRecord) GetTimestamp() int64 { return r.timestamp }
func (r *mockRecord) GetOffset() int64    { return r.offset }

// mockSeaweedMQHandler is a stateful mock for unit testing without real SeaweedMQ
type mockSeaweedMQHandler struct {
	mu      sync.RWMutex
	topics  map[string]*integration.KafkaTopicInfo
	records map[string]map[int32][]integration.SMQRecord // topic -> partition -> records
	offsets map[string]map[int32]int64                   // topic -> partition -> next offset
}

func newMockSeaweedMQHandler() *mockSeaweedMQHandler {
	return &mockSeaweedMQHandler{
		topics:  make(map[string]*integration.KafkaTopicInfo),
		records: make(map[string]map[int32][]integration.SMQRecord),
		offsets: make(map[string]map[int32]int64),
	}
}

func (m *mockSeaweedMQHandler) TopicExists(topic string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.topics[topic]
	return exists
}

func (m *mockSeaweedMQHandler) ListTopics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	topics := make([]string, 0, len(m.topics))
	for topic := range m.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (m *mockSeaweedMQHandler) CreateTopic(topic string, partitions int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.topics[topic]; exists {
		return fmt.Errorf("topic already exists")
	}
	m.topics[topic] = &integration.KafkaTopicInfo{
		Name:       topic,
		Partitions: partitions,
	}
	return nil
}

func (m *mockSeaweedMQHandler) CreateTopicWithSchemas(name string, partitions int32, keyRecordType *schema_pb.RecordType, valueRecordType *schema_pb.RecordType) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.topics[name]; exists {
		return fmt.Errorf("topic already exists")
	}
	m.topics[name] = &integration.KafkaTopicInfo{
		Name:       name,
		Partitions: partitions,
	}
	return nil
}

func (m *mockSeaweedMQHandler) DeleteTopic(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.topics, topic)
	return nil
}

func (m *mockSeaweedMQHandler) GetTopicInfo(topic string) (*integration.KafkaTopicInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	info, exists := m.topics[topic]
	return info, exists
}

func (m *mockSeaweedMQHandler) InvalidateTopicExistsCache(topic string) {
	// Mock handler doesn't cache topic existence, so this is a no-op
}

func (m *mockSeaweedMQHandler) ProduceRecord(ctx context.Context, topicName string, partitionID int32, key, value []byte) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if topic exists
	if _, exists := m.topics[topicName]; !exists {
		return 0, fmt.Errorf("topic does not exist: %s", topicName)
	}

	// Initialize partition records if needed
	if _, exists := m.records[topicName]; !exists {
		m.records[topicName] = make(map[int32][]integration.SMQRecord)
		m.offsets[topicName] = make(map[int32]int64)
	}

	// Get next offset
	offset := m.offsets[topicName][partitionID]
	m.offsets[topicName][partitionID]++

	// Store record
	record := &mockRecord{
		key:    key,
		value:  value,
		offset: offset,
	}
	m.records[topicName][partitionID] = append(m.records[topicName][partitionID], record)

	return offset, nil
}

func (m *mockSeaweedMQHandler) ProduceRecordValue(ctx context.Context, topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error) {
	return m.ProduceRecord(ctx, topicName, partitionID, key, recordValueBytes)
}

func (m *mockSeaweedMQHandler) GetStoredRecords(ctx context.Context, topic string, partition int32, fromOffset int64, maxRecords int) ([]integration.SMQRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if topic exists
	if _, exists := m.topics[topic]; !exists {
		return nil, fmt.Errorf("topic does not exist: %s", topic)
	}

	// Get partition records
	partitionRecords, exists := m.records[topic][partition]
	if !exists || len(partitionRecords) == 0 {
		return []integration.SMQRecord{}, nil
	}

	// Find records starting from fromOffset
	result := make([]integration.SMQRecord, 0, maxRecords)
	for _, record := range partitionRecords {
		if record.GetOffset() >= fromOffset {
			result = append(result, record)
			if len(result) >= maxRecords {
				break
			}
		}
	}

	return result, nil
}

func (m *mockSeaweedMQHandler) GetEarliestOffset(topic string, partition int32) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if topic exists
	if _, exists := m.topics[topic]; !exists {
		return 0, fmt.Errorf("topic does not exist: %s", topic)
	}

	// Get partition records
	partitionRecords, exists := m.records[topic][partition]
	if !exists || len(partitionRecords) == 0 {
		return 0, nil
	}

	return partitionRecords[0].GetOffset(), nil
}

func (m *mockSeaweedMQHandler) GetLatestOffset(topic string, partition int32) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if topic exists
	if _, exists := m.topics[topic]; !exists {
		return 0, fmt.Errorf("topic does not exist: %s", topic)
	}

	// Return next offset (latest + 1)
	if offsets, exists := m.offsets[topic]; exists {
		return offsets[partition], nil
	}

	return 0, nil
}

func (m *mockSeaweedMQHandler) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fmt.Errorf("mock handler: not implemented")
}

func (m *mockSeaweedMQHandler) CreatePerConnectionBrokerClient() (*integration.BrokerClient, error) {
	// Return a minimal broker client that won't actually connect
	return nil, fmt.Errorf("mock handler: per-connection broker client not available in unit test mode")
}

func (m *mockSeaweedMQHandler) GetFilerClientAccessor() *filer_client.FilerClientAccessor {
	return nil
}

func (m *mockSeaweedMQHandler) GetBrokerAddresses() []string {
	return []string{"localhost:9092"} // Return a dummy broker address for unit tests
}

func (m *mockSeaweedMQHandler) Close() error { return nil }

func (m *mockSeaweedMQHandler) SetProtocolHandler(h integration.ProtocolHandler) {}

// NewMinimalTestHandler creates a minimal handler for unit testing
// that won't actually process Kafka protocol requests
func NewMinimalTestHandler() *protocol.Handler {
	return protocol.NewTestHandlerWithMock(newMockSeaweedMQHandler())
}
