package protocol

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// testSMQRecord implements the SMQRecord interface for testing
type testSMQRecord struct {
	offset    int64
	timestamp int64
	key       []byte
	value     []byte
}

func (r *testSMQRecord) GetOffset() int64    { return r.offset }
func (r *testSMQRecord) GetTimestamp() int64 { return r.timestamp }
func (r *testSMQRecord) GetKey() []byte      { return r.key }
func (r *testSMQRecord) GetValue() []byte    { return r.value }

// testSeaweedMQHandlerForUnitTests is a minimal mock implementation for unit testing
type testSeaweedMQHandlerForUnitTests struct {
	topics  map[string]bool
	ledgers map[string]*offset.Ledger
	records map[string][]offset.SMQRecord // Store records for GetStoredRecords
	mu      sync.RWMutex
}

func (t *testSeaweedMQHandlerForUnitTests) TopicExists(topic string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.topics[topic]
}

func (t *testSeaweedMQHandlerForUnitTests) ListTopics() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	topics := make([]string, 0, len(t.topics))
	for topic := range t.topics {
		topics = append(topics, topic)
	}
	return topics
}

func (t *testSeaweedMQHandlerForUnitTests) CreateTopic(topic string, partitions int32) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.topics[topic] = true
	return nil
}

func (t *testSeaweedMQHandlerForUnitTests) CreateTopicWithSchemas(name string, partitions int32, valueRecordType *schema_pb.RecordType, keyRecordType *schema_pb.RecordType) error {
	// For test handler, just delegate to CreateTopic (ignore schemas)
	return t.CreateTopic(name, partitions)
}

func (t *testSeaweedMQHandlerForUnitTests) DeleteTopic(topic string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.topics, topic)
	return nil
}

func (t *testSeaweedMQHandlerForUnitTests) GetTopicInfo(topic string) (*integration.KafkaTopicInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
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

func (t *testSeaweedMQHandlerForUnitTests) GetOrCreateLedger(topic string, partition int32) *offset.Ledger {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := topicPartitionKeyForTest(topic, partition)
	if ledger, exists := t.ledgers[key]; exists {
		return ledger
	}
	ledger := offset.NewLedger()
	t.ledgers[key] = ledger
	return ledger
}

func (t *testSeaweedMQHandlerForUnitTests) GetLedger(topic string, partition int32) *offset.Ledger {
	t.mu.RLock()
	defer t.mu.RUnlock()
	key := topicPartitionKeyForTest(topic, partition)
	return t.ledgers[key]
}

func (t *testSeaweedMQHandlerForUnitTests) ProduceRecord(topicName string, partitionID int32, key, value []byte) (int64, error) {
	ledger := t.GetOrCreateLedger(topicName, partitionID)
	// Assign an offset first
	kafkaOffset := ledger.AssignOffsets(1)
	// Append the record with current timestamp and estimated size
	timestamp := time.Now().UnixNano()
	size := int32(len(key) + len(value))
	if err := ledger.AppendRecord(kafkaOffset, timestamp, size); err != nil {
		return -1, err
	}

	// Store the record for GetStoredRecords
	t.mu.Lock()
	recordKey := topicPartitionKeyForTest(topicName, partitionID)
	if t.records == nil {
		t.records = make(map[string][]offset.SMQRecord)
	}
	t.records[recordKey] = append(t.records[recordKey], &testSMQRecord{
		offset:    kafkaOffset,
		timestamp: timestamp,
		key:       key,
		value:     value,
	})
	t.mu.Unlock()

	return kafkaOffset, nil
}

func (t *testSeaweedMQHandlerForUnitTests) ProduceRecordValue(topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error) {
	// For testing, just delegate to ProduceRecord with the raw recordValueBytes
	return t.ProduceRecord(topicName, partitionID, key, recordValueBytes)
}

func (t *testSeaweedMQHandlerForUnitTests) GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	recordKey := topicPartitionKeyForTest(topic, partition)
	allRecords, exists := t.records[recordKey]
	if !exists {
		return []offset.SMQRecord{}, nil
	}

	// Filter records by offset range
	var result []offset.SMQRecord
	for _, record := range allRecords {
		if record.GetOffset() >= fromOffset {
			result = append(result, record)
			if len(result) >= maxRecords {
				break
			}
		}
	}

	return result, nil
}

func (t *testSeaweedMQHandlerForUnitTests) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fmt.Errorf("test handler doesn't have filer access")
}

func (t *testSeaweedMQHandlerForUnitTests) GetBrokerAddresses() []string {
	return []string{"localhost:17777"}
}

func (t *testSeaweedMQHandlerForUnitTests) Close() error {
	return nil
}

// topicPartitionKeyForTest creates a unique key for topic-partition combination
func topicPartitionKeyForTest(topic string, partition int32) string {
	return topic + "-" + strconv.Itoa(int(partition))
}

// NewHandlerForUnitTests creates a handler for unit testing without requiring SeaweedMQ masters
// This should ONLY be used for unit tests that don't need real SeaweedMQ functionality
func NewHandlerForUnitTests() *Handler {
	return &Handler{
		seaweedMQHandler: &testSeaweedMQHandlerForUnitTests{
			topics:  make(map[string]bool),
			ledgers: make(map[string]*offset.Ledger),
			records: make(map[string][]offset.SMQRecord),
		},
		groupCoordinator:  consumer.NewGroupCoordinator(),
		registeredSchemas: make(map[string]bool),
	}
}
