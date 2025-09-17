package protocol

import (
	"strconv"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// testSeaweedMQHandlerForUnitTests is a minimal mock implementation for unit testing
type testSeaweedMQHandlerForUnitTests struct {
	topics  map[string]bool
	ledgers map[string]*offset.Ledger
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

func (t *testSeaweedMQHandlerForUnitTests) DeleteTopic(topic string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.topics, topic)
	return nil
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
	return kafkaOffset, nil
}

func (t *testSeaweedMQHandlerForUnitTests) GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error) {
	return []offset.SMQRecord{}, nil // Empty for testing
}

func (t *testSeaweedMQHandlerForUnitTests) GetFilerClient() filer_pb.SeaweedFilerClient {
	return nil
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
		},
		groupCoordinator:   consumer.NewGroupCoordinator(),
		topicMetadataCache: make(map[string]*CachedTopicMetadata),
	}
}
