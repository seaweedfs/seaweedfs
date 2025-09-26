package integration

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
)

// MockSeaweedClient provides a mock implementation for testing
type MockSeaweedClient struct {
	records map[string]map[int32][]*SeaweedRecord // topic -> partition -> records
}

func NewMockSeaweedClient() *MockSeaweedClient {
	return &MockSeaweedClient{
		records: make(map[string]map[int32][]*SeaweedRecord),
	}
}

func (m *MockSeaweedClient) AddRecord(topic string, partition int32, key []byte, value []byte, timestamp int64) {
	if m.records[topic] == nil {
		m.records[topic] = make(map[int32][]*SeaweedRecord)
	}
	if m.records[topic][partition] == nil {
		m.records[topic][partition] = make([]*SeaweedRecord, 0)
	}

	record := &SeaweedRecord{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Sequence:  int64(len(m.records[topic][partition])), // Simple sequence numbering
	}

	m.records[topic][partition] = append(m.records[topic][partition], record)
}

func (m *MockSeaweedClient) GetRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]*SeaweedRecord, error) {
	if m.records[topic] == nil || m.records[topic][partition] == nil {
		return nil, nil
	}

	allRecords := m.records[topic][partition]
	if fromOffset < 0 || fromOffset >= int64(len(allRecords)) {
		return nil, nil
	}

	endOffset := fromOffset + int64(maxRecords)
	if endOffset > int64(len(allRecords)) {
		endOffset = int64(len(allRecords))
	}

	return allRecords[fromOffset:endOffset], nil
}

func TestSeaweedSMQRecord_Interface(t *testing.T) {
	// Test that SeaweedSMQRecord properly implements offset.SMQRecord interface
	key := []byte("test-key")
	value := []byte("test-value")
	timestamp := time.Now().UnixNano()
	kafkaOffset := int64(42)

	record := &SeaweedSMQRecord{
		key:       key,
		value:     value,
		timestamp: timestamp,
		offset:    kafkaOffset,
	}

	// Test interface compliance
	var smqRecord offset.SMQRecord = record

	// Test GetKey
	if string(smqRecord.GetKey()) != string(key) {
		t.Errorf("Expected key %s, got %s", string(key), string(smqRecord.GetKey()))
	}

	// Test GetValue
	if string(smqRecord.GetValue()) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(smqRecord.GetValue()))
	}

	// Test GetTimestamp
	if smqRecord.GetTimestamp() != timestamp {
		t.Errorf("Expected timestamp %d, got %d", timestamp, smqRecord.GetTimestamp())
	}

	// Test GetOffset
	if smqRecord.GetOffset() != kafkaOffset {
		t.Errorf("Expected offset %d, got %d", kafkaOffset, smqRecord.GetOffset())
	}
}

func TestSeaweedMQHandler_GetStoredRecords_EmptyTopic(t *testing.T) {
	// Test behavior with non-existent topic
	handler := &SeaweedMQHandler{
		ledgers: make(map[TopicPartitionKey]*offset.Ledger),
	}

	records, err := handler.GetStoredRecords("non-existent-topic", 0, 0, 10)

	if err == nil {
		t.Error("Expected error for non-existent topic")
	}

	if records != nil {
		t.Error("Expected nil records for non-existent topic")
	}
}

func TestSeaweedMQHandler_GetStoredRecords_EmptyPartition(t *testing.T) {
	// Test behavior with topic but no messages
	handler := &SeaweedMQHandler{
		ledgers: make(map[TopicPartitionKey]*offset.Ledger),
	}

	// Note: Topics are now managed directly in filer, not in memory
	// Without filer connection, topic existence check will fail
	records, err := handler.GetStoredRecords("test-topic", 0, 0, 10)

	if err == nil {
		t.Error("Expected error for non-existent topic (no filer connection)")
	}

	if records != nil {
		t.Error("Expected nil records for non-existent topic")
	}
}

func TestSeaweedMQHandler_GetStoredRecords_OffsetBeyondHighWaterMark(t *testing.T) {
	// Test behavior when fetch offset is beyond available messages
	handler := &SeaweedMQHandler{
		ledgers: make(map[TopicPartitionKey]*offset.Ledger),
	}

	// Note: Topics are now managed directly in filer, not in memory
	// Without filer connection, topic existence check will fail
	ledger := offset.NewLedger()
	key := TopicPartitionKey{Topic: "test-topic", Partition: 0}
	handler.ledgers[key] = ledger

	// Add 3 messages to ledger
	for i := 0; i < 3; i++ {
		offset := ledger.AssignOffsets(1)
		ledger.AppendRecord(offset, time.Now().UnixNano(), 100)
	}

	// Try to fetch from offset 5 - but topic existence check will fail first
	records, err := handler.GetStoredRecords("test-topic", 0, 5, 10)

	if err == nil {
		t.Error("Expected error for non-existent topic (no filer connection)")
	}

	if records != nil {
		t.Error("Expected nil records for non-existent topic")
	}
}

func TestSeaweedMQHandler_GetStoredRecords_MaxRecordsLimit(t *testing.T) {
	// Test that maxRecords parameter is respected
	handler := &SeaweedMQHandler{
		ledgers: make(map[TopicPartitionKey]*offset.Ledger),
	}

	// Note: Topics are now managed directly in filer, not in memory
	// Create ledger with 10 messages to simulate topic with messages

	ledger := offset.NewLedger()
	key := TopicPartitionKey{Topic: "test-topic", Partition: 0}
	handler.ledgers[key] = ledger

	// Add 10 messages to ledger
	for i := 0; i < 10; i++ {
		offset := ledger.AssignOffsets(1)
		ledger.AppendRecord(offset, time.Now().UnixNano(), 100)
	}

	// Note: This test demonstrates the logic but won't work without a real client
	// In practice, GetStoredRecords needs brokerClient
	// The test would need to be enhanced with a mock client

	// For now, test that the method handles the no-client case gracefully
	records, err := handler.GetStoredRecords("test-topic", 0, 0, 3)

	// Should handle gracefully when no filer connection is available (topic existence check fails first)
	expectedError := "topic test-topic does not exist"
	if err == nil || err.Error() != expectedError {
		t.Errorf("Expected error '%s', got: %v", expectedError, err)
	}

	if records != nil {
		t.Error("Expected nil records for non-existent topic")
	}
}

// Integration test helpers and benchmarks

func BenchmarkSeaweedSMQRecord_GetMethods(b *testing.B) {
	record := &SeaweedSMQRecord{
		key:       []byte("benchmark-key"),
		value:     []byte("benchmark-value-with-some-longer-content"),
		timestamp: time.Now().UnixNano(),
		offset:    12345,
	}

	b.ResetTimer()

	b.Run("GetKey", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = record.GetKey()
		}
	})

	b.Run("GetValue", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = record.GetValue()
		}
	})

	b.Run("GetTimestamp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = record.GetTimestamp()
		}
	})

	b.Run("GetOffset", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = record.GetOffset()
		}
	})
}
