package integration

import (
	"testing"
	"time"
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
		Offset:    int64(len(m.records[topic][partition])), // Simple offset numbering
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
	// Test that SeaweedSMQRecord properly implements SMQRecord interface
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
	var smqRecord SMQRecord = record

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
	// Note: Ledgers have been removed - SMQ broker handles all offset management directly
	// This test is now obsolete as GetStoredRecords requires a real broker connection
	t.Skip("Test obsolete: ledgers removed, SMQ broker handles offset management")
}

func TestSeaweedMQHandler_GetStoredRecords_EmptyPartition(t *testing.T) {
	// Note: Ledgers have been removed - SMQ broker handles all offset management directly
	// This test is now obsolete as GetStoredRecords requires a real broker connection
	t.Skip("Test obsolete: ledgers removed, SMQ broker handles offset management")
}

func TestSeaweedMQHandler_GetStoredRecords_OffsetBeyondHighWaterMark(t *testing.T) {
	// Note: Ledgers have been removed - SMQ broker handles all offset management directly
	// This test is now obsolete as GetStoredRecords requires a real broker connection
	t.Skip("Test obsolete: ledgers removed, SMQ broker handles offset management")
}

func TestSeaweedMQHandler_GetStoredRecords_MaxRecordsLimit(t *testing.T) {
	// Note: Ledgers have been removed - SMQ broker handles all offset management directly
	// This test is now obsolete as GetStoredRecords requires a real broker connection
	t.Skip("Test obsolete: ledgers removed, SMQ broker handles offset management")
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
