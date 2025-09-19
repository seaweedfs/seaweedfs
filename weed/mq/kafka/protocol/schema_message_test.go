package protocol

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/protobuf/proto"
)

func TestProduceSchemaBasedRecordWithoutSchema(t *testing.T) {
	// Test that when schema management is disabled, it falls back to raw message handling
	handler := &Handler{}

	key := []byte("test-key")
	value := []byte("test-value")

	// Mock the seaweedMQHandler to capture the call
	mockHandler := &testSeaweedMQHandlerForUnitTests{
		topics:  make(map[string]bool),
		ledgers: make(map[string]*offset.Ledger),
		records: make(map[string][]offset.SMQRecord),
	}
	mockHandler.topics["test-topic"] = true
	handler.seaweedMQHandler = mockHandler

	// Since schema management is not enabled, should call ProduceRecord
	offset, err := handler.produceSchemaBasedRecord("test-topic", 0, key, value)
	if err != nil {
		t.Fatalf("Failed to produce record: %v", err)
	}

	if offset < 0 {
		t.Errorf("Expected non-negative offset, got %d", offset)
	}
}

func TestDecodeRecordValueToKafkaMessageWithoutSchema(t *testing.T) {
	handler := &Handler{}

	// Create a test RecordValue with schema-based fields (not fixed structure)
	recordValue := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"user_name": {
				Kind: &schema_pb.Value_StringValue{StringValue: "john_doe"},
			},
			"user_age": {
				Kind: &schema_pb.Value_Int32Value{Int32Value: 30},
			},
			"is_active": {
				Kind: &schema_pb.Value_BoolValue{BoolValue: true},
			},
		},
	}

	// Marshal to bytes
	recordValueBytes, err := proto.Marshal(recordValue)
	if err != nil {
		t.Fatalf("Failed to marshal RecordValue: %v", err)
	}

	// Decode back to Kafka message (should fallback to JSON since no schema manager)
	decodedValue := handler.decodeRecordValueToKafkaMessage(recordValueBytes)
	if decodedValue == nil {
		t.Fatal("Decoded value is nil")
	}

	// Should be JSON representation
	decodedStr := string(decodedValue)
	if !containsSubstring(decodedStr, `"user_name":"john_doe"`) {
		t.Errorf("Decoded JSON missing user_name field: %s", decodedStr)
	}
	if !containsSubstring(decodedStr, `"user_age":30`) {
		t.Errorf("Decoded JSON missing user_age field: %s", decodedStr)
	}
	if !containsSubstring(decodedStr, `"is_active":true`) {
		t.Errorf("Decoded JSON missing is_active field: %s", decodedStr)
	}
}

func TestDecodeRecordValueBackwardCompatibility(t *testing.T) {
	handler := &Handler{}

	// Test with raw bytes (not RecordValue)
	rawBytes := []byte("raw-kafka-message")
	decodedValue := handler.decodeRecordValueToKafkaMessage(rawBytes)

	if decodedValue == nil {
		t.Fatal("Decoded value is nil")
	}

	if string(decodedValue) != string(rawBytes) {
		t.Errorf("Expected raw bytes '%s', got '%s'", string(rawBytes), string(decodedValue))
	}
}

func TestRecordValueToJSON(t *testing.T) {
	handler := &Handler{}

	recordValue := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"string_field": {
				Kind: &schema_pb.Value_StringValue{StringValue: "test"},
			},
			"int_field": {
				Kind: &schema_pb.Value_Int32Value{Int32Value: 42},
			},
			"bool_field": {
				Kind: &schema_pb.Value_BoolValue{BoolValue: true},
			},
			"bytes_field": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("bytes")},
			},
		},
	}

	jsonBytes := handler.recordValueToJSON(recordValue)
	jsonStr := string(jsonBytes)

	// Check that all fields are present (order may vary)
	if !containsSubstring(jsonStr, `"string_field":"test"`) {
		t.Errorf("JSON missing string field: %s", jsonStr)
	}
	if !containsSubstring(jsonStr, `"int_field":42`) {
		t.Errorf("JSON missing int field: %s", jsonStr)
	}
	if !containsSubstring(jsonStr, `"bool_field":true`) {
		t.Errorf("JSON missing bool field: %s", jsonStr)
	}
	if !containsSubstring(jsonStr, `"bytes_field":"bytes"`) {
		t.Errorf("JSON missing bytes field: %s", jsonStr)
	}
}

// Helper function to check if a string contains a substring
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr) >= 0
}

func findSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
