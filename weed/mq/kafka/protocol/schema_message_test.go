package protocol

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/protobuf/proto"
)

func TestCreateRecordValueFromKafkaMessage(t *testing.T) {
	handler := &Handler{}

	key := []byte("test-key")
	value := []byte("test-value")

	recordValue, err := handler.createRecordValueFromKafkaMessage(key, value)
	if err != nil {
		t.Fatalf("Failed to create RecordValue: %v", err)
	}

	// Verify structure
	if recordValue.Fields == nil {
		t.Fatal("RecordValue.Fields is nil")
	}

	// Check key field
	keyField, exists := recordValue.Fields["key"]
	if !exists {
		t.Fatal("Missing 'key' field in RecordValue")
	}
	if keyValue, ok := keyField.Kind.(*schema_pb.Value_BytesValue); ok {
		if string(keyValue.BytesValue) != "test-key" {
			t.Errorf("Expected key 'test-key', got '%s'", string(keyValue.BytesValue))
		}
	} else {
		t.Errorf("Key field is not BytesValue, got %T", keyField.Kind)
	}

	// Check value field
	valueField, exists := recordValue.Fields["value"]
	if !exists {
		t.Fatal("Missing 'value' field in RecordValue")
	}
	if valueValue, ok := valueField.Kind.(*schema_pb.Value_BytesValue); ok {
		if string(valueValue.BytesValue) != "test-value" {
			t.Errorf("Expected value 'test-value', got '%s'", string(valueValue.BytesValue))
		}
	} else {
		t.Errorf("Value field is not BytesValue, got %T", valueField.Kind)
	}

	// Check timestamp field
	timestampField, exists := recordValue.Fields["timestamp"]
	if !exists {
		t.Fatal("Missing 'timestamp' field in RecordValue")
	}
	if _, ok := timestampField.Kind.(*schema_pb.Value_TimestampValue); !ok {
		t.Errorf("Timestamp field is not TimestampValue, got %T", timestampField.Kind)
	}
}

func TestDecodeRecordValueToKafkaMessage(t *testing.T) {
	handler := &Handler{}

	// Create a test RecordValue
	originalValue := []byte("original-kafka-message")
	recordValue := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"key": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("test-key")},
			},
			"value": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: originalValue},
			},
			"timestamp": {
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: time.Now().UnixNano() / 1000,
						IsUtc:           true,
					},
				},
			},
		},
	}

	// Marshal to bytes
	recordValueBytes, err := proto.Marshal(recordValue)
	if err != nil {
		t.Fatalf("Failed to marshal RecordValue: %v", err)
	}

	// Decode back to Kafka message
	decodedValue := handler.decodeRecordValueToKafkaMessage(recordValueBytes)
	if decodedValue == nil {
		t.Fatal("Decoded value is nil")
	}

	if string(decodedValue) != string(originalValue) {
		t.Errorf("Expected decoded value '%s', got '%s'", string(originalValue), string(decodedValue))
	}
}

func TestDecodeRecordValueWithSchematizedMessage(t *testing.T) {
	handler := &Handler{}

	// Create a RecordValue with nested schematized message
	nestedRecord := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"name": {
				Kind: &schema_pb.Value_StringValue{StringValue: "John Doe"},
			},
			"age": {
				Kind: &schema_pb.Value_Int32Value{Int32Value: 30},
			},
		},
	}

	recordValue := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"key": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("user-123")},
			},
			"value": {
				Kind: &schema_pb.Value_RecordValue{RecordValue: nestedRecord},
			},
			"timestamp": {
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: time.Now().UnixNano() / 1000,
						IsUtc:           true,
					},
				},
			},
			"schema_id": {
				Kind: &schema_pb.Value_Int32Value{Int32Value: 123},
			},
			"schema_format": {
				Kind: &schema_pb.Value_StringValue{StringValue: "JSON_SCHEMA"},
			},
		},
	}

	// Marshal to bytes
	recordValueBytes, err := proto.Marshal(recordValue)
	if err != nil {
		t.Fatalf("Failed to marshal RecordValue: %v", err)
	}

	// Decode back to Kafka message
	decodedValue := handler.decodeRecordValueToKafkaMessage(recordValueBytes)
	if decodedValue == nil {
		t.Fatal("Decoded value is nil")
	}

	// Should return JSON representation since schema manager is not enabled
	decodedStr := string(decodedValue)

	// Check that it contains the expected fields (order may vary)
	if !containsSubstring(decodedStr, `"name":"John Doe"`) {
		t.Errorf("Decoded JSON missing name field: %s", decodedStr)
	}
	if !containsSubstring(decodedStr, `"age":30`) {
		t.Errorf("Decoded JSON missing age field: %s", decodedStr)
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

func TestProduceSchemaBasedRecordRoundTrip(t *testing.T) {
	// This test verifies the complete round-trip: Kafka message -> RecordValue -> Kafka message
	handler := &Handler{}

	originalKey := []byte("test-key")
	originalValue := []byte(`{"message":"hello world"}`)

	// Step 1: Create RecordValue from Kafka message
	recordValue, err := handler.createRecordValueFromKafkaMessage(originalKey, originalValue)
	if err != nil {
		t.Fatalf("Failed to create RecordValue: %v", err)
	}

	// Step 2: Marshal RecordValue to bytes (simulating storage in SMQ)
	recordValueBytes, err := proto.Marshal(recordValue)
	if err != nil {
		t.Fatalf("Failed to marshal RecordValue: %v", err)
	}

	// Step 3: Decode RecordValue back to Kafka message
	decodedValue := handler.decodeRecordValueToKafkaMessage(recordValueBytes)
	if decodedValue == nil {
		t.Fatal("Decoded value is nil")
	}

	// Step 4: Verify round-trip integrity
	if string(decodedValue) != string(originalValue) {
		t.Errorf("Round-trip failed: expected '%s', got '%s'", string(originalValue), string(decodedValue))
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
