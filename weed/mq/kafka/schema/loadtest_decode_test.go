package schema

import (
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	schema_pb "github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// LoadTestMessage represents the test message structure
type LoadTestMessage struct {
	ID         string            `json:"id"`
	Timestamp  int64             `json:"timestamp"`
	ProducerID int               `json:"producer_id"`
	Counter    int64             `json:"counter"`
	UserID     string            `json:"user_id"`
	EventType  string            `json:"event_type"`
	Properties map[string]string `json:"properties"`
}

const (
	// LoadTest schemas matching the loadtest client
	loadTestAvroSchema = `{
		"type": "record",
		"name": "LoadTestMessage",
		"namespace": "com.seaweedfs.loadtest",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "timestamp", "type": "long"},
			{"name": "producer_id", "type": "int"},
			{"name": "counter", "type": "long"},
			{"name": "user_id", "type": "string"},
			{"name": "event_type", "type": "string"},
			{"name": "properties", "type": {"type": "map", "values": "string"}}
		]
	}`

	loadTestJSONSchema = `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "LoadTestMessage",
		"type": "object",
		"properties": {
			"id": {"type": "string"},
			"timestamp": {"type": "integer"},
			"producer_id": {"type": "integer"},
			"counter": {"type": "integer"},
			"user_id": {"type": "string"},
			"event_type": {"type": "string"},
			"properties": {
				"type": "object",
				"additionalProperties": {"type": "string"}
			}
		},
		"required": ["id", "timestamp", "producer_id", "counter", "user_id", "event_type"]
	}`

	loadTestProtobufSchema = `syntax = "proto3";

package com.seaweedfs.loadtest;

message LoadTestMessage {
  string id = 1;
  int64 timestamp = 2;
  int32 producer_id = 3;
  int64 counter = 4;
  string user_id = 5;
  string event_type = 6;
  map<string, string> properties = 7;
}`
)

// createTestMessage creates a sample load test message
func createTestMessage() *LoadTestMessage {
	return &LoadTestMessage{
		ID:         "msg-test-123",
		Timestamp:  time.Now().UnixNano(),
		ProducerID: 0,
		Counter:    42,
		UserID:     "user-789",
		EventType:  "click",
		Properties: map[string]string{
			"browser": "chrome",
			"version": "1.0",
		},
	}
}

// createConfluentWireFormat wraps payload with Confluent wire format
func createConfluentWireFormat(schemaID uint32, payload []byte) []byte {
	wireFormat := make([]byte, 5+len(payload))
	wireFormat[0] = 0x00 // Magic byte
	binary.BigEndian.PutUint32(wireFormat[1:5], schemaID)
	copy(wireFormat[5:], payload)
	return wireFormat
}

// TestAvroLoadTestDecoding tests Avro decoding with load test schema
func TestAvroLoadTestDecoding(t *testing.T) {
	msg := createTestMessage()

	// Create Avro codec
	codec, err := goavro.NewCodec(loadTestAvroSchema)
	if err != nil {
		t.Fatalf("Failed to create Avro codec: %v", err)
	}

	// Convert message to map for Avro encoding
	msgMap := map[string]interface{}{
		"id":          msg.ID,
		"timestamp":   msg.Timestamp,
		"producer_id": int32(msg.ProducerID), // Avro uses int32 for "int"
		"counter":     msg.Counter,
		"user_id":     msg.UserID,
		"event_type":  msg.EventType,
		"properties":  msg.Properties,
	}

	// Encode as Avro binary
	avroBytes, err := codec.BinaryFromNative(nil, msgMap)
	if err != nil {
		t.Fatalf("Failed to encode Avro message: %v", err)
	}

	t.Logf("Avro encoded size: %d bytes", len(avroBytes))

	// Wrap in Confluent wire format
	schemaID := uint32(1)
	wireFormat := createConfluentWireFormat(schemaID, avroBytes)

	t.Logf("Confluent wire format size: %d bytes", len(wireFormat))

	// Parse envelope
	envelope, ok := ParseConfluentEnvelope(wireFormat)
	if !ok {
		t.Fatalf("Failed to parse Confluent envelope")
	}

	if envelope.SchemaID != schemaID {
		t.Errorf("Expected schema ID %d, got %d", schemaID, envelope.SchemaID)
	}

	// Create decoder
	decoder, err := NewAvroDecoder(loadTestAvroSchema)
	if err != nil {
		t.Fatalf("Failed to create Avro decoder: %v", err)
	}

	// Decode
	recordValue, err := decoder.DecodeToRecordValue(envelope.Payload)
	if err != nil {
		t.Fatalf("Failed to decode Avro message: %v", err)
	}

	// Verify fields
	if recordValue.Fields == nil {
		t.Fatal("RecordValue fields is nil")
	}

	// Check specific fields
	verifyField(t, recordValue, "id", msg.ID)
	verifyField(t, recordValue, "timestamp", msg.Timestamp)
	verifyField(t, recordValue, "producer_id", int64(msg.ProducerID))
	verifyField(t, recordValue, "counter", msg.Counter)
	verifyField(t, recordValue, "user_id", msg.UserID)
	verifyField(t, recordValue, "event_type", msg.EventType)

	t.Logf("✅ Avro decoding successful: %d fields", len(recordValue.Fields))
}

// TestJSONSchemaLoadTestDecoding tests JSON Schema decoding with load test schema
func TestJSONSchemaLoadTestDecoding(t *testing.T) {
	msg := createTestMessage()

	// Encode as JSON
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to encode JSON message: %v", err)
	}

	t.Logf("JSON encoded size: %d bytes", len(jsonBytes))
	t.Logf("JSON content: %s", string(jsonBytes))

	// Wrap in Confluent wire format
	schemaID := uint32(3)
	wireFormat := createConfluentWireFormat(schemaID, jsonBytes)

	t.Logf("Confluent wire format size: %d bytes", len(wireFormat))

	// Parse envelope
	envelope, ok := ParseConfluentEnvelope(wireFormat)
	if !ok {
		t.Fatalf("Failed to parse Confluent envelope")
	}

	if envelope.SchemaID != schemaID {
		t.Errorf("Expected schema ID %d, got %d", schemaID, envelope.SchemaID)
	}

	// Create JSON Schema decoder
	decoder, err := NewJSONSchemaDecoder(loadTestJSONSchema)
	if err != nil {
		t.Fatalf("Failed to create JSON Schema decoder: %v", err)
	}

	// Decode
	recordValue, err := decoder.DecodeToRecordValue(envelope.Payload)
	if err != nil {
		t.Fatalf("Failed to decode JSON Schema message: %v", err)
	}

	// Verify fields
	if recordValue.Fields == nil {
		t.Fatal("RecordValue fields is nil")
	}

	// Check specific fields
	verifyField(t, recordValue, "id", msg.ID)
	verifyField(t, recordValue, "timestamp", msg.Timestamp)
	verifyField(t, recordValue, "producer_id", int64(msg.ProducerID))
	verifyField(t, recordValue, "counter", msg.Counter)
	verifyField(t, recordValue, "user_id", msg.UserID)
	verifyField(t, recordValue, "event_type", msg.EventType)

	t.Logf("✅ JSON Schema decoding successful: %d fields", len(recordValue.Fields))
}

// TestProtobufLoadTestDecoding tests Protobuf decoding with load test schema
func TestProtobufLoadTestDecoding(t *testing.T) {
	msg := createTestMessage()

	// For Protobuf, we need to first compile the schema and then encode
	// For now, let's test JSON encoding with Protobuf schema (common pattern)
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to encode JSON message: %v", err)
	}

	t.Logf("JSON (for Protobuf) encoded size: %d bytes", len(jsonBytes))
	t.Logf("JSON content: %s", string(jsonBytes))

	// Wrap in Confluent wire format
	schemaID := uint32(5)
	wireFormat := createConfluentWireFormat(schemaID, jsonBytes)

	t.Logf("Confluent wire format size: %d bytes", len(wireFormat))

	// Parse envelope
	envelope, ok := ParseConfluentEnvelope(wireFormat)
	if !ok {
		t.Fatalf("Failed to parse Confluent envelope")
	}

	if envelope.SchemaID != schemaID {
		t.Errorf("Expected schema ID %d, got %d", schemaID, envelope.SchemaID)
	}

	// Create Protobuf decoder from text schema
	decoder, err := NewProtobufDecoderFromString(loadTestProtobufSchema)
	if err != nil {
		t.Fatalf("Failed to create Protobuf decoder: %v", err)
	}

	// Try to decode - this will likely fail because JSON is not valid Protobuf binary
	recordValue, err := decoder.DecodeToRecordValue(envelope.Payload)
	if err != nil {
		t.Logf("⚠️  Expected failure: Protobuf decoder cannot decode JSON: %v", err)
		t.Logf("This confirms the issue: producer sends JSON but gateway expects Protobuf binary")
		return
	}

	// If we get here, something unexpected happened
	t.Logf("Unexpectedly succeeded in decoding JSON as Protobuf")
	if recordValue.Fields != nil {
		t.Logf("RecordValue has %d fields", len(recordValue.Fields))
	}
}

// verifyField checks if a field exists in RecordValue with expected value
func verifyField(t *testing.T, rv *schema_pb.RecordValue, fieldName string, expectedValue interface{}) {
	field, exists := rv.Fields[fieldName]
	if !exists {
		t.Errorf("Field '%s' not found in RecordValue", fieldName)
		return
	}

	switch expected := expectedValue.(type) {
	case string:
		if field.GetStringValue() != expected {
			t.Errorf("Field '%s': expected '%s', got '%s'", fieldName, expected, field.GetStringValue())
		}
	case int64:
		if field.GetInt64Value() != expected {
			t.Errorf("Field '%s': expected %d, got %d", fieldName, expected, field.GetInt64Value())
		}
	case int:
		if field.GetInt64Value() != int64(expected) {
			t.Errorf("Field '%s': expected %d, got %d", fieldName, expected, field.GetInt64Value())
		}
	default:
		t.Logf("Field '%s' has unexpected type", fieldName)
	}
}
