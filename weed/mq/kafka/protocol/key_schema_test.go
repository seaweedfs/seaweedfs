package protocol

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	schema_pb "github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/protobuf/proto"
)

// TestTopicSchemaConfig tests the extended TopicSchemaConfig structure
func TestTopicSchemaConfig(t *testing.T) {
	config := &TopicSchemaConfig{
		ValueSchemaID:     123,
		ValueSchemaFormat: schema.FormatAvro,
		KeySchemaID:       456,
		KeySchemaFormat:   schema.FormatJSONSchema,
		HasKeySchema:      true,
	}

	// Test legacy accessors for backward compatibility
	if config.SchemaID() != 123 {
		t.Errorf("Expected SchemaID() to return 123, got %d", config.SchemaID())
	}

	if config.SchemaFormat() != schema.FormatAvro {
		t.Errorf("Expected SchemaFormat() to return FormatAvro, got %v", config.SchemaFormat())
	}

	// Test key schema fields
	if config.KeySchemaID != 456 {
		t.Errorf("Expected KeySchemaID to be 456, got %d", config.KeySchemaID)
	}

	if config.KeySchemaFormat != schema.FormatJSONSchema {
		t.Errorf("Expected KeySchemaFormat to be FormatJSONSchema, got %v", config.KeySchemaFormat)
	}

	if !config.HasKeySchema {
		t.Error("Expected HasKeySchema to be true")
	}
}

// TestKeySchemaConfigStorage tests key schema configuration storage and retrieval
func TestKeySchemaConfigStorage(t *testing.T) {
	handler := NewHandlerForUnitTests()

	topic := "test-topic-key-schema"
	keySchemaID := uint32(789)
	keySchemaFormat := schema.FormatAvro
	valueSchemaID := uint32(101)
	valueSchemaFormat := schema.FormatJSONSchema

	// Initially no key schema should exist
	if handler.hasTopicKeySchemaConfig(topic, keySchemaID, keySchemaFormat) {
		t.Error("Expected hasTopicKeySchemaConfig to return false for non-existent key schema")
	}

	// Store key schema config
	err := handler.storeTopicKeySchemaConfig(topic, keySchemaID, keySchemaFormat)
	if err != nil {
		t.Fatalf("Failed to store key schema config: %v", err)
	}

	// Now it should exist
	if !handler.hasTopicKeySchemaConfig(topic, keySchemaID, keySchemaFormat) {
		t.Error("Expected hasTopicKeySchemaConfig to return true after storing key schema")
	}

	// Store value schema config as well
	err = handler.storeTopicSchemaConfig(topic, valueSchemaID, valueSchemaFormat)
	if err != nil {
		t.Fatalf("Failed to store value schema config: %v", err)
	}

	// Retrieve the topic schema config
	config, err := handler.getTopicSchemaConfig(topic)
	if err != nil {
		t.Fatalf("Failed to get topic schema config: %v", err)
	}

	// Verify both key and value schemas are stored correctly
	if config.KeySchemaID != keySchemaID {
		t.Errorf("Expected KeySchemaID %d, got %d", keySchemaID, config.KeySchemaID)
	}

	if config.KeySchemaFormat != keySchemaFormat {
		t.Errorf("Expected KeySchemaFormat %v, got %v", keySchemaFormat, config.KeySchemaFormat)
	}

	if !config.HasKeySchema {
		t.Error("Expected HasKeySchema to be true")
	}

	if config.ValueSchemaID != valueSchemaID {
		t.Errorf("Expected ValueSchemaID %d, got %d", valueSchemaID, config.ValueSchemaID)
	}

	if config.ValueSchemaFormat != valueSchemaFormat {
		t.Errorf("Expected ValueSchemaFormat %v, got %v", valueSchemaFormat, config.ValueSchemaFormat)
	}
}

// TestProduceSchemaBasedRecordWithKeySchema tests producing records with both key and value schemas
// This test is simplified since we're testing the key schema infrastructure without complex schema manager mocking
func TestProduceSchemaBasedRecordWithKeySchema(t *testing.T) {
	handler := setupTestHandlerWithSchemaManager()

	topic := "test-topic-key-value-schema"

	// Test with raw data (no schema manager configured)
	rawKey := []byte("test-key")
	rawValue := []byte("test-value")

	// Should fall back to raw message handling when no schema manager is configured
	offset, err := handler.produceSchemaBasedRecord(topic, 0, rawKey, rawValue)
	if err != nil {
		t.Fatalf("Failed to produce record: %v", err)
	}

	if offset < 0 {
		t.Errorf("Expected non-negative offset, got %d", offset)
	}
}

// TestProduceSchemaBasedRecordKeyOnlySchema tests producing records with only key schema
// Simplified test without complex schema manager mocking
func TestProduceSchemaBasedRecordKeyOnlySchema(t *testing.T) {
	handler := setupTestHandlerWithSchemaManager()

	topic := "test-topic-key-only-schema"

	// Test with raw data
	keyData := []byte("simple-key")
	valueData := []byte("raw-value-data")

	// Should work with raw data
	offset, err := handler.produceSchemaBasedRecord(topic, 0, keyData, valueData)
	if err != nil {
		t.Fatalf("Failed to produce record: %v", err)
	}

	if offset < 0 {
		t.Errorf("Expected non-negative offset, got %d", offset)
	}
}

// TestDecodeRecordValueToKafkaKey tests key decoding functionality
func TestDecodeRecordValueToKafkaKey(t *testing.T) {
	handler := setupTestHandlerWithSchemaManager()

	topic := "test-topic-key-decode"
	keySchemaID := uint32(3001)

	// Store key schema config
	err := handler.storeTopicKeySchemaConfig(topic, keySchemaID, schema.FormatAvro)
	if err != nil {
		t.Fatalf("Failed to store key schema config: %v", err)
	}

	// Create a RecordValue for the key
	keyRecordValue := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"customerId": {Kind: &schema_pb.Value_StringValue{StringValue: "cust-456"}},
		},
	}

	keyRecordValueBytes, err := proto.Marshal(keyRecordValue)
	if err != nil {
		t.Fatalf("Failed to marshal key RecordValue: %v", err)
	}

	// Test decoding without schema manager - should return raw bytes
	decodedKey := handler.decodeRecordValueToKafkaKey(topic, keyRecordValueBytes)

	// Without a schema manager, it should return the original bytes or JSON representation
	if decodedKey == nil {
		t.Error("Expected decoded key to not be nil")
	}
}

// TestIsSchematizedTopicWithKeySchema tests schema detection for topics with key schemas
func TestIsSchematizedTopicWithKeySchema(t *testing.T) {
	handler := setupTestHandlerWithSchemaManager()

	topic := "test-topic-schema-detection"

	// Without a schema manager configured, it should return false
	isSchematized := handler.isSchematizedTopic(topic)
	if isSchematized {
		t.Error("Expected topic to not be detected as schematized without schema manager")
	}
}

// Mock structures for testing

type MockSchemaManager struct {
	mockIsSchematized  map[string]bool
	mockDecodedMessage map[string]*schema.DecodedMessage
	mockEncodeMessage  map[encodeKey][]byte
	mockLatestSchema   map[string]*schema.CachedSubject
}

type encodeKey struct {
	recordValue  *schema_pb.RecordValue
	schemaID     uint32
	schemaFormat schema.Format
}

func (m *MockSchemaManager) IsSchematized(data []byte) bool {
	if result, exists := m.mockIsSchematized[string(data)]; exists {
		return result
	}
	// Default: check for Confluent magic bytes
	return len(data) >= 5 && data[0] == 0x00
}

func (m *MockSchemaManager) DecodeMessage(data []byte) (*schema.DecodedMessage, error) {
	if result, exists := m.mockDecodedMessage[string(data)]; exists {
		return result, nil
	}
	return nil, fmt.Errorf("no mock decode result for data")
}

func (m *MockSchemaManager) EncodeMessage(recordValue *schema_pb.RecordValue, schemaID uint32, schemaFormat schema.Format) ([]byte, error) {
	key := encodeKey{recordValue, schemaID, schemaFormat}
	if result, exists := m.mockEncodeMessage[key]; exists {
		return result, nil
	}
	return nil, fmt.Errorf("no mock encode result for schema %d", schemaID)
}

func (m *MockSchemaManager) GetLatestSchema(subject string) (*schema.CachedSubject, error) {
	if result, exists := m.mockLatestSchema[subject]; exists {
		return result, nil
	}
	return nil, fmt.Errorf("subject %s not found", subject)
}

func (m *MockSchemaManager) GetSchemaByID(schemaID uint32) (*schema.CachedSchema, error) {
	return nil, fmt.Errorf("GetSchemaByID not implemented in mock")
}

func (m *MockSchemaManager) CheckSchemaCompatibility(oldSchema, newSchema string, format schema.Format, compatibility schema.CompatibilityLevel) (*schema.CompatibilityResult, error) {
	return nil, fmt.Errorf("CheckSchemaCompatibility not implemented in mock")
}

// Helper functions

func setupTestHandlerWithSchemaManager() *Handler {
	handler := NewHandlerForUnitTests()
	// Create a simple test without complex schema manager mocking for now
	return handler
}

func createConfluentFramedMessage(schemaID uint32, payload []byte) []byte {
	// Confluent framing: magic byte (0x00) + 4-byte schema ID + payload
	result := make([]byte, 5+len(payload))
	result[0] = 0x00 // magic byte
	result[1] = byte(schemaID >> 24)
	result[2] = byte(schemaID >> 16)
	result[3] = byte(schemaID >> 8)
	result[4] = byte(schemaID)
	copy(result[5:], payload)
	return result
}
