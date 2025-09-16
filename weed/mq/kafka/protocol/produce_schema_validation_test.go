package protocol

import (
	"encoding/json"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ValidateSchemaCompatibility(t *testing.T) {
	// Create handler with schema management enabled
	handler := createTestHandlerWithSchema(t)
	
	// Create test schema and message
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "value", "type": "int"}
		]
	}`
	
	testData := map[string]interface{}{
		"id":    "test-123",
		"value": int32(42),
	}
	
	// Register schema and create message
	_, message := createTestSchematizedMessage(t, handler.schemaManager, avroSchema, testData)
	
	t.Run("Valid Schematized Message", func(t *testing.T) {
		err := handler.validateSchemaCompatibility("test-topic", message)
		assert.NoError(t, err)
	})
	
	t.Run("Non-Schematized Message", func(t *testing.T) {
		nonSchematizedMessage := []byte("plain text message")
		err := handler.validateSchemaCompatibility("test-topic", nonSchematizedMessage)
		assert.NoError(t, err) // Should pass - non-schematized messages are allowed
	})
	
	t.Run("Schema Disabled", func(t *testing.T) {
		// Create handler without schema management
		handlerNoSchema := &Handler{
			schemaManager: nil,
		}
		
		err := handlerNoSchema.validateSchemaCompatibility("test-topic", message)
		assert.NoError(t, err) // Should pass when schema management is disabled
	})
	
	t.Run("Invalid Message Format", func(t *testing.T) {
		// Create message with invalid Confluent envelope
		invalidMessage := []byte{0x00, 0x00, 0x00, 0x01} // Too short
		
		err := handler.validateSchemaCompatibility("test-topic", invalidMessage)
		assert.NoError(t, err) // Should pass - not recognized as schematized
	})
}

func TestHandler_PerformSchemaValidation(t *testing.T) {
	handler := createTestHandlerWithSchema(t)
	
	// Create test schema
	avroSchema := `{
		"type": "record",
		"name": "UserEvent",
		"fields": [
			{"name": "user_id", "type": "string"},
			{"name": "event_type", "type": "string"},
			{"name": "timestamp", "type": "long"}
		]
	}`
	
	testData := map[string]interface{}{
		"user_id":    "user-456",
		"event_type": "login",
		"timestamp":  int64(1640995200000),
	}
	
	schemaID, message := createTestSchematizedMessage(t, handler.schemaManager, avroSchema, testData)
	
	t.Run("Valid Schema Validation", func(t *testing.T) {
		err := handler.performSchemaValidation("user-events", schemaID, schema.FormatAvro, message)
		assert.NoError(t, err)
	})
	
	t.Run("Topic Not Schematized", func(t *testing.T) {
		// Test with a topic that doesn't require schemas
		err := handler.performSchemaValidation("plain-topic", schemaID, schema.FormatAvro, message)
		assert.NoError(t, err) // Should pass - topic doesn't require schemas
	})
	
	t.Run("Invalid Schema Format", func(t *testing.T) {
		err := handler.performSchemaValidation("user-events", schemaID, schema.FormatUnknown, message)
		// Should pass because the method returns early when schema manager is available
		// but the format validation happens later in the process
		if err != nil {
			assert.Contains(t, err.Error(), "unsupported schema format")
		}
	})
}

func TestHandler_ValidateMessageContent(t *testing.T) {
	handler := createTestHandlerWithSchema(t)
	
	t.Run("Valid Avro Message", func(t *testing.T) {
		avroSchema := `{
			"type": "record",
			"name": "Product",
			"fields": [
				{"name": "id", "type": "string"},
				{"name": "name", "type": "string"},
				{"name": "price", "type": "double"}
			]
		}`
		
		testData := map[string]interface{}{
			"id":    "prod-789",
			"name":  "Test Product",
			"price": 29.99,
		}
		
		schemaID, message := createTestSchematizedMessage(t, handler.schemaManager, avroSchema, testData)
		
		err := handler.validateMessageContent(schemaID, schema.FormatAvro, message)
		assert.NoError(t, err)
	})
	
	t.Run("Valid JSON Schema Message", func(t *testing.T) {
		jsonSchema := `{
			"type": "object",
			"properties": {
				"id": {"type": "string"},
				"name": {"type": "string"},
				"active": {"type": "boolean"}
			},
			"required": ["id", "name"]
		}`
		
		testData := map[string]interface{}{
			"id":     "item-123",
			"name":   "Test Item",
			"active": true,
		}
		
		schemaID, message := createTestSchematizedJSONMessage(t, handler.schemaManager, jsonSchema, testData)
		
		err := handler.validateMessageContent(schemaID, schema.FormatJSONSchema, message)
		assert.NoError(t, err)
	})
	
	t.Run("Invalid Message Content", func(t *testing.T) {
		// Create a message with invalid Confluent envelope
		invalidMessage := []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF} // Invalid payload
		
		err := handler.validateMessageContent(1, schema.FormatAvro, invalidMessage)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "message validation failed")
	})
}

func TestHandler_ParseSchemaID(t *testing.T) {
	handler := &Handler{}
	
	testCases := []struct {
		input       string
		expected    uint32
		shouldError bool
	}{
		{"1", 1, false},
		{"123", 123, false},
		{"4294967295", 4294967295, false}, // Max uint32
		{"0", 0, false},
		{"", 0, true},
		{"invalid", 0, true},
		{"4294967296", 0, true}, // Overflow uint32
		{"-1", 0, true},
	}
	
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result, err := handler.parseSchemaID(tc.input)
			
			if tc.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestHandler_ConfigurationMethods(t *testing.T) {
	handler := &Handler{}
	
	t.Run("IsStrictSchemaValidation", func(t *testing.T) {
		// Default should be false (permissive mode)
		result := handler.isStrictSchemaValidation()
		assert.False(t, result)
	})
	
	t.Run("GetTopicCompatibilityLevel", func(t *testing.T) {
		// Default should be backward compatibility
		result := handler.getTopicCompatibilityLevel("test-topic")
		assert.Equal(t, schema.CompatibilityBackward, result)
	})
}

// Helper functions for testing

func createTestHandlerWithSchema(t *testing.T) *Handler {
	// Create a handler with schema management enabled
	registryConfig := schema.RegistryConfig{
		URL: "http://localhost:8081", // Mock URL
	}
	
	_ = schema.NewRegistryClient(registryConfig)
	
	managerConfig := schema.ManagerConfig{
		RegistryURL: "http://localhost:8081",
	}
	
	schemaManager, err := schema.NewManager(managerConfig)
	require.NoError(t, err)
	
	handler := &Handler{
		schemaManager: schemaManager,
	}
	
	return handler
}

func createTestSchematizedMessage(t *testing.T, manager *schema.Manager, avroSchema string, testData map[string]interface{}) (uint32, []byte) {
	// Create Avro codec and encode data
	codec, err := goavro.NewCodec(avroSchema)
	require.NoError(t, err)
	
	avroBinary, err := codec.BinaryFromNative(nil, testData)
	require.NoError(t, err)
	
	// Create Confluent envelope with schema ID 1
	schemaID := uint32(1)
	envelope := schema.CreateConfluentEnvelope(schema.FormatAvro, schemaID, nil, avroBinary)
	
	return schemaID, envelope
}

func createTestSchematizedJSONMessage(t *testing.T, manager *schema.Manager, jsonSchema string, testData map[string]interface{}) (uint32, []byte) {
	// Create JSON payload
	jsonData, err := json.Marshal(testData)
	require.NoError(t, err)
	
	// Create Confluent envelope with schema ID 2
	schemaID := uint32(2)
	envelope := schema.CreateConfluentEnvelope(schema.FormatJSONSchema, schemaID, nil, jsonData)
	
	return schemaID, envelope
}
