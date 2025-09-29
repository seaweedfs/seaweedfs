package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProduceHandler_SchemaIntegration tests the Produce handler with schema integration
func TestProduceHandler_SchemaIntegration(t *testing.T) {
	// Create mock schema registry
	registry := createProduceTestRegistry(t)
	defer registry.Close()

	// Create handler with schema management
	handler := NewTestHandler()
	defer handler.Close()

	// Enable schema management
	err := handler.EnableSchemaManagement(schema.ManagerConfig{
		RegistryURL: registry.URL,
	})
	require.NoError(t, err)

	// For this test, don't enable broker integration to avoid connection issues
	// We're testing schema processing, not broker connectivity

	t.Run("Schematized Message Processing", func(t *testing.T) {
		schemaID := int32(1)
		schemaJSON := `{
			"type": "record",
			"name": "TestMessage",
			"fields": [
				{"name": "id", "type": "string"},
				{"name": "message", "type": "string"}
			]
		}`

		// Register schema
		registerProduceTestSchema(t, registry, schemaID, schemaJSON)

		// Create test data
		testData := map[string]interface{}{
			"id":      "test-123",
			"message": "Hello Schema World",
		}

		// Encode with Avro
		codec, err := goavro.NewCodec(schemaJSON)
		require.NoError(t, err)
		avroBinary, err := codec.BinaryFromNative(nil, testData)
		require.NoError(t, err)

		// Create Confluent envelope
		envelope := createProduceTestEnvelope(schemaID, avroBinary)

		// Test schema processing (without broker integration)
		testKey := []byte("test-key")
		err = handler.processSchematizedMessage("test-topic", 0, testKey, envelope)
		require.NoError(t, err)

		// Verify handler state (schema enabled but no broker integration for this test)
		assert.True(t, handler.IsSchemaEnabled())
		assert.False(t, handler.IsBrokerIntegrationEnabled())
	})

	t.Run("Non-Schematized Message Processing", func(t *testing.T) {
		// Test with raw message
		rawMessage := []byte("This is not schematized")

		// Should not fail, just skip schema processing
		testKey := []byte("raw-key")
		err := handler.processSchematizedMessage("test-topic", 0, testKey, rawMessage)
		require.NoError(t, err)
	})

	t.Run("Schema Validation", func(t *testing.T) {
		schemaID := int32(2)
		schemaJSON := `{
			"type": "record",
			"name": "ValidationTest",
			"fields": [
				{"name": "value", "type": "int"}
			]
		}`

		registerProduceTestSchema(t, registry, schemaID, schemaJSON)

		// Create valid test data
		testData := map[string]interface{}{
			"value": int32(42),
		}

		codec, err := goavro.NewCodec(schemaJSON)
		require.NoError(t, err)
		avroBinary, err := codec.BinaryFromNative(nil, testData)
		require.NoError(t, err)

		envelope := createProduceTestEnvelope(schemaID, avroBinary)

		// Test schema compatibility validation
		err = handler.validateSchemaCompatibility("validation-topic", envelope)
		require.NoError(t, err)
	})

	t.Run("Error Handling", func(t *testing.T) {
		// Test with invalid schema ID
		invalidEnvelope := createProduceTestEnvelope(999, []byte("invalid"))

		testKey := []byte("error-key")
		err := handler.processSchematizedMessage("error-topic", 0, testKey, invalidEnvelope)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema decoding failed")
	})
}

// TestProduceHandler_BrokerIntegration tests broker integration functionality
func TestProduceHandler_BrokerIntegration(t *testing.T) {
	registry := createProduceTestRegistry(t)
	defer registry.Close()

	handler := NewTestHandler()
	defer handler.Close()

	t.Run("Enable Broker Integration", func(t *testing.T) {
		// Should fail without schema management
		err := handler.EnableBrokerIntegration([]string{"localhost:17777"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema management must be enabled")

		// Enable schema management first
		err = handler.EnableSchemaManagement(schema.ManagerConfig{
			RegistryURL: registry.URL,
		})
		require.NoError(t, err)

		// Now broker integration should work (but may fail in tests due to missing broker)
		err = handler.EnableBrokerIntegration([]string{"localhost:17777"})
		require.NoError(t, err)

		assert.True(t, handler.IsBrokerIntegrationEnabled())
	})

	t.Run("Disable Schema Management", func(t *testing.T) {
		// Enable both
		err := handler.EnableSchemaManagement(schema.ManagerConfig{
			RegistryURL: registry.URL,
		})
		require.NoError(t, err)

		err = handler.EnableBrokerIntegration([]string{"localhost:17777"})
		require.NoError(t, err)

		// Disable should clean up both
		handler.DisableSchemaManagement()

		assert.False(t, handler.IsSchemaEnabled())
		assert.False(t, handler.IsBrokerIntegrationEnabled())
	})
}

// TestProduceHandler_MessageExtraction tests message extraction from record sets
func TestProduceHandler_MessageExtraction(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	t.Run("Extract Messages From Record Set", func(t *testing.T) {
		// Create a mock record set (arbitrary data)
		recordSet := []byte("mock-record-set-data-with-sufficient-length-for-testing")

		messages, err := handler.extractMessagesFromRecordSet(recordSet)
		require.NoError(t, err)
		assert.Equal(t, 1, len(messages))
		assert.Equal(t, recordSet, messages[0])
	})

	t.Run("Extract Messages Error Handling", func(t *testing.T) {
		// Too short record set
		shortRecordSet := []byte("short")

		_, err := handler.extractMessagesFromRecordSet(shortRecordSet)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "record set too small")
	})
}

// Helper functions for produce schema tests

func createProduceTestRegistry(t *testing.T) *httptest.Server {
	schemas := make(map[int32]string)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subjects":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
		default:
			// Handle schema requests
			var schemaID int32
			if n, err := fmt.Sscanf(r.URL.Path, "/schemas/ids/%d", &schemaID); n == 1 && err == nil {
				if schema, exists := schemas[schemaID]; exists {
					response := fmt.Sprintf(`{"schema": %q}`, schema)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(response))
				} else {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(`{"error_code": 40403, "message": "Schema not found"}`))
				}
			} else if r.Method == "POST" && r.URL.Path == "/register-schema" {
				var req struct {
					SchemaID int32  `json:"schema_id"`
					Schema   string `json:"schema"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
					schemas[req.SchemaID] = req.Schema
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"success": true}`))
				} else {
					w.WriteHeader(http.StatusBadRequest)
				}
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}
	}))
}

func registerProduceTestSchema(t *testing.T, registry *httptest.Server, schemaID int32, schema string) {
	reqBody := fmt.Sprintf(`{"schema_id": %d, "schema": %q}`, schemaID, schema)
	resp, err := http.Post(registry.URL+"/register-schema", "application/json", bytes.NewReader([]byte(reqBody)))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func createProduceTestEnvelope(schemaID int32, data []byte) []byte {
	envelope := make([]byte, 5+len(data))
	envelope[0] = 0x00 // Magic byte
	binary.BigEndian.PutUint32(envelope[1:5], uint32(schemaID))
	copy(envelope[5:], data)
	return envelope
}
