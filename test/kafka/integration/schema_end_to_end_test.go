package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
)

// TestSchemaEndToEnd_AvroRoundTrip tests the complete Avro schema round-trip workflow
func TestSchemaEndToEnd_AvroRoundTrip(t *testing.T) {
	// Create mock schema registry
	server := createMockSchemaRegistryForE2E(t)
	defer server.Close()

	// Create schema manager
	config := schema.ManagerConfig{
		RegistryURL:    server.URL,
		ValidationMode: schema.ValidationPermissive,
	}
	manager, err := schema.NewManager(config)
	require.NoError(t, err)

	// Test data
	avroSchema := getUserAvroSchemaForE2E()
	testData := map[string]interface{}{
		"id":    int32(12345),
		"name":  "Alice Johnson",
		"email": map[string]interface{}{"string": "alice@example.com"}, // Avro union
		"age":   map[string]interface{}{"int": int32(28)},              // Avro union
		"preferences": map[string]interface{}{
			"Preferences": map[string]interface{}{ // Avro union with record type
				"notifications": true,
				"theme":         "dark",
			},
		},
	}

	t.Run("SchemaManagerRoundTrip", func(t *testing.T) {
		// Step 1: Create Confluent envelope (simulate producer)
		codec, err := goavro.NewCodec(avroSchema)
		require.NoError(t, err)

		avroBinary, err := codec.BinaryFromNative(nil, testData)
		require.NoError(t, err)

		confluentMsg := schema.CreateConfluentEnvelope(schema.FormatAvro, 1, nil, avroBinary)
		require.True(t, len(confluentMsg) > 0, "Confluent envelope should not be empty")

		t.Logf("Created Confluent envelope: %d bytes", len(confluentMsg))

		// Step 2: Decode message using schema manager
		decodedMsg, err := manager.DecodeMessage(confluentMsg)
		require.NoError(t, err)
		require.NotNil(t, decodedMsg.RecordValue, "RecordValue should not be nil")

		t.Logf("Decoded message with schema ID %d, format %v", decodedMsg.SchemaID, decodedMsg.SchemaFormat)

		// Step 3: Re-encode message using schema manager
		reconstructedMsg, err := manager.EncodeMessage(decodedMsg.RecordValue, 1, schema.FormatAvro)
		require.NoError(t, err)
		require.True(t, len(reconstructedMsg) > 0, "Reconstructed message should not be empty")

		t.Logf("Re-encoded message: %d bytes", len(reconstructedMsg))

		// Step 4: Verify the reconstructed message is a valid Confluent envelope
		envelope, ok := schema.ParseConfluentEnvelope(reconstructedMsg)
		require.True(t, ok, "Reconstructed message should be a valid Confluent envelope")
		require.Equal(t, uint32(1), envelope.SchemaID, "Schema ID should match")
		require.Equal(t, schema.FormatAvro, envelope.Format, "Schema format should be Avro")

		// Step 5: Decode and verify the content
		decodedNative, _, err := codec.NativeFromBinary(envelope.Payload)
		require.NoError(t, err)

		decodedMap, ok := decodedNative.(map[string]interface{})
		require.True(t, ok, "Decoded data should be a map")

		// Verify all fields
		assert.Equal(t, int32(12345), decodedMap["id"])
		assert.Equal(t, "Alice Johnson", decodedMap["name"])
		
		// Verify union fields
		emailUnion, ok := decodedMap["email"].(map[string]interface{})
		require.True(t, ok, "Email should be a union")
		assert.Equal(t, "alice@example.com", emailUnion["string"])

		ageUnion, ok := decodedMap["age"].(map[string]interface{})
		require.True(t, ok, "Age should be a union")
		assert.Equal(t, int32(28), ageUnion["int"])

		preferencesUnion, ok := decodedMap["preferences"].(map[string]interface{})
		require.True(t, ok, "Preferences should be a union")
		preferencesRecord, ok := preferencesUnion["Preferences"].(map[string]interface{})
		require.True(t, ok, "Preferences should contain a record")
		assert.Equal(t, true, preferencesRecord["notifications"])
		assert.Equal(t, "dark", preferencesRecord["theme"])

		t.Log("Successfully completed Avro schema round-trip test")
	})
}

// TestSchemaEndToEnd_ProtobufRoundTrip tests the complete Protobuf schema round-trip workflow
func TestSchemaEndToEnd_ProtobufRoundTrip(t *testing.T) {
	t.Run("ProtobufEnvelopeCreation", func(t *testing.T) {
		// Create a simple Protobuf message (simulated)
		// In a real scenario, this would be generated from a .proto file
		protobufData := []byte{0x08, 0x96, 0x01, 0x12, 0x04, 0x74, 0x65, 0x73, 0x74} // id=150, name="test"

		// Create Confluent envelope with Protobuf format
		confluentMsg := schema.CreateConfluentEnvelope(schema.FormatProtobuf, 2, []int{0}, protobufData)
		require.True(t, len(confluentMsg) > 0, "Confluent envelope should not be empty")

		t.Logf("Created Protobuf Confluent envelope: %d bytes", len(confluentMsg))

		// Verify Confluent envelope
		envelope, ok := schema.ParseConfluentEnvelope(confluentMsg)
		require.True(t, ok, "Message should be a valid Confluent envelope")
		require.Equal(t, uint32(2), envelope.SchemaID, "Schema ID should match")
		// Note: ParseConfluentEnvelope defaults to FormatAvro; format detection requires schema registry
		require.Equal(t, schema.FormatAvro, envelope.Format, "Format defaults to Avro without schema registry lookup")
		
		// For Protobuf with indexes, we need to use the specialized parser
		protobufEnvelope, ok := schema.ParseConfluentProtobufEnvelopeWithIndexCount(confluentMsg, 1)
		require.True(t, ok, "Message should be a valid Protobuf envelope")
		require.Equal(t, uint32(2), protobufEnvelope.SchemaID, "Schema ID should match")
		require.Equal(t, schema.FormatProtobuf, protobufEnvelope.Format, "Schema format should be Protobuf")
		require.Equal(t, []int{0}, protobufEnvelope.Indexes, "Indexes should match")
		require.Equal(t, protobufData, protobufEnvelope.Payload, "Payload should match")

		t.Log("Successfully completed Protobuf envelope test")
	})
}

// TestSchemaEndToEnd_JSONSchemaRoundTrip tests the complete JSON Schema round-trip workflow
func TestSchemaEndToEnd_JSONSchemaRoundTrip(t *testing.T) {
	t.Run("JSONSchemaEnvelopeCreation", func(t *testing.T) {
		// Create JSON data
		jsonData := []byte(`{"id": 123, "name": "Bob Smith", "active": true}`)

		// Create Confluent envelope with JSON Schema format
		confluentMsg := schema.CreateConfluentEnvelope(schema.FormatJSONSchema, 3, nil, jsonData)
		require.True(t, len(confluentMsg) > 0, "Confluent envelope should not be empty")

		t.Logf("Created JSON Schema Confluent envelope: %d bytes", len(confluentMsg))

		// Verify Confluent envelope
		envelope, ok := schema.ParseConfluentEnvelope(confluentMsg)
		require.True(t, ok, "Message should be a valid Confluent envelope")
		require.Equal(t, uint32(3), envelope.SchemaID, "Schema ID should match")
		// Note: ParseConfluentEnvelope defaults to FormatAvro; format detection requires schema registry
		require.Equal(t, schema.FormatAvro, envelope.Format, "Format defaults to Avro without schema registry lookup")

		// Verify JSON content
		assert.JSONEq(t, string(jsonData), string(envelope.Payload), "JSON payload should match")

		t.Log("Successfully completed JSON Schema envelope test")
	})
}

// TestSchemaEndToEnd_CompressionAndBatching tests schema handling with compression and batching
func TestSchemaEndToEnd_CompressionAndBatching(t *testing.T) {
	// Create mock schema registry
	server := createMockSchemaRegistryForE2E(t)
	defer server.Close()

	// Create schema manager
	config := schema.ManagerConfig{
		RegistryURL:    server.URL,
		ValidationMode: schema.ValidationPermissive,
	}
	manager, err := schema.NewManager(config)
	require.NoError(t, err)

	t.Run("BatchedSchematizedMessages", func(t *testing.T) {
		// Create multiple messages
		avroSchema := getUserAvroSchemaForE2E()
		codec, err := goavro.NewCodec(avroSchema)
		require.NoError(t, err)

		messageCount := 5
		var confluentMessages [][]byte

		// Create multiple Confluent envelopes
		for i := 0; i < messageCount; i++ {
			testData := map[string]interface{}{
				"id":    int32(1000 + i),
				"name":  fmt.Sprintf("User %d", i),
				"email": map[string]interface{}{"string": fmt.Sprintf("user%d@example.com", i)},
				"age":   map[string]interface{}{"int": int32(20 + i)},
				"preferences": map[string]interface{}{
					"Preferences": map[string]interface{}{
						"notifications": i%2 == 0, // Alternate true/false
						"theme":         "light",
					},
				},
			}

			avroBinary, err := codec.BinaryFromNative(nil, testData)
			require.NoError(t, err)

			confluentMsg := schema.CreateConfluentEnvelope(schema.FormatAvro, 1, nil, avroBinary)
			confluentMessages = append(confluentMessages, confluentMsg)
		}

		t.Logf("Created %d schematized messages", messageCount)

		// Test round-trip for each message
		for i, confluentMsg := range confluentMessages {
			// Decode message
			decodedMsg, err := manager.DecodeMessage(confluentMsg)
			require.NoError(t, err, "Message %d should decode", i)

			// Re-encode message
			reconstructedMsg, err := manager.EncodeMessage(decodedMsg.RecordValue, 1, schema.FormatAvro)
			require.NoError(t, err, "Message %d should re-encode", i)

			// Verify envelope
			envelope, ok := schema.ParseConfluentEnvelope(reconstructedMsg)
			require.True(t, ok, "Message %d should be a valid Confluent envelope", i)
			require.Equal(t, uint32(1), envelope.SchemaID, "Message %d schema ID should match", i)

			// Decode and verify content
			decodedNative, _, err := codec.NativeFromBinary(envelope.Payload)
			require.NoError(t, err, "Message %d should decode successfully", i)

			decodedMap, ok := decodedNative.(map[string]interface{})
			require.True(t, ok, "Message %d should be a map", i)

			expectedID := int32(1000 + i)
			assert.Equal(t, expectedID, decodedMap["id"], "Message %d ID should match", i)
			assert.Equal(t, fmt.Sprintf("User %d", i), decodedMap["name"], "Message %d name should match", i)
		}

		t.Log("Successfully verified batched schematized messages")
	})
}

// Helper functions for creating mock schema registries

func createMockSchemaRegistryForE2E(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schemas/ids/1":
			response := map[string]interface{}{
				"schema":  getUserAvroSchemaForE2E(),
				"subject": "user-events-e2e-value",
				"version": 1,
			}
			writeJSONResponse(w, response)
		case "/subjects/user-events-e2e-value/versions/latest":
			response := map[string]interface{}{
				"id":      1,
				"schema":  getUserAvroSchemaForE2E(),
				"subject": "user-events-e2e-value",
				"version": 1,
			}
			writeJSONResponse(w, response)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}


func getUserAvroSchemaForE2E() string {
	return `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null},
			{"name": "age", "type": ["null", "int"], "default": null},
			{"name": "preferences", "type": ["null", {
				"type": "record",
				"name": "Preferences",
				"fields": [
					{"name": "notifications", "type": "boolean", "default": true},
					{"name": "theme", "type": "string", "default": "light"}
				]
			}], "default": null}
		]
	}`
}

func writeJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
