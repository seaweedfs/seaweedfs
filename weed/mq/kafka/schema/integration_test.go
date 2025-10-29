package schema

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
)

// TestFullIntegration_AvroWorkflow tests the complete Avro workflow
func TestFullIntegration_AvroWorkflow(t *testing.T) {
	// Create comprehensive mock schema registry
	server := createMockSchemaRegistry(t)
	defer server.Close()

	// Create manager with realistic configuration
	config := ManagerConfig{
		RegistryURL:     server.URL,
		ValidationMode:  ValidationPermissive,
		EnableMirroring: false,
		CacheTTL:        "5m",
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Test 1: Producer workflow - encode schematized message
	t.Run("Producer_Workflow", func(t *testing.T) {
		// Create realistic user data (with proper Avro union handling)
		userData := map[string]interface{}{
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

		// Create Avro message (simulate what a Kafka producer would send)
		avroSchema := getUserAvroSchema()
		codec, err := goavro.NewCodec(avroSchema)
		if err != nil {
			t.Fatalf("Failed to create Avro codec: %v", err)
		}

		avroBinary, err := codec.BinaryFromNative(nil, userData)
		if err != nil {
			t.Fatalf("Failed to encode Avro data: %v", err)
		}

		// Create Confluent envelope (what Kafka Gateway receives)
		confluentMsg := CreateConfluentEnvelope(FormatAvro, 1, nil, avroBinary)

		// Decode message (Produce path processing)
		decodedMsg, err := manager.DecodeMessage(confluentMsg)
		if err != nil {
			t.Fatalf("Failed to decode message: %v", err)
		}

		// Verify decoded data
		if decodedMsg.SchemaID != 1 {
			t.Errorf("Expected schema ID 1, got %d", decodedMsg.SchemaID)
		}

		if decodedMsg.SchemaFormat != FormatAvro {
			t.Errorf("Expected Avro format, got %v", decodedMsg.SchemaFormat)
		}

		// Verify field values
		fields := decodedMsg.RecordValue.Fields
		if fields["id"].GetInt32Value() != 12345 {
			t.Errorf("Expected id=12345, got %v", fields["id"].GetInt32Value())
		}

		if fields["name"].GetStringValue() != "Alice Johnson" {
			t.Errorf("Expected name='Alice Johnson', got %v", fields["name"].GetStringValue())
		}

		t.Logf("Successfully processed producer message with %d fields", len(fields))
	})

	// Test 2: Consumer workflow - reconstruct original message
	t.Run("Consumer_Workflow", func(t *testing.T) {
		// Create test RecordValue (simulate what's stored in SeaweedMQ)
		testData := map[string]interface{}{
			"id":    int32(67890),
			"name":  "Bob Smith",
			"email": map[string]interface{}{"string": "bob@example.com"},
			"age":   map[string]interface{}{"int": int32(35)}, // Avro union
		}
		recordValue := MapToRecordValue(testData)

		// Reconstruct message (Fetch path processing)
		reconstructedMsg, err := manager.EncodeMessage(recordValue, 1, FormatAvro)
		if err != nil {
			t.Fatalf("Failed to reconstruct message: %v", err)
		}

		// Verify reconstructed message can be parsed
		envelope, ok := ParseConfluentEnvelope(reconstructedMsg)
		if !ok {
			t.Fatal("Failed to parse reconstructed envelope")
		}

		if envelope.SchemaID != 1 {
			t.Errorf("Expected schema ID 1, got %d", envelope.SchemaID)
		}

		// Verify the payload can be decoded by Avro
		avroSchema := getUserAvroSchema()
		codec, err := goavro.NewCodec(avroSchema)
		if err != nil {
			t.Fatalf("Failed to create Avro codec: %v", err)
		}

		decodedData, _, err := codec.NativeFromBinary(envelope.Payload)
		if err != nil {
			t.Fatalf("Failed to decode reconstructed Avro data: %v", err)
		}

		// Verify data integrity
		decodedMap := decodedData.(map[string]interface{})
		if decodedMap["id"] != int32(67890) {
			t.Errorf("Expected id=67890, got %v", decodedMap["id"])
		}

		if decodedMap["name"] != "Bob Smith" {
			t.Errorf("Expected name='Bob Smith', got %v", decodedMap["name"])
		}

		t.Logf("Successfully reconstructed consumer message: %d bytes", len(reconstructedMsg))
	})

	// Test 3: Round-trip integrity
	t.Run("Round_Trip_Integrity", func(t *testing.T) {
		originalData := map[string]interface{}{
			"id":    int32(99999),
			"name":  "Charlie Brown",
			"email": map[string]interface{}{"string": "charlie@example.com"},
			"age":   map[string]interface{}{"int": int32(42)}, // Avro union
			"preferences": map[string]interface{}{
				"Preferences": map[string]interface{}{ // Avro union with record type
					"notifications": true,
					"theme":         "dark",
				},
			},
		}

		// Encode -> Decode -> Encode -> Decode
		avroSchema := getUserAvroSchema()
		codec, _ := goavro.NewCodec(avroSchema)

		// Step 1: Original -> Confluent
		avroBinary, _ := codec.BinaryFromNative(nil, originalData)
		confluentMsg := CreateConfluentEnvelope(FormatAvro, 1, nil, avroBinary)

		// Step 2: Confluent -> RecordValue
		decodedMsg, _ := manager.DecodeMessage(confluentMsg)

		// Step 3: RecordValue -> Confluent
		reconstructedMsg, encodeErr := manager.EncodeMessage(decodedMsg.RecordValue, 1, FormatAvro)
		if encodeErr != nil {
			t.Fatalf("Failed to encode message: %v", encodeErr)
		}

		// Verify the reconstructed message is valid
		if len(reconstructedMsg) == 0 {
			t.Fatal("Reconstructed message is empty")
		}

		// Step 4: Confluent -> Verify
		finalDecodedMsg, err := manager.DecodeMessage(reconstructedMsg)
		if err != nil {
			// Debug: Check if the reconstructed message is properly formatted
			envelope, ok := ParseConfluentEnvelope(reconstructedMsg)
			if !ok {
				t.Fatalf("Round-trip failed: reconstructed message is not a valid Confluent envelope")
			}
			t.Logf("Debug: Envelope SchemaID=%d, Format=%v, PayloadLen=%d",
				envelope.SchemaID, envelope.Format, len(envelope.Payload))
			t.Fatalf("Round-trip failed: %v", err)
		}

		// Verify data integrity through complete round-trip
		finalFields := finalDecodedMsg.RecordValue.Fields
		if finalFields["id"].GetInt32Value() != 99999 {
			t.Error("Round-trip failed for id field")
		}

		if finalFields["name"].GetStringValue() != "Charlie Brown" {
			t.Error("Round-trip failed for name field")
		}

		t.Log("Round-trip integrity test passed")
	})
}

// TestFullIntegration_MultiFormatSupport tests all schema formats together
func TestFullIntegration_MultiFormatSupport(t *testing.T) {
	server := createMockSchemaRegistry(t)
	defer server.Close()

	config := ManagerConfig{
		RegistryURL:    server.URL,
		ValidationMode: ValidationPermissive,
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	testCases := []struct {
		name     string
		format   Format
		schemaID uint32
		testData interface{}
	}{
		{
			name:     "Avro_Format",
			format:   FormatAvro,
			schemaID: 1,
			testData: map[string]interface{}{
				"id":   int32(123),
				"name": "Avro User",
			},
		},
		{
			name:     "JSON_Schema_Format",
			format:   FormatJSONSchema,
			schemaID: 3,
			testData: map[string]interface{}{
				"id":     float64(456), // JSON numbers are float64
				"name":   "JSON User",
				"active": true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create RecordValue from test data
			recordValue := MapToRecordValue(tc.testData.(map[string]interface{}))

			// Test encoding
			encoded, err := manager.EncodeMessage(recordValue, tc.schemaID, tc.format)
			if err != nil {
				if tc.format == FormatProtobuf {
					// Protobuf encoding may fail due to incomplete implementation
					t.Skipf("Protobuf encoding not fully implemented: %v", err)
				} else {
					t.Fatalf("Failed to encode %s message: %v", tc.name, err)
				}
			}

			// Test decoding
			decoded, err := manager.DecodeMessage(encoded)
			if err != nil {
				t.Fatalf("Failed to decode %s message: %v", tc.name, err)
			}

			// Verify format
			if decoded.SchemaFormat != tc.format {
				t.Errorf("Expected format %v, got %v", tc.format, decoded.SchemaFormat)
			}

			// Verify schema ID
			if decoded.SchemaID != tc.schemaID {
				t.Errorf("Expected schema ID %d, got %d", tc.schemaID, decoded.SchemaID)
			}

			t.Logf("Successfully processed %s format", tc.name)
		})
	}
}

// TestIntegration_CachePerformance tests caching behavior under load
func TestIntegration_CachePerformance(t *testing.T) {
	server := createMockSchemaRegistry(t)
	defer server.Close()

	config := ManagerConfig{
		RegistryURL:    server.URL,
		ValidationMode: ValidationPermissive,
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Create test message
	testData := map[string]interface{}{
		"id":   int32(1),
		"name": "Cache Test",
	}

	avroSchema := getUserAvroSchema()
	codec, _ := goavro.NewCodec(avroSchema)
	avroBinary, _ := codec.BinaryFromNative(nil, testData)
	testMsg := CreateConfluentEnvelope(FormatAvro, 1, nil, avroBinary)

	// First decode (should hit registry)
	start := time.Now()
	_, err = manager.DecodeMessage(testMsg)
	if err != nil {
		t.Fatalf("First decode failed: %v", err)
	}
	firstDuration := time.Since(start)

	// Subsequent decodes (should hit cache)
	start = time.Now()
	for i := 0; i < 100; i++ {
		_, err = manager.DecodeMessage(testMsg)
		if err != nil {
			t.Fatalf("Cached decode failed: %v", err)
		}
	}
	cachedDuration := time.Since(start)

	// Verify cache performance improvement
	avgCachedTime := cachedDuration / 100
	if avgCachedTime >= firstDuration {
		t.Logf("Warning: Cache may not be effective. First: %v, Avg Cached: %v",
			firstDuration, avgCachedTime)
	}

	// Check cache stats
	decoders, schemas, subjects := manager.GetCacheStats()
	if decoders == 0 || schemas == 0 {
		t.Error("Expected non-zero cache stats")
	}

	t.Logf("Cache performance: First decode: %v, Average cached: %v",
		firstDuration, avgCachedTime)
	t.Logf("Cache stats: %d decoders, %d schemas, %d subjects",
		decoders, schemas, subjects)
}

// TestIntegration_ErrorHandling tests error scenarios
func TestIntegration_ErrorHandling(t *testing.T) {
	server := createMockSchemaRegistry(t)
	defer server.Close()

	config := ManagerConfig{
		RegistryURL:    server.URL,
		ValidationMode: ValidationStrict,
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	testCases := []struct {
		name        string
		message     []byte
		expectError bool
		errorType   string
	}{
		{
			name:        "Non_Schematized_Message",
			message:     []byte("plain text message"),
			expectError: true,
			errorType:   "not schematized",
		},
		{
			name:        "Invalid_Schema_ID",
			message:     CreateConfluentEnvelope(FormatAvro, 999, nil, []byte("payload")),
			expectError: true,
			errorType:   "schema not found",
		},
		{
			name:        "Empty_Payload",
			message:     CreateConfluentEnvelope(FormatAvro, 1, nil, []byte{}),
			expectError: true,
			errorType:   "empty payload",
		},
		{
			name:        "Corrupted_Avro_Data",
			message:     CreateConfluentEnvelope(FormatAvro, 1, nil, []byte("invalid avro")),
			expectError: true,
			errorType:   "decode failed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := manager.DecodeMessage(tc.message)

			if (err != nil) != tc.expectError {
				t.Errorf("Expected error: %v, got error: %v", tc.expectError, err != nil)
			}

			if tc.expectError && err != nil {
				t.Logf("Expected error occurred: %v", err)
			}
		})
	}
}

// TestIntegration_SchemaEvolution tests schema evolution scenarios
func TestIntegration_SchemaEvolution(t *testing.T) {
	server := createMockSchemaRegistryWithEvolution(t)
	defer server.Close()

	config := ManagerConfig{
		RegistryURL:    server.URL,
		ValidationMode: ValidationPermissive,
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Test decoding messages with different schema versions
	t.Run("Schema_V1_Message", func(t *testing.T) {
		// Create message with schema v1 (basic user)
		userData := map[string]interface{}{
			"id":   int32(1),
			"name": "User V1",
		}

		avroSchema := getUserAvroSchemaV1()
		codec, _ := goavro.NewCodec(avroSchema)
		avroBinary, _ := codec.BinaryFromNative(nil, userData)
		msg := CreateConfluentEnvelope(FormatAvro, 1, nil, avroBinary)

		decoded, err := manager.DecodeMessage(msg)
		if err != nil {
			t.Fatalf("Failed to decode v1 message: %v", err)
		}

		if decoded.Version != 1 {
			t.Errorf("Expected version 1, got %d", decoded.Version)
		}
	})

	t.Run("Schema_V2_Message", func(t *testing.T) {
		// Create message with schema v2 (user with email)
		userData := map[string]interface{}{
			"id":    int32(2),
			"name":  "User V2",
			"email": map[string]interface{}{"string": "user@example.com"},
		}

		avroSchema := getUserAvroSchemaV2()
		codec, _ := goavro.NewCodec(avroSchema)
		avroBinary, _ := codec.BinaryFromNative(nil, userData)
		msg := CreateConfluentEnvelope(FormatAvro, 2, nil, avroBinary)

		decoded, err := manager.DecodeMessage(msg)
		if err != nil {
			t.Fatalf("Failed to decode v2 message: %v", err)
		}

		if decoded.Version != 2 {
			t.Errorf("Expected version 2, got %d", decoded.Version)
		}
	})
}

// Helper functions for creating mock schema registries

func createMockSchemaRegistry(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subjects":
			// List subjects
			subjects := []string{"user-value", "product-value", "order-value"}
			json.NewEncoder(w).Encode(subjects)

		case "/schemas/ids/1":
			// Avro user schema
			response := map[string]interface{}{
				"schema":  getUserAvroSchema(),
				"subject": "user-value",
				"version": 1,
			}
			json.NewEncoder(w).Encode(response)

		case "/schemas/ids/2":
			// Protobuf schema (simplified)
			response := map[string]interface{}{
				"schema":  "syntax = \"proto3\"; message User { int32 id = 1; string name = 2; }",
				"subject": "user-value",
				"version": 2,
			}
			json.NewEncoder(w).Encode(response)

		case "/schemas/ids/3":
			// JSON Schema
			response := map[string]interface{}{
				"schema":  getUserJSONSchema(),
				"subject": "user-value",
				"version": 3,
			}
			json.NewEncoder(w).Encode(response)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func createMockSchemaRegistryWithEvolution(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schemas/ids/1":
			// Schema v1
			response := map[string]interface{}{
				"schema":  getUserAvroSchemaV1(),
				"subject": "user-value",
				"version": 1,
			}
			json.NewEncoder(w).Encode(response)

		case "/schemas/ids/2":
			// Schema v2 (evolved)
			response := map[string]interface{}{
				"schema":  getUserAvroSchemaV2(),
				"subject": "user-value",
				"version": 2,
			}
			json.NewEncoder(w).Encode(response)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// Schema definitions for testing

func getUserAvroSchema() string {
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

func getUserAvroSchemaV1() string {
	return `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`
}

func getUserAvroSchemaV2() string {
	return `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null}
		]
	}`
}

func getUserJSONSchema() string {
	return `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"id": {"type": "integer"},
			"name": {"type": "string"},
			"active": {"type": "boolean"}
		},
		"required": ["id", "name"]
	}`
}

// Benchmark tests for integration scenarios

func BenchmarkIntegration_AvroDecoding(b *testing.B) {
	server := createMockSchemaRegistry(nil)
	defer server.Close()

	config := ManagerConfig{RegistryURL: server.URL}
	manager, _ := NewManager(config)

	// Create test message
	testData := map[string]interface{}{
		"id":   int32(1),
		"name": "Benchmark User",
	}

	avroSchema := getUserAvroSchema()
	codec, _ := goavro.NewCodec(avroSchema)
	avroBinary, _ := codec.BinaryFromNative(nil, testData)
	testMsg := CreateConfluentEnvelope(FormatAvro, 1, nil, avroBinary)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manager.DecodeMessage(testMsg)
	}
}

func BenchmarkIntegration_JSONSchemaDecoding(b *testing.B) {
	server := createMockSchemaRegistry(nil)
	defer server.Close()

	config := ManagerConfig{RegistryURL: server.URL}
	manager, _ := NewManager(config)

	// Create test message
	jsonData := []byte(`{"id": 1, "name": "Benchmark User", "active": true}`)
	testMsg := CreateConfluentEnvelope(FormatJSONSchema, 3, nil, jsonData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manager.DecodeMessage(testMsg)
	}
}
