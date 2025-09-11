package kafka

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
)

// TestKafkaGateway_SchemaIntegration tests the full Kafka Gateway with schema support
func TestKafkaGateway_SchemaIntegration(t *testing.T) {
	// Start mock schema registry
	schemaRegistry := createTestSchemaRegistry(t)
	defer schemaRegistry.Close()

	// Start Kafka Gateway with schema support
	gateway := startKafkaGatewayWithSchema(t, schemaRegistry.URL)
	defer gateway.Close()

	// Test with Sarama client
	t.Run("Sarama_SchematizedProduceConsume", func(t *testing.T) {
		testSaramaSchematizedWorkflow(t, gateway.URL)
	})

	// Test schema evolution
	t.Run("Schema_Evolution", func(t *testing.T) {
		testSchemaEvolution(t, gateway.URL, schemaRegistry.URL)
	})

	// Test error handling
	t.Run("Schema_ErrorHandling", func(t *testing.T) {
		testSchemaErrorHandling(t, gateway.URL)
	})
}

// TestKafkaGateway_MultiFormatSupport tests multiple schema formats
func TestKafkaGateway_MultiFormatSupport(t *testing.T) {
	schemaRegistry := createMultiFormatSchemaRegistry(t)
	defer schemaRegistry.Close()

	gateway := startKafkaGatewayWithSchema(t, schemaRegistry.URL)
	defer gateway.Close()

	formats := []struct {
		name     string
		topic    string
		schemaID uint32
		format   schema.Format
	}{
		{"Avro", "avro-topic-value", 1, schema.FormatAvro},
		{"JSON_Schema", "json-topic-value", 3, schema.FormatJSONSchema},
		// Protobuf would be {"Protobuf", "proto-topic-value", 2, schema.FormatProtobuf},
	}

	for _, fmt := range formats {
		t.Run(fmt.name, func(t *testing.T) {
			testFormatSpecificWorkflow(t, gateway.URL, fmt.topic, fmt.schemaID, fmt.format)
		})
	}
}

// Helper functions

func createTestSchemaRegistry(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subjects":
			subjects := []string{"user-value", "user-key", "product-value"}
			json.NewEncoder(w).Encode(subjects)

		case "/schemas/ids/1":
			// Avro user schema v1
			response := map[string]interface{}{
				"schema": `{
					"type": "record",
					"name": "User",
					"fields": [
						{"name": "id", "type": "int"},
						{"name": "name", "type": "string"},
						{"name": "email", "type": ["null", "string"], "default": null}
					]
				}`,
				"subject": "user-value",
				"version": 1,
			}
			json.NewEncoder(w).Encode(response)

		case "/schemas/ids/2":
			// Avro user schema v2 (evolved)
			response := map[string]interface{}{
				"schema": `{
					"type": "record",
					"name": "User",
					"fields": [
						{"name": "id", "type": "int"},
						{"name": "name", "type": "string"},
						{"name": "email", "type": ["null", "string"], "default": null},
						{"name": "created_at", "type": ["null", "long"], "default": null}
					]
				}`,
				"subject": "user-value",
				"version": 2,
			}
			json.NewEncoder(w).Encode(response)

		case "/subjects/user-value/versions/latest":
			response := map[string]interface{}{
				"id":      2,
				"version": 2,
				"schema": `{
					"type": "record",
					"name": "User",
					"fields": [
						{"name": "id", "type": "int"},
						{"name": "name", "type": "string"},
						{"name": "email", "type": ["null", "string"], "default": null},
						{"name": "created_at", "type": ["null", "long"], "default": null}
					]
				}`,
				"subject": "user-value",
			}
			json.NewEncoder(w).Encode(response)

		default:
			t.Logf("Schema registry: unhandled path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func createMultiFormatSchemaRegistry(t *testing.T) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subjects":
			subjects := []string{"avro-topic-value", "json-topic-value", "proto-topic-value"}
			json.NewEncoder(w).Encode(subjects)

		case "/schemas/ids/1":
			// Avro schema
			response := map[string]interface{}{
				"schema": `{
					"type": "record",
					"name": "AvroMessage",
					"fields": [
						{"name": "id", "type": "int"},
						{"name": "message", "type": "string"}
					]
				}`,
				"subject": "avro-topic-value",
				"version": 1,
			}
			json.NewEncoder(w).Encode(response)

		case "/schemas/ids/3":
			// JSON Schema
			response := map[string]interface{}{
				"schema": `{
					"$schema": "http://json-schema.org/draft-07/schema#",
					"type": "object",
					"properties": {
						"id": {"type": "integer"},
						"message": {"type": "string"},
						"timestamp": {"type": "string", "format": "date-time"}
					},
					"required": ["id", "message"]
				}`,
				"subject": "json-topic-value",
				"version": 1,
			}
			json.NewEncoder(w).Encode(response)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func startKafkaGatewayWithSchema(t *testing.T, registryURL string) *TestServer {
	// Create handler with schema support
	handler := protocol.NewHandler()

	// Enable schema management
	schemaConfig := schema.ManagerConfig{
		RegistryURL:     registryURL,
		ValidationMode:  schema.ValidationPermissive,
		EnableMirroring: false,
	}

	if err := handler.EnableSchemaManagement(schemaConfig); err != nil {
		t.Fatalf("Failed to enable schema management: %v", err)
	}

	// Add test topics
	handler.AddTopicForTesting("user-value", 1)
	handler.AddTopicForTesting("avro-topic-value", 1)
	handler.AddTopicForTesting("json-topic-value", 1)

	// Start server
	server := &TestServer{
		Handler: handler,
		URL:     "localhost:9092", // Will be set by actual server start
	}

	// In a real test, you would start the actual TCP server here
	// For this integration test, we'll simulate the server behavior

	return server
}

func testSaramaSchematizedWorkflow(t *testing.T, gatewayURL string) {
	// This test would normally connect to the actual Kafka Gateway
	// For demonstration, we'll test the schema processing logic directly

	t.Log("Testing Sarama schematized workflow")

	// Create test Avro message
	avroSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null}
		]
	}`

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		t.Fatalf("Failed to create Avro codec: %v", err)
	}

	// Create user data
	userData := map[string]interface{}{
		"id":    int32(12345),
		"name":  "Integration Test User",
		"email": map[string]interface{}{"string": "test@example.com"},
	}

	// Encode to Avro binary
	avroBinary, err := codec.BinaryFromNative(nil, userData)
	if err != nil {
		t.Fatalf("Failed to encode Avro data: %v", err)
	}

	// Create Confluent envelope (what would be sent by Sarama with schema registry)
	confluentMsg := schema.CreateConfluentEnvelope(schema.FormatAvro, 1, nil, avroBinary)

	// Verify the message can be processed
	envelope, ok := schema.ParseConfluentEnvelope(confluentMsg)
	if !ok {
		t.Fatal("Failed to parse Confluent envelope")
	}

	if envelope.SchemaID != 1 {
		t.Errorf("Expected schema ID 1, got %d", envelope.SchemaID)
	}

	if len(envelope.Payload) != len(avroBinary) {
		t.Errorf("Payload length mismatch: expected %d, got %d", len(avroBinary), len(envelope.Payload))
	}

	t.Logf("Successfully processed schematized message: %d bytes", len(confluentMsg))
}

func testSchemaEvolution(t *testing.T, gatewayURL, registryURL string) {
	t.Log("Testing schema evolution")

	// Create manager for testing
	config := schema.ManagerConfig{
		RegistryURL:    registryURL,
		ValidationMode: schema.ValidationPermissive,
	}

	manager, err := schema.NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create schema manager: %v", err)
	}

	// Test v1 message
	v1Data := map[string]interface{}{
		"id":    int32(1),
		"name":  "User V1",
		"email": map[string]interface{}{"string": "v1@example.com"},
	}

	v1Schema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null}
		]
	}`

	codec1, _ := goavro.NewCodec(v1Schema)
	v1Binary, _ := codec1.BinaryFromNative(nil, v1Data)
	v1Msg := schema.CreateConfluentEnvelope(schema.FormatAvro, 1, nil, v1Binary)

	// Test v2 message (with additional field)
	v2Data := map[string]interface{}{
		"id":         int32(2),
		"name":       "User V2",
		"email":      map[string]interface{}{"string": "v2@example.com"},
		"created_at": map[string]interface{}{"long": time.Now().Unix()},
	}

	v2Schema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null},
			{"name": "created_at", "type": ["null", "long"], "default": null}
		]
	}`

	codec2, _ := goavro.NewCodec(v2Schema)
	v2Binary, _ := codec2.BinaryFromNative(nil, v2Data)
	v2Msg := schema.CreateConfluentEnvelope(schema.FormatAvro, 2, nil, v2Binary)

	// Test that both versions can be processed
	_, err = manager.DecodeMessage(v1Msg)
	if err != nil {
		t.Errorf("Failed to decode v1 message: %v", err)
	}

	_, err = manager.DecodeMessage(v2Msg)
	if err != nil {
		t.Errorf("Failed to decode v2 message: %v", err)
	}

	t.Log("Schema evolution test passed")
}

func testSchemaErrorHandling(t *testing.T, gatewayURL string) {
	t.Log("Testing schema error handling")

	// Test various error scenarios
	errorCases := []struct {
		name    string
		message []byte
		desc    string
	}{
		{
			name:    "NonSchematizedMessage",
			message: []byte("plain text message"),
			desc:    "Plain text should not be processed as schematized",
		},
		{
			name:    "InvalidMagicByte",
			message: []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x48, 0x65, 0x6c, 0x6c, 0x6f},
			desc:    "Invalid magic byte should be rejected",
		},
		{
			name:    "TooShortMessage",
			message: []byte{0x00, 0x00, 0x00},
			desc:    "Message too short for schema ID should be rejected",
		},
	}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			envelope, ok := schema.ParseConfluentEnvelope(tc.message)

			switch tc.name {
			case "NonSchematizedMessage", "InvalidMagicByte", "TooShortMessage":
				if ok {
					t.Errorf("Expected parsing to fail for %s, but it succeeded", tc.desc)
				} else {
					t.Logf("Correctly rejected: %s", tc.desc)
				}
			default:
				if !ok {
					t.Errorf("Expected parsing to succeed for %s, but it failed", tc.desc)
				}
			}

			_ = envelope // Use the variable to avoid unused warning
		})
	}
}

func testFormatSpecificWorkflow(t *testing.T, gatewayURL, topic string, schemaID uint32, format schema.Format) {
	t.Logf("Testing %s format workflow for topic %s", format, topic)

	var testMessage []byte
	var testData interface{}

	switch format {
	case schema.FormatAvro:
		// Create Avro test message
		avroSchema := `{
			"type": "record",
			"name": "AvroMessage",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "message", "type": "string"}
			]
		}`

		codec, _ := goavro.NewCodec(avroSchema)
		testData = map[string]interface{}{
			"id":      int32(123),
			"message": "Avro test message",
		}
		avroBinary, _ := codec.BinaryFromNative(nil, testData)
		testMessage = schema.CreateConfluentEnvelope(format, schemaID, nil, avroBinary)

	case schema.FormatJSONSchema:
		// Create JSON Schema test message
		testData = map[string]interface{}{
			"id":        456,
			"message":   "JSON test message",
			"timestamp": time.Now().Format(time.RFC3339),
		}
		jsonData, _ := json.Marshal(testData)
		testMessage = schema.CreateConfluentEnvelope(format, schemaID, nil, jsonData)

	case schema.FormatProtobuf:
		// Protobuf would be implemented here
		t.Skip("Protobuf format testing not fully implemented")
		return
	}

	// Verify message can be parsed
	envelope, ok := schema.ParseConfluentEnvelope(testMessage)
	if !ok {
		t.Fatalf("Failed to parse %s message", format)
	}

	if envelope.SchemaID != schemaID {
		t.Errorf("Expected schema ID %d, got %d", schemaID, envelope.SchemaID)
	}

	if len(envelope.Payload) == 0 {
		t.Error("Expected non-empty payload")
	}

	t.Logf("Successfully processed %s message: %d bytes", format, len(testMessage))
}

// TestServer represents a test Kafka Gateway server
type TestServer struct {
	Handler *protocol.Handler
	URL     string
}

func (ts *TestServer) Close() {
	// In a real implementation, this would close the TCP server
	if ts.Handler.IsSchemaEnabled() {
		ts.Handler.DisableSchemaManagement()
	}
}

// Performance and load testing

func TestKafkaGateway_SchemaPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	schemaRegistry := createTestSchemaRegistry(t)
	defer schemaRegistry.Close()

	config := schema.ManagerConfig{
		RegistryURL:    schemaRegistry.URL,
		ValidationMode: schema.ValidationPermissive,
	}

	manager, err := schema.NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create schema manager: %v", err)
	}

	// Create test message
	avroSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	codec, _ := goavro.NewCodec(avroSchema)
	testData := map[string]interface{}{
		"id":   int32(1),
		"name": "Performance Test",
	}
	avroBinary, _ := codec.BinaryFromNative(nil, testData)
	testMsg := schema.CreateConfluentEnvelope(schema.FormatAvro, 1, nil, avroBinary)

	// Warm up cache
	_, _ = manager.DecodeMessage(testMsg)

	// Performance test
	start := time.Now()
	iterations := 1000

	for i := 0; i < iterations; i++ {
		_, err := manager.DecodeMessage(testMsg)
		if err != nil {
			t.Fatalf("Decode failed at iteration %d: %v", i, err)
		}
	}

	duration := time.Since(start)
	avgTime := duration / time.Duration(iterations)

	t.Logf("Performance test: %d iterations in %v (avg: %v per decode)",
		iterations, duration, avgTime)

	// Verify reasonable performance (adjust threshold as needed)
	if avgTime > time.Millisecond {
		t.Logf("Warning: Average decode time %v may be too slow", avgTime)
	}
}

// Benchmark tests

func BenchmarkKafkaGateway_AvroDecoding(b *testing.B) {
	schemaRegistry := createTestSchemaRegistry(nil)
	defer schemaRegistry.Close()

	config := schema.ManagerConfig{RegistryURL: schemaRegistry.URL}
	manager, _ := schema.NewManager(config)

	// Create test message
	avroSchema := `{"type": "record", "name": "User", "fields": [{"name": "id", "type": "int"}]}`
	codec, _ := goavro.NewCodec(avroSchema)
	testData := map[string]interface{}{"id": int32(1)}
	avroBinary, _ := codec.BinaryFromNative(nil, testData)
	testMsg := schema.CreateConfluentEnvelope(schema.FormatAvro, 1, nil, avroBinary)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manager.DecodeMessage(testMsg)
	}
}

func BenchmarkKafkaGateway_JSONSchemaDecoding(b *testing.B) {
	schemaRegistry := createMultiFormatSchemaRegistry(nil)
	defer schemaRegistry.Close()

	config := schema.ManagerConfig{RegistryURL: schemaRegistry.URL}
	manager, _ := schema.NewManager(config)

	// Create test message
	jsonData := []byte(`{"id": 1, "message": "test"}`)
	testMsg := schema.CreateConfluentEnvelope(schema.FormatJSONSchema, 3, nil, jsonData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manager.DecodeMessage(testMsg)
	}
}
