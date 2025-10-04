package schema

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/linkedin/goavro/v2"
)

func TestManager_DecodeMessage(t *testing.T) {
	// Create mock schema registry
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/schemas/ids/1" {
			response := map[string]interface{}{
				"schema": `{
					"type": "record",
					"name": "User",
					"fields": [
						{"name": "id", "type": "int"},
						{"name": "name", "type": "string"}
					]
				}`,
				"subject": "user-value",
				"version": 1,
			}
			json.NewEncoder(w).Encode(response)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create manager
	config := ManagerConfig{
		RegistryURL:    server.URL,
		ValidationMode: ValidationPermissive,
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Create test Avro message
	avroSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		t.Fatalf("Failed to create Avro codec: %v", err)
	}

	// Create test data
	testRecord := map[string]interface{}{
		"id":   int32(123),
		"name": "John Doe",
	}

	// Encode to Avro binary
	avroBinary, err := codec.BinaryFromNative(nil, testRecord)
	if err != nil {
		t.Fatalf("Failed to encode Avro data: %v", err)
	}

	// Create Confluent envelope
	confluentMsg := CreateConfluentEnvelope(FormatAvro, 1, nil, avroBinary)

	// Test decoding
	decodedMsg, err := manager.DecodeMessage(confluentMsg)
	if err != nil {
		t.Fatalf("Failed to decode message: %v", err)
	}

	// Verify decoded message
	if decodedMsg.SchemaID != 1 {
		t.Errorf("Expected schema ID 1, got %d", decodedMsg.SchemaID)
	}

	if decodedMsg.SchemaFormat != FormatAvro {
		t.Errorf("Expected Avro format, got %v", decodedMsg.SchemaFormat)
	}

	if decodedMsg.Subject != "user-value" {
		t.Errorf("Expected subject 'user-value', got %s", decodedMsg.Subject)
	}

	// Verify decoded data
	if decodedMsg.RecordValue == nil {
		t.Fatal("Expected non-nil RecordValue")
	}

	idValue := decodedMsg.RecordValue.Fields["id"]
	if idValue == nil || idValue.GetInt32Value() != 123 {
		t.Errorf("Expected id=123, got %v", idValue)
	}

	nameValue := decodedMsg.RecordValue.Fields["name"]
	if nameValue == nil || nameValue.GetStringValue() != "John Doe" {
		t.Errorf("Expected name='John Doe', got %v", nameValue)
	}
}

func TestManager_IsSchematized(t *testing.T) {
	config := ManagerConfig{
		RegistryURL: "http://localhost:8081", // Not used for this test
	}

	manager, err := NewManager(config)
	if err != nil {
		// Skip test if we can't connect to registry
		t.Skip("Skipping test - no registry available")
	}

	tests := []struct {
		name     string
		message  []byte
		expected bool
	}{
		{
			name:     "schematized message",
			message:  []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x48, 0x65, 0x6c, 0x6c, 0x6f},
			expected: true,
		},
		{
			name:     "non-schematized message",
			message:  []byte{0x48, 0x65, 0x6c, 0x6c, 0x6f}, // Just "Hello"
			expected: false,
		},
		{
			name:     "empty message",
			message:  []byte{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := manager.IsSchematized(tt.message)
			if result != tt.expected {
				t.Errorf("IsSchematized() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestManager_GetSchemaInfo(t *testing.T) {
	// Create mock schema registry
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/schemas/ids/42" {
			response := map[string]interface{}{
				"schema": `{
					"type": "record",
					"name": "Product",
					"fields": [
						{"name": "id", "type": "string"},
						{"name": "price", "type": "double"}
					]
				}`,
				"subject": "product-value",
				"version": 3,
			}
			json.NewEncoder(w).Encode(response)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	config := ManagerConfig{
		RegistryURL: server.URL,
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Create test message with schema ID 42
	testMsg := CreateConfluentEnvelope(FormatAvro, 42, nil, []byte("test-payload"))

	schemaID, format, err := manager.GetSchemaInfo(testMsg)
	if err != nil {
		t.Fatalf("Failed to get schema info: %v", err)
	}

	if schemaID != 42 {
		t.Errorf("Expected schema ID 42, got %d", schemaID)
	}

	if format != FormatAvro {
		t.Errorf("Expected Avro format, got %v", format)
	}
}

func TestManager_CacheManagement(t *testing.T) {
	config := ManagerConfig{
		RegistryURL: "http://localhost:8081", // Not used for this test
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Skip("Skipping test - no registry available")
	}

	// Check initial cache stats
	decoders, schemas, subjects := manager.GetCacheStats()
	if decoders != 0 || schemas != 0 || subjects != 0 {
		t.Errorf("Expected empty cache initially, got decoders=%d, schemas=%d, subjects=%d",
			decoders, schemas, subjects)
	}

	// Clear cache (should be no-op on empty cache)
	manager.ClearCache()

	// Verify still empty
	decoders, schemas, subjects = manager.GetCacheStats()
	if decoders != 0 || schemas != 0 || subjects != 0 {
		t.Errorf("Expected empty cache after clear, got decoders=%d, schemas=%d, subjects=%d",
			decoders, schemas, subjects)
	}
}

func TestManager_EncodeMessage(t *testing.T) {
	// Create mock schema registry
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/schemas/ids/1" {
			response := map[string]interface{}{
				"schema": `{
					"type": "record",
					"name": "User",
					"fields": [
						{"name": "id", "type": "int"},
						{"name": "name", "type": "string"}
					]
				}`,
				"subject": "user-value",
				"version": 1,
			}
			json.NewEncoder(w).Encode(response)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	config := ManagerConfig{
		RegistryURL: server.URL,
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Create test RecordValue
	testMap := map[string]interface{}{
		"id":   int32(456),
		"name": "Jane Smith",
	}
	recordValue := MapToRecordValue(testMap)

	// Test encoding
	encoded, err := manager.EncodeMessage(recordValue, 1, FormatAvro)
	if err != nil {
		t.Fatalf("Failed to encode message: %v", err)
	}

	// Verify it's a valid Confluent envelope
	envelope, ok := ParseConfluentEnvelope(encoded)
	if !ok {
		t.Fatal("Encoded message is not a valid Confluent envelope")
	}

	if envelope.SchemaID != 1 {
		t.Errorf("Expected schema ID 1, got %d", envelope.SchemaID)
	}

	if envelope.Format != FormatAvro {
		t.Errorf("Expected Avro format, got %v", envelope.Format)
	}

	// Test round-trip: decode the encoded message
	decodedMsg, err := manager.DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("Failed to decode round-trip message: %v", err)
	}

	// Verify round-trip data integrity
	if decodedMsg.RecordValue.Fields["id"].GetInt32Value() != 456 {
		t.Error("Round-trip failed for id field")
	}

	if decodedMsg.RecordValue.Fields["name"].GetStringValue() != "Jane Smith" {
		t.Error("Round-trip failed for name field")
	}
}

// Benchmark tests
func BenchmarkManager_DecodeMessage(b *testing.B) {
	// Setup (similar to TestManager_DecodeMessage but simplified)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"schema":  `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`,
			"subject": "user-value",
			"version": 1,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	config := ManagerConfig{RegistryURL: server.URL}
	manager, _ := NewManager(config)

	// Create test message
	codec, _ := goavro.NewCodec(`{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`)
	avroBinary, _ := codec.BinaryFromNative(nil, map[string]interface{}{"id": int32(123)})
	testMsg := CreateConfluentEnvelope(FormatAvro, 1, nil, avroBinary)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = manager.DecodeMessage(testMsg)
	}
}
