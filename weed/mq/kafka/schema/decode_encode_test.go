package schema

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSchemaDecodeEncode_Avro tests comprehensive Avro decode/encode workflow
func TestSchemaDecodeEncode_Avro(t *testing.T) {
	// Create mock schema registry
	registry := createMockSchemaRegistryForDecodeTest(t)
	defer registry.Close()

	manager, err := NewManager(ManagerConfig{
		RegistryURL: registry.URL,
	})
	require.NoError(t, err)

	// Test data
	testCases := []struct {
		name       string
		schemaID   int32
		schemaJSON string
		testData   map[string]interface{}
	}{
		{
			name:     "Simple User Record",
			schemaID: 1,
			schemaJSON: `{
				"type": "record",
				"name": "User",
				"fields": [
					{"name": "id", "type": "int"},
					{"name": "name", "type": "string"},
					{"name": "email", "type": ["null", "string"], "default": null}
				]
			}`,
			testData: map[string]interface{}{
				"id":    int32(123),
				"name":  "John Doe",
				"email": map[string]interface{}{"string": "john@example.com"},
			},
		},
		{
			name:     "Complex Record with Arrays",
			schemaID: 2,
			schemaJSON: `{
				"type": "record",
				"name": "Order",
				"fields": [
					{"name": "order_id", "type": "string"},
					{"name": "items", "type": {"type": "array", "items": "string"}},
					{"name": "total", "type": "double"},
					{"name": "metadata", "type": {"type": "map", "values": "string"}}
				]
			}`,
			testData: map[string]interface{}{
				"order_id": "ORD-001",
				"items":    []interface{}{"item1", "item2", "item3"},
				"total":    99.99,
				"metadata": map[string]interface{}{
					"source":   "web",
					"campaign": "summer2024",
				},
			},
		},
		{
			name:     "Union Types",
			schemaID: 3,
			schemaJSON: `{
				"type": "record",
				"name": "Event",
				"fields": [
					{"name": "event_id", "type": "string"},
					{"name": "payload", "type": ["null", "string", "int"]},
					{"name": "timestamp", "type": "long"}
				]
			}`,
			testData: map[string]interface{}{
				"event_id":  "evt-123",
				"payload":   map[string]interface{}{"int": int32(42)},
				"timestamp": int64(1640995200000),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Register schema in mock registry
			registerSchemaInMock(t, registry, tc.schemaID, tc.schemaJSON)

			// Create Avro codec
			codec, err := goavro.NewCodec(tc.schemaJSON)
			require.NoError(t, err)

			// Encode test data to Avro binary
			avroBinary, err := codec.BinaryFromNative(nil, tc.testData)
			require.NoError(t, err)

			// Create Confluent envelope
			envelope := createConfluentEnvelope(tc.schemaID, avroBinary)

			// Test decode
			decoded, err := manager.DecodeMessage(envelope)
			require.NoError(t, err)
			assert.Equal(t, uint32(tc.schemaID), decoded.SchemaID)
			assert.Equal(t, FormatAvro, decoded.SchemaFormat)
			assert.NotNil(t, decoded.RecordValue)

			// Verify decoded fields match original data
			verifyDecodedFields(t, tc.testData, decoded.RecordValue.Fields)

			// Test encode back to Confluent envelope
			reconstructed, err := manager.EncodeMessage(decoded.RecordValue, decoded.SchemaID, decoded.SchemaFormat)
			require.NoError(t, err)

			// Verify reconstructed envelope
			assert.Equal(t, envelope[:5], reconstructed[:5]) // Magic byte + schema ID

			// Decode reconstructed data to verify round-trip integrity
			decodedAgain, err := manager.DecodeMessage(reconstructed)
			require.NoError(t, err)
			assert.Equal(t, decoded.SchemaID, decodedAgain.SchemaID)
			assert.Equal(t, decoded.SchemaFormat, decodedAgain.SchemaFormat)

			// Verify fields are identical after round-trip
			verifyRecordValuesEqual(t, decoded.RecordValue, decodedAgain.RecordValue)
		})
	}
}

// TestSchemaDecodeEncode_JSONSchema tests JSON Schema decode/encode workflow
func TestSchemaDecodeEncode_JSONSchema(t *testing.T) {
	registry := createMockSchemaRegistryForDecodeTest(t)
	defer registry.Close()

	manager, err := NewManager(ManagerConfig{
		RegistryURL: registry.URL,
	})
	require.NoError(t, err)

	testCases := []struct {
		name       string
		schemaID   int32
		schemaJSON string
		testData   map[string]interface{}
	}{
		{
			name:     "Product Schema",
			schemaID: 10,
			schemaJSON: `{
				"type": "object",
				"properties": {
					"product_id": {"type": "string"},
					"name": {"type": "string"},
					"price": {"type": "number"},
					"in_stock": {"type": "boolean"},
					"tags": {
						"type": "array",
						"items": {"type": "string"}
					}
				},
				"required": ["product_id", "name", "price"]
			}`,
			testData: map[string]interface{}{
				"product_id": "PROD-123",
				"name":       "Awesome Widget",
				"price":      29.99,
				"in_stock":   true,
				"tags":       []interface{}{"electronics", "gadget"},
			},
		},
		{
			name:     "Nested Object Schema",
			schemaID: 11,
			schemaJSON: `{
				"type": "object",
				"properties": {
					"customer": {
						"type": "object",
						"properties": {
							"id": {"type": "integer"},
							"name": {"type": "string"},
							"address": {
								"type": "object",
								"properties": {
									"street": {"type": "string"},
									"city": {"type": "string"},
									"zip": {"type": "string"}
								}
							}
						}
					},
					"order_date": {"type": "string", "format": "date"}
				}
			}`,
			testData: map[string]interface{}{
				"customer": map[string]interface{}{
					"id":   float64(456), // JSON numbers are float64
					"name": "Jane Smith",
					"address": map[string]interface{}{
						"street": "123 Main St",
						"city":   "Anytown",
						"zip":    "12345",
					},
				},
				"order_date": "2024-01-15",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Register schema in mock registry
			registerSchemaInMock(t, registry, tc.schemaID, tc.schemaJSON)

			// Encode test data to JSON
			jsonBytes, err := json.Marshal(tc.testData)
			require.NoError(t, err)

			// Create Confluent envelope
			envelope := createConfluentEnvelope(tc.schemaID, jsonBytes)

			// Test decode
			decoded, err := manager.DecodeMessage(envelope)
			require.NoError(t, err)
			assert.Equal(t, uint32(tc.schemaID), decoded.SchemaID)
			assert.Equal(t, FormatJSONSchema, decoded.SchemaFormat)
			assert.NotNil(t, decoded.RecordValue)

			// Test encode back to Confluent envelope
			reconstructed, err := manager.EncodeMessage(decoded.RecordValue, decoded.SchemaID, decoded.SchemaFormat)
			require.NoError(t, err)

			// Verify reconstructed envelope has correct header
			assert.Equal(t, envelope[:5], reconstructed[:5]) // Magic byte + schema ID

			// Decode reconstructed data to verify round-trip integrity
			decodedAgain, err := manager.DecodeMessage(reconstructed)
			require.NoError(t, err)
			assert.Equal(t, decoded.SchemaID, decodedAgain.SchemaID)
			assert.Equal(t, decoded.SchemaFormat, decodedAgain.SchemaFormat)

			// Verify fields are identical after round-trip
			verifyRecordValuesEqual(t, decoded.RecordValue, decodedAgain.RecordValue)
		})
	}
}

// TestSchemaDecodeEncode_Protobuf tests Protobuf decode/encode workflow (basic structure)
func TestSchemaDecodeEncode_Protobuf(t *testing.T) {
	registry := createMockSchemaRegistryForDecodeTest(t)
	defer registry.Close()

	manager, err := NewManager(ManagerConfig{
		RegistryURL: registry.URL,
	})
	require.NoError(t, err)

	// For now, test that Protobuf detection works but decoding returns appropriate error
	schemaID := int32(20)
	protoSchema := `syntax = "proto3"; message TestMessage { string name = 1; int32 id = 2; }`

	// Register schema in mock registry
	registerSchemaInMock(t, registry, schemaID, protoSchema)

	// Create a mock protobuf message (simplified)
	protobufData := []byte{0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x7b} // name="test", id=123
	envelope := createConfluentEnvelope(schemaID, protobufData)

	// Test decode - should detect as Protobuf but return error for now
	decoded, err := manager.DecodeMessage(envelope)

	// Expect error since Protobuf decoding is not fully implemented
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "protobuf decoding not fully implemented")
	assert.Nil(t, decoded)
}

// TestSchemaDecodeEncode_ErrorHandling tests various error conditions
func TestSchemaDecodeEncode_ErrorHandling(t *testing.T) {
	registry := createMockSchemaRegistryForDecodeTest(t)
	defer registry.Close()

	manager, err := NewManager(ManagerConfig{
		RegistryURL: registry.URL,
	})
	require.NoError(t, err)

	t.Run("Invalid Confluent Envelope", func(t *testing.T) {
		// Too short envelope
		_, err := manager.DecodeMessage([]byte{0x00, 0x00})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "envelope too short")

		// Wrong magic byte
		wrongMagic := []byte{0x01, 0x00, 0x00, 0x00, 0x01, 0x41, 0x42}
		_, err = manager.DecodeMessage(wrongMagic)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid magic byte")
	})

	t.Run("Schema Not Found", func(t *testing.T) {
		// Create envelope with non-existent schema ID
		envelope := createConfluentEnvelope(999, []byte("test"))
		_, err := manager.DecodeMessage(envelope)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "schema not found")
	})

	t.Run("Invalid Avro Data", func(t *testing.T) {
		schemaID := int32(100)
		schemaJSON := `{"type": "record", "name": "Test", "fields": [{"name": "id", "type": "int"}]}`
		registerSchemaInMock(t, registry, schemaID, schemaJSON)

		// Create envelope with invalid Avro data
		envelope := createConfluentEnvelope(schemaID, []byte("invalid avro data"))
		_, err := manager.DecodeMessage(envelope)
		assert.Error(t, err)
		if err != nil {
			assert.Contains(t, err.Error(), "failed to decode")
		} else {
			t.Error("Expected error but got nil - this indicates a bug in error handling")
		}
	})

	t.Run("Invalid JSON Data", func(t *testing.T) {
		schemaID := int32(101)
		schemaJSON := `{"type": "object", "properties": {"name": {"type": "string"}}}`
		registerSchemaInMock(t, registry, schemaID, schemaJSON)

		// Create envelope with invalid JSON data
		envelope := createConfluentEnvelope(schemaID, []byte("{invalid json"))
		_, err := manager.DecodeMessage(envelope)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode")
	})
}

// TestSchemaDecodeEncode_CachePerformance tests caching behavior
func TestSchemaDecodeEncode_CachePerformance(t *testing.T) {
	registry := createMockSchemaRegistryForDecodeTest(t)
	defer registry.Close()

	manager, err := NewManager(ManagerConfig{
		RegistryURL: registry.URL,
	})
	require.NoError(t, err)

	schemaID := int32(200)
	schemaJSON := `{"type": "record", "name": "CacheTest", "fields": [{"name": "value", "type": "string"}]}`
	registerSchemaInMock(t, registry, schemaID, schemaJSON)

	// Create test data
	testData := map[string]interface{}{"value": "test"}
	codec, err := goavro.NewCodec(schemaJSON)
	require.NoError(t, err)
	avroBinary, err := codec.BinaryFromNative(nil, testData)
	require.NoError(t, err)
	envelope := createConfluentEnvelope(schemaID, avroBinary)

	// First decode - should populate cache
	decoded1, err := manager.DecodeMessage(envelope)
	require.NoError(t, err)

	// Second decode - should use cache
	decoded2, err := manager.DecodeMessage(envelope)
	require.NoError(t, err)

	// Verify both results are identical
	assert.Equal(t, decoded1.SchemaID, decoded2.SchemaID)
	assert.Equal(t, decoded1.SchemaFormat, decoded2.SchemaFormat)
	verifyRecordValuesEqual(t, decoded1.RecordValue, decoded2.RecordValue)

	// Check cache stats
	decoders, schemas, subjects := manager.GetCacheStats()
	assert.True(t, decoders > 0)
	assert.True(t, schemas > 0)
	assert.True(t, subjects >= 0)
}

// Helper functions

func createMockSchemaRegistryForDecodeTest(t *testing.T) *httptest.Server {
	schemas := make(map[int32]string)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subjects":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
		default:
			// Handle schema requests like /schemas/ids/1
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
				// Custom endpoint for test registration
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

func registerSchemaInMock(t *testing.T, registry *httptest.Server, schemaID int32, schema string) {
	reqBody := fmt.Sprintf(`{"schema_id": %d, "schema": %q}`, schemaID, schema)
	resp, err := http.Post(registry.URL+"/register-schema", "application/json", bytes.NewReader([]byte(reqBody)))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func createConfluentEnvelope(schemaID int32, data []byte) []byte {
	envelope := make([]byte, 5+len(data))
	envelope[0] = 0x00 // Magic byte
	binary.BigEndian.PutUint32(envelope[1:5], uint32(schemaID))
	copy(envelope[5:], data)
	return envelope
}

func verifyDecodedFields(t *testing.T, expected map[string]interface{}, actual map[string]*schema_pb.Value) {
	for key, expectedValue := range expected {
		actualValue, exists := actual[key]
		require.True(t, exists, "Field %s should exist", key)

		switch v := expectedValue.(type) {
		case int32:
			assert.Equal(t, int64(v), actualValue.GetInt64Value(), "Field %s should match", key)
		case string:
			assert.Equal(t, v, actualValue.GetStringValue(), "Field %s should match", key)
		case float64:
			assert.Equal(t, v, actualValue.GetDoubleValue(), "Field %s should match", key)
		case bool:
			assert.Equal(t, v, actualValue.GetBoolValue(), "Field %s should match", key)
		case []interface{}:
			listValue := actualValue.GetListValue()
			require.NotNil(t, listValue, "Field %s should be a list", key)
			assert.Equal(t, len(v), len(listValue.Values), "List %s should have correct length", key)
		case map[string]interface{}:
			if unionValue, isUnion := v[key]; isUnion {
				// Handle Avro union types
				switch unionValue.(type) {
				case int32:
					assert.Equal(t, int64(unionValue.(int32)), actualValue.GetInt64Value())
				case string:
					assert.Equal(t, unionValue.(string), actualValue.GetStringValue())
				}
			} else {
				// Handle regular maps/objects
				recordValue := actualValue.GetRecordValue()
				require.NotNil(t, recordValue, "Field %s should be a record", key)
				verifyDecodedFields(t, v, recordValue.Fields)
			}
		}
	}
}

func verifyRecordValuesEqual(t *testing.T, expected, actual *schema_pb.RecordValue) {
	require.Equal(t, len(expected.Fields), len(actual.Fields), "Record should have same number of fields")

	for key, expectedValue := range expected.Fields {
		actualValue, exists := actual.Fields[key]
		require.True(t, exists, "Field %s should exist", key)

		// Compare values based on type
		switch expectedValue.Kind.(type) {
		case *schema_pb.Value_StringValue:
			assert.Equal(t, expectedValue.GetStringValue(), actualValue.GetStringValue())
		case *schema_pb.Value_Int64Value:
			assert.Equal(t, expectedValue.GetInt64Value(), actualValue.GetInt64Value())
		case *schema_pb.Value_DoubleValue:
			assert.Equal(t, expectedValue.GetDoubleValue(), actualValue.GetDoubleValue())
		case *schema_pb.Value_BoolValue:
			assert.Equal(t, expectedValue.GetBoolValue(), actualValue.GetBoolValue())
		case *schema_pb.Value_ListValue:
			expectedList := expectedValue.GetListValue()
			actualList := actualValue.GetListValue()
			require.Equal(t, len(expectedList.Values), len(actualList.Values))
			for i, expectedItem := range expectedList.Values {
				verifyValuesEqual(t, expectedItem, actualList.Values[i])
			}
		case *schema_pb.Value_RecordValue:
			verifyRecordValuesEqual(t, expectedValue.GetRecordValue(), actualValue.GetRecordValue())
		}
	}
}

func verifyValuesEqual(t *testing.T, expected, actual *schema_pb.Value) {
	switch expected.Kind.(type) {
	case *schema_pb.Value_StringValue:
		assert.Equal(t, expected.GetStringValue(), actual.GetStringValue())
	case *schema_pb.Value_Int64Value:
		assert.Equal(t, expected.GetInt64Value(), actual.GetInt64Value())
	case *schema_pb.Value_DoubleValue:
		assert.Equal(t, expected.GetDoubleValue(), actual.GetDoubleValue())
	case *schema_pb.Value_BoolValue:
		assert.Equal(t, expected.GetBoolValue(), actual.GetBoolValue())
	default:
		t.Errorf("Unsupported value type for comparison")
	}
}
