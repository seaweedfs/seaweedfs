package schema

import (
	"encoding/json"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestNewJSONSchemaDecoder(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		expectErr bool
	}{
		{
			name: "valid object schema",
			schema: `{
				"$schema": "http://json-schema.org/draft-07/schema#",
				"type": "object",
				"properties": {
					"id": {"type": "integer"},
					"name": {"type": "string"},
					"active": {"type": "boolean"}
				},
				"required": ["id", "name"]
			}`,
			expectErr: false,
		},
		{
			name: "valid array schema",
			schema: `{
				"$schema": "http://json-schema.org/draft-07/schema#",
				"type": "array",
				"items": {
					"type": "string"
				}
			}`,
			expectErr: false,
		},
		{
			name: "valid string schema with format",
			schema: `{
				"$schema": "http://json-schema.org/draft-07/schema#",
				"type": "string",
				"format": "date-time"
			}`,
			expectErr: false,
		},
		{
			name:      "invalid JSON",
			schema:    `{"invalid": json}`,
			expectErr: true,
		},
		{
			name:      "empty schema",
			schema:    "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoder, err := NewJSONSchemaDecoder(tt.schema)

			if (err != nil) != tt.expectErr {
				t.Errorf("NewJSONSchemaDecoder() error = %v, expectErr %v", err, tt.expectErr)
				return
			}

			if !tt.expectErr && decoder == nil {
				t.Error("Expected non-nil decoder for valid schema")
			}
		})
	}
}

func TestJSONSchemaDecoder_Decode(t *testing.T) {
	schema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"id": {"type": "integer"},
			"name": {"type": "string"},
			"email": {"type": "string", "format": "email"},
			"age": {"type": "integer", "minimum": 0},
			"active": {"type": "boolean"}
		},
		"required": ["id", "name"]
	}`

	decoder, err := NewJSONSchemaDecoder(schema)
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	tests := []struct {
		name      string
		jsonData  string
		expectErr bool
	}{
		{
			name: "valid complete data",
			jsonData: `{
				"id": 123,
				"name": "John Doe",
				"email": "john@example.com",
				"age": 30,
				"active": true
			}`,
			expectErr: false,
		},
		{
			name: "valid minimal data",
			jsonData: `{
				"id": 456,
				"name": "Jane Smith"
			}`,
			expectErr: false,
		},
		{
			name: "missing required field",
			jsonData: `{
				"name": "Missing ID"
			}`,
			expectErr: true,
		},
		{
			name: "invalid type",
			jsonData: `{
				"id": "not-a-number",
				"name": "John Doe"
			}`,
			expectErr: true,
		},
		{
			name: "invalid email format",
			jsonData: `{
				"id": 123,
				"name": "John Doe",
				"email": "not-an-email"
			}`,
			expectErr: true,
		},
		{
			name: "negative age",
			jsonData: `{
				"id": 123,
				"name": "John Doe",
				"age": -5
			}`,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := decoder.Decode([]byte(tt.jsonData))

			if (err != nil) != tt.expectErr {
				t.Errorf("Decode() error = %v, expectErr %v", err, tt.expectErr)
				return
			}

			if !tt.expectErr {
				if result == nil {
					t.Error("Expected non-nil result for valid data")
				}

				// Verify some basic fields
				if id, exists := result["id"]; exists {
					// Numbers are now json.Number for precision
					if _, ok := id.(json.Number); !ok {
						t.Errorf("Expected id to be json.Number, got %T", id)
					}
				}

				if name, exists := result["name"]; exists {
					if _, ok := name.(string); !ok {
						t.Errorf("Expected name to be string, got %T", name)
					}
				}
			}
		})
	}
}

func TestJSONSchemaDecoder_DecodeToRecordValue(t *testing.T) {
	schema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"id": {"type": "integer"},
			"name": {"type": "string"},
			"tags": {
				"type": "array",
				"items": {"type": "string"}
			}
		}
	}`

	decoder, err := NewJSONSchemaDecoder(schema)
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	jsonData := `{
		"id": 789,
		"name": "Test User",
		"tags": ["tag1", "tag2", "tag3"]
	}`

	recordValue, err := decoder.DecodeToRecordValue([]byte(jsonData))
	if err != nil {
		t.Fatalf("Failed to decode to RecordValue: %v", err)
	}

	// Verify RecordValue structure
	if recordValue.Fields == nil {
		t.Fatal("Expected non-nil fields")
	}

	// Check id field
	idValue := recordValue.Fields["id"]
	if idValue == nil {
		t.Fatal("Expected id field")
	}
	// JSON numbers are decoded as float64 by default
	// The MapToRecordValue function should handle this conversion
	expectedID := int64(789)
	actualID := idValue.GetInt64Value()
	if actualID != expectedID {
		// Try checking if it was stored as float64 instead
		if floatVal := idValue.GetDoubleValue(); floatVal == 789.0 {
			t.Logf("ID was stored as float64: %v", floatVal)
		} else {
			t.Errorf("Expected id=789, got int64=%v, float64=%v", actualID, floatVal)
		}
	}

	// Check name field
	nameValue := recordValue.Fields["name"]
	if nameValue == nil {
		t.Fatal("Expected name field")
	}
	if nameValue.GetStringValue() != "Test User" {
		t.Errorf("Expected name='Test User', got %v", nameValue.GetStringValue())
	}

	// Check tags array
	tagsValue := recordValue.Fields["tags"]
	if tagsValue == nil {
		t.Fatal("Expected tags field")
	}
	tagsList := tagsValue.GetListValue()
	if tagsList == nil || len(tagsList.Values) != 3 {
		t.Errorf("Expected tags array with 3 elements, got %v", tagsList)
	}
}

func TestJSONSchemaDecoder_InferRecordType(t *testing.T) {
	schema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"id": {"type": "integer", "format": "int32"},
			"name": {"type": "string"},
			"score": {"type": "number", "format": "float"},
			"timestamp": {"type": "string", "format": "date-time"},
			"data": {"type": "string", "format": "byte"},
			"active": {"type": "boolean"},
			"tags": {
				"type": "array",
				"items": {"type": "string"}
			},
			"metadata": {
				"type": "object",
				"properties": {
					"source": {"type": "string"}
				}
			}
		},
		"required": ["id", "name"]
	}`

	decoder, err := NewJSONSchemaDecoder(schema)
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	recordType, err := decoder.InferRecordType()
	if err != nil {
		t.Fatalf("Failed to infer RecordType: %v", err)
	}

	if len(recordType.Fields) != 8 {
		t.Errorf("Expected 8 fields, got %d", len(recordType.Fields))
	}

	// Create a map for easier field lookup
	fieldMap := make(map[string]*schema_pb.Field)
	for _, field := range recordType.Fields {
		fieldMap[field.Name] = field
	}

	// Test specific field types
	if fieldMap["id"].Type.GetScalarType() != schema_pb.ScalarType_INT32 {
		t.Error("Expected id field to be INT32")
	}

	if fieldMap["name"].Type.GetScalarType() != schema_pb.ScalarType_STRING {
		t.Error("Expected name field to be STRING")
	}

	if fieldMap["score"].Type.GetScalarType() != schema_pb.ScalarType_FLOAT {
		t.Error("Expected score field to be FLOAT")
	}

	if fieldMap["timestamp"].Type.GetScalarType() != schema_pb.ScalarType_TIMESTAMP {
		t.Error("Expected timestamp field to be TIMESTAMP")
	}

	if fieldMap["data"].Type.GetScalarType() != schema_pb.ScalarType_BYTES {
		t.Error("Expected data field to be BYTES")
	}

	if fieldMap["active"].Type.GetScalarType() != schema_pb.ScalarType_BOOL {
		t.Error("Expected active field to be BOOL")
	}

	// Test array field
	if fieldMap["tags"].Type.GetListType() == nil {
		t.Error("Expected tags field to be LIST")
	}

	// Test nested object field
	if fieldMap["metadata"].Type.GetRecordType() == nil {
		t.Error("Expected metadata field to be RECORD")
	}

	// Test required fields
	if !fieldMap["id"].IsRequired {
		t.Error("Expected id field to be required")
	}

	if !fieldMap["name"].IsRequired {
		t.Error("Expected name field to be required")
	}

	if fieldMap["active"].IsRequired {
		t.Error("Expected active field to be optional")
	}
}

func TestJSONSchemaDecoder_EncodeFromRecordValue(t *testing.T) {
	schema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"id": {"type": "integer"},
			"name": {"type": "string"},
			"active": {"type": "boolean"}
		},
		"required": ["id", "name"]
	}`

	decoder, err := NewJSONSchemaDecoder(schema)
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	// Create test RecordValue
	testMap := map[string]interface{}{
		"id":     int64(123),
		"name":   "Test User",
		"active": true,
	}
	recordValue := MapToRecordValue(testMap)

	// Encode back to JSON
	jsonData, err := decoder.EncodeFromRecordValue(recordValue)
	if err != nil {
		t.Fatalf("Failed to encode RecordValue: %v", err)
	}

	// Verify the JSON is valid and contains expected data
	var result map[string]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		t.Fatalf("Failed to parse generated JSON: %v", err)
	}

	if result["id"] != float64(123) { // JSON numbers are float64
		t.Errorf("Expected id=123, got %v", result["id"])
	}

	if result["name"] != "Test User" {
		t.Errorf("Expected name='Test User', got %v", result["name"])
	}

	if result["active"] != true {
		t.Errorf("Expected active=true, got %v", result["active"])
	}
}

func TestJSONSchemaDecoder_ArrayAndPrimitiveSchemas(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		jsonData string
		expectOK bool
	}{
		{
			name: "array schema",
			schema: `{
				"$schema": "http://json-schema.org/draft-07/schema#",
				"type": "array",
				"items": {"type": "string"}
			}`,
			jsonData: `["item1", "item2", "item3"]`,
			expectOK: true,
		},
		{
			name: "string schema",
			schema: `{
				"$schema": "http://json-schema.org/draft-07/schema#",
				"type": "string"
			}`,
			jsonData: `"hello world"`,
			expectOK: true,
		},
		{
			name: "number schema",
			schema: `{
				"$schema": "http://json-schema.org/draft-07/schema#",
				"type": "number"
			}`,
			jsonData: `42.5`,
			expectOK: true,
		},
		{
			name: "boolean schema",
			schema: `{
				"$schema": "http://json-schema.org/draft-07/schema#",
				"type": "boolean"
			}`,
			jsonData: `true`,
			expectOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoder, err := NewJSONSchemaDecoder(tt.schema)
			if err != nil {
				t.Fatalf("Failed to create decoder: %v", err)
			}

			result, err := decoder.Decode([]byte(tt.jsonData))

			if (err == nil) != tt.expectOK {
				t.Errorf("Decode() error = %v, expectOK %v", err, tt.expectOK)
				return
			}

			if tt.expectOK && result == nil {
				t.Error("Expected non-nil result for valid data")
			}
		})
	}
}

func TestJSONSchemaDecoder_GetSchemaInfo(t *testing.T) {
	schema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "User Schema",
		"description": "A schema for user objects",
		"type": "object",
		"properties": {
			"id": {"type": "integer"}
		}
	}`

	decoder, err := NewJSONSchemaDecoder(schema)
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	info := decoder.GetSchemaInfo()

	if info["title"] != "User Schema" {
		t.Errorf("Expected title='User Schema', got %v", info["title"])
	}

	if info["description"] != "A schema for user objects" {
		t.Errorf("Expected description='A schema for user objects', got %v", info["description"])
	}

	if info["schema_version"] != "http://json-schema.org/draft-07/schema#" {
		t.Errorf("Expected schema_version='http://json-schema.org/draft-07/schema#', got %v", info["schema_version"])
	}

	if info["type"] != "object" {
		t.Errorf("Expected type='object', got %v", info["type"])
	}
}

// Benchmark tests
func BenchmarkJSONSchemaDecoder_Decode(b *testing.B) {
	schema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"id": {"type": "integer"},
			"name": {"type": "string"}
		}
	}`

	decoder, _ := NewJSONSchemaDecoder(schema)
	jsonData := []byte(`{"id": 123, "name": "John Doe"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = decoder.Decode(jsonData)
	}
}

func BenchmarkJSONSchemaDecoder_DecodeToRecordValue(b *testing.B) {
	schema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"id": {"type": "integer"},
			"name": {"type": "string"}
		}
	}`

	decoder, _ := NewJSONSchemaDecoder(schema)
	jsonData := []byte(`{"id": 123, "name": "John Doe"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = decoder.DecodeToRecordValue(jsonData)
	}
}
