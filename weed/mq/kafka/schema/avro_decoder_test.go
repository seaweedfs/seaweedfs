package schema

import (
	"reflect"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestNewAvroDecoder(t *testing.T) {
	tests := []struct {
		name      string
		schema    string
		expectErr bool
	}{
		{
			name: "valid record schema",
			schema: `{
				"type": "record",
				"name": "User",
				"fields": [
					{"name": "id", "type": "int"},
					{"name": "name", "type": "string"}
				]
			}`,
			expectErr: false,
		},
		{
			name: "valid enum schema",
			schema: `{
				"type": "enum",
				"name": "Color",
				"symbols": ["RED", "GREEN", "BLUE"]
			}`,
			expectErr: false,
		},
		{
			name:      "invalid schema",
			schema:    `{"invalid": "schema"}`,
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
			decoder, err := NewAvroDecoder(tt.schema)

			if (err != nil) != tt.expectErr {
				t.Errorf("NewAvroDecoder() error = %v, expectErr %v", err, tt.expectErr)
				return
			}

			if !tt.expectErr && decoder == nil {
				t.Error("Expected non-nil decoder for valid schema")
			}
		})
	}
}

func TestAvroDecoder_Decode(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": ["null", "string"], "default": null}
		]
	}`

	decoder, err := NewAvroDecoder(schema)
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	// Create test data
	codec, _ := goavro.NewCodec(schema)
	testRecord := map[string]interface{}{
		"id":   int32(123),
		"name": "John Doe",
		"email": map[string]interface{}{
			"string": "john@example.com", // Avro union format
		},
	}

	// Encode to binary
	binary, err := codec.BinaryFromNative(nil, testRecord)
	if err != nil {
		t.Fatalf("Failed to encode test data: %v", err)
	}

	// Test decoding
	result, err := decoder.Decode(binary)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	// Verify results
	if result["id"] != int32(123) {
		t.Errorf("Expected id=123, got %v", result["id"])
	}

	if result["name"] != "John Doe" {
		t.Errorf("Expected name='John Doe', got %v", result["name"])
	}

	// For union types, Avro returns a map with the type name as key
	if emailMap, ok := result["email"].(map[string]interface{}); ok {
		if emailMap["string"] != "john@example.com" {
			t.Errorf("Expected email='john@example.com', got %v", emailMap["string"])
		}
	} else {
		t.Errorf("Expected email to be a union map, got %v", result["email"])
	}
}

func TestAvroDecoder_DecodeToRecordValue(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "SimpleRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	decoder, err := NewAvroDecoder(schema)
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	// Create and encode test data
	codec, _ := goavro.NewCodec(schema)
	testRecord := map[string]interface{}{
		"id":   int32(456),
		"name": "Jane Smith",
	}

	binary, err := codec.BinaryFromNative(nil, testRecord)
	if err != nil {
		t.Fatalf("Failed to encode test data: %v", err)
	}

	// Test decoding to RecordValue
	recordValue, err := decoder.DecodeToRecordValue(binary)
	if err != nil {
		t.Fatalf("Failed to decode to RecordValue: %v", err)
	}

	// Verify RecordValue structure
	if recordValue.Fields == nil {
		t.Fatal("Expected non-nil fields")
	}

	idValue := recordValue.Fields["id"]
	if idValue == nil {
		t.Fatal("Expected id field")
	}

	if idValue.GetInt32Value() != 456 {
		t.Errorf("Expected id=456, got %v", idValue.GetInt32Value())
	}

	nameValue := recordValue.Fields["name"]
	if nameValue == nil {
		t.Fatal("Expected name field")
	}

	if nameValue.GetStringValue() != "Jane Smith" {
		t.Errorf("Expected name='Jane Smith', got %v", nameValue.GetStringValue())
	}
}

func TestMapToRecordValue(t *testing.T) {
	testMap := map[string]interface{}{
		"bool_field":   true,
		"int32_field":  int32(123),
		"int64_field":  int64(456),
		"float_field":  float32(1.23),
		"double_field": float64(4.56),
		"string_field": "hello",
		"bytes_field":  []byte("world"),
		"null_field":   nil,
		"array_field":  []interface{}{"a", "b", "c"},
		"nested_field": map[string]interface{}{
			"inner": "value",
		},
	}

	recordValue := MapToRecordValue(testMap)

	// Test each field type
	if !recordValue.Fields["bool_field"].GetBoolValue() {
		t.Error("Expected bool_field=true")
	}

	if recordValue.Fields["int32_field"].GetInt32Value() != 123 {
		t.Error("Expected int32_field=123")
	}

	if recordValue.Fields["int64_field"].GetInt64Value() != 456 {
		t.Error("Expected int64_field=456")
	}

	if recordValue.Fields["float_field"].GetFloatValue() != 1.23 {
		t.Error("Expected float_field=1.23")
	}

	if recordValue.Fields["double_field"].GetDoubleValue() != 4.56 {
		t.Error("Expected double_field=4.56")
	}

	if recordValue.Fields["string_field"].GetStringValue() != "hello" {
		t.Error("Expected string_field='hello'")
	}

	if string(recordValue.Fields["bytes_field"].GetBytesValue()) != "world" {
		t.Error("Expected bytes_field='world'")
	}

	// Test null value (converted to empty string)
	if recordValue.Fields["null_field"].GetStringValue() != "" {
		t.Error("Expected null_field to be empty string")
	}

	// Test array
	arrayValue := recordValue.Fields["array_field"].GetListValue()
	if arrayValue == nil || len(arrayValue.Values) != 3 {
		t.Error("Expected array with 3 elements")
	}

	// Test nested record
	nestedValue := recordValue.Fields["nested_field"].GetRecordValue()
	if nestedValue == nil {
		t.Fatal("Expected nested record")
	}

	if nestedValue.Fields["inner"].GetStringValue() != "value" {
		t.Error("Expected nested inner='value'")
	}
}

func TestGoValueToSchemaValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected func(*schema_pb.Value) bool
	}{
		{
			name:  "nil value",
			input: nil,
			expected: func(v *schema_pb.Value) bool {
				return v.GetStringValue() == ""
			},
		},
		{
			name:  "bool value",
			input: true,
			expected: func(v *schema_pb.Value) bool {
				return v.GetBoolValue() == true
			},
		},
		{
			name:  "int32 value",
			input: int32(123),
			expected: func(v *schema_pb.Value) bool {
				return v.GetInt32Value() == 123
			},
		},
		{
			name:  "int64 value",
			input: int64(456),
			expected: func(v *schema_pb.Value) bool {
				return v.GetInt64Value() == 456
			},
		},
		{
			name:  "string value",
			input: "test",
			expected: func(v *schema_pb.Value) bool {
				return v.GetStringValue() == "test"
			},
		},
		{
			name:  "bytes value",
			input: []byte("data"),
			expected: func(v *schema_pb.Value) bool {
				return string(v.GetBytesValue()) == "data"
			},
		},
		{
			name:  "time value",
			input: time.Unix(1234567890, 0),
			expected: func(v *schema_pb.Value) bool {
				return v.GetTimestampValue() != nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := goValueToSchemaValue(tt.input)
			if !tt.expected(result) {
				t.Errorf("goValueToSchemaValue() failed for %v", tt.input)
			}
		})
	}
}

func TestInferRecordTypeFromMap(t *testing.T) {
	testMap := map[string]interface{}{
		"id":       int64(123),
		"name":     "test",
		"active":   true,
		"score":    float64(95.5),
		"tags":     []interface{}{"tag1", "tag2"},
		"metadata": map[string]interface{}{"key": "value"},
	}

	recordType := InferRecordTypeFromMap(testMap)

	if len(recordType.Fields) != 6 {
		t.Errorf("Expected 6 fields, got %d", len(recordType.Fields))
	}

	// Create a map for easier field lookup
	fieldMap := make(map[string]*schema_pb.Field)
	for _, field := range recordType.Fields {
		fieldMap[field.Name] = field
	}

	// Test field types
	if fieldMap["id"].Type.GetScalarType() != schema_pb.ScalarType_INT64 {
		t.Error("Expected id field to be INT64")
	}

	if fieldMap["name"].Type.GetScalarType() != schema_pb.ScalarType_STRING {
		t.Error("Expected name field to be STRING")
	}

	if fieldMap["active"].Type.GetScalarType() != schema_pb.ScalarType_BOOL {
		t.Error("Expected active field to be BOOL")
	}

	if fieldMap["score"].Type.GetScalarType() != schema_pb.ScalarType_DOUBLE {
		t.Error("Expected score field to be DOUBLE")
	}

	// Test array field
	if fieldMap["tags"].Type.GetListType() == nil {
		t.Error("Expected tags field to be LIST")
	}

	// Test nested record field
	if fieldMap["metadata"].Type.GetRecordType() == nil {
		t.Error("Expected metadata field to be RECORD")
	}
}

func TestInferTypeFromValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected schema_pb.ScalarType
	}{
		{"nil", nil, schema_pb.ScalarType_STRING}, // Default for nil
		{"bool", true, schema_pb.ScalarType_BOOL},
		{"int32", int32(123), schema_pb.ScalarType_INT32},
		{"int64", int64(456), schema_pb.ScalarType_INT64},
		{"int", int(789), schema_pb.ScalarType_INT64},
		{"float32", float32(1.23), schema_pb.ScalarType_FLOAT},
		{"float64", float64(4.56), schema_pb.ScalarType_DOUBLE},
		{"string", "test", schema_pb.ScalarType_STRING},
		{"bytes", []byte("data"), schema_pb.ScalarType_BYTES},
		{"time", time.Now(), schema_pb.ScalarType_TIMESTAMP},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferTypeFromValue(tt.input)

			// Handle special cases
			if tt.input == nil || reflect.TypeOf(tt.input).Kind() == reflect.Slice ||
				reflect.TypeOf(tt.input).Kind() == reflect.Map {
				// Skip scalar type check for complex types
				return
			}

			if result.GetScalarType() != tt.expected {
				t.Errorf("inferTypeFromValue() = %v, want %v", result.GetScalarType(), tt.expected)
			}
		})
	}
}

// Integration test with real Avro data
func TestAvroDecoder_Integration(t *testing.T) {
	// Complex Avro schema with nested records and arrays
	schema := `{
		"type": "record",
		"name": "Order",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "customer_id", "type": "int"},
			{"name": "total", "type": "double"},
			{"name": "items", "type": {
				"type": "array",
				"items": {
					"type": "record",
					"name": "Item",
					"fields": [
						{"name": "product_id", "type": "string"},
						{"name": "quantity", "type": "int"},
						{"name": "price", "type": "double"}
					]
				}
			}},
			{"name": "metadata", "type": {
				"type": "record",
				"name": "Metadata",
				"fields": [
					{"name": "source", "type": "string"},
					{"name": "timestamp", "type": "long"}
				]
			}}
		]
	}`

	decoder, err := NewAvroDecoder(schema)
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	// Create complex test data
	codec, _ := goavro.NewCodec(schema)
	testOrder := map[string]interface{}{
		"id":          "order-123",
		"customer_id": int32(456),
		"total":       float64(99.99),
		"items": []interface{}{
			map[string]interface{}{
				"product_id": "prod-1",
				"quantity":   int32(2),
				"price":      float64(29.99),
			},
			map[string]interface{}{
				"product_id": "prod-2",
				"quantity":   int32(1),
				"price":      float64(39.99),
			},
		},
		"metadata": map[string]interface{}{
			"source":    "web",
			"timestamp": int64(1234567890),
		},
	}

	// Encode to binary
	binary, err := codec.BinaryFromNative(nil, testOrder)
	if err != nil {
		t.Fatalf("Failed to encode test data: %v", err)
	}

	// Decode to RecordValue
	recordValue, err := decoder.DecodeToRecordValue(binary)
	if err != nil {
		t.Fatalf("Failed to decode to RecordValue: %v", err)
	}

	// Verify complex structure
	if recordValue.Fields["id"].GetStringValue() != "order-123" {
		t.Error("Expected order ID to be preserved")
	}

	if recordValue.Fields["customer_id"].GetInt32Value() != 456 {
		t.Error("Expected customer ID to be preserved")
	}

	// Check array handling
	itemsArray := recordValue.Fields["items"].GetListValue()
	if itemsArray == nil || len(itemsArray.Values) != 2 {
		t.Fatal("Expected items array with 2 elements")
	}

	// Check nested record handling
	metadataRecord := recordValue.Fields["metadata"].GetRecordValue()
	if metadataRecord == nil {
		t.Fatal("Expected metadata record")
	}

	if metadataRecord.Fields["source"].GetStringValue() != "web" {
		t.Error("Expected metadata source to be preserved")
	}
}

// Benchmark tests
func BenchmarkAvroDecoder_Decode(b *testing.B) {
	schema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`

	decoder, _ := NewAvroDecoder(schema)
	codec, _ := goavro.NewCodec(schema)

	testRecord := map[string]interface{}{
		"id":   int32(123),
		"name": "John Doe",
	}

	binary, _ := codec.BinaryFromNative(nil, testRecord)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = decoder.Decode(binary)
	}
}

func BenchmarkMapToRecordValue(b *testing.B) {
	testMap := map[string]interface{}{
		"id":     int64(123),
		"name":   "test",
		"active": true,
		"score":  float64(95.5),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = MapToRecordValue(testMap)
	}
}
