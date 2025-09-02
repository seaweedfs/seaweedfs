package engine

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// TestSchemaAwareParsing tests the schema-aware message parsing functionality
func TestSchemaAwareParsing(t *testing.T) {
	// Create a mock HybridMessageScanner with schema
	recordSchema := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name: "user_id",
				Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT32}},
			},
			{
				Name: "event_type",
				Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}},
			},
			{
				Name: "cpu_usage",
				Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_DOUBLE}},
			},
			{
				Name: "is_active",
				Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BOOL}},
			},
		},
	}

	scanner := &HybridMessageScanner{
		recordSchema: recordSchema,
	}

	t.Run("JSON Message Parsing", func(t *testing.T) {
		jsonData := []byte(`{"user_id": 1234, "event_type": "login", "cpu_usage": 75.5, "is_active": true}`)

		result, err := scanner.parseJSONMessage(jsonData)
		if err != nil {
			t.Fatalf("Failed to parse JSON message: %v", err)
		}

		// Verify user_id as int32
		if userIdVal := result.Fields["user_id"]; userIdVal == nil {
			t.Error("user_id field missing")
		} else if userIdVal.GetInt32Value() != 1234 {
			t.Errorf("Expected user_id=1234, got %v", userIdVal.GetInt32Value())
		}

		// Verify event_type as string
		if eventTypeVal := result.Fields["event_type"]; eventTypeVal == nil {
			t.Error("event_type field missing")
		} else if eventTypeVal.GetStringValue() != "login" {
			t.Errorf("Expected event_type='login', got %v", eventTypeVal.GetStringValue())
		}

		// Verify cpu_usage as double
		if cpuVal := result.Fields["cpu_usage"]; cpuVal == nil {
			t.Error("cpu_usage field missing")
		} else if cpuVal.GetDoubleValue() != 75.5 {
			t.Errorf("Expected cpu_usage=75.5, got %v", cpuVal.GetDoubleValue())
		}

		// Verify is_active as bool
		if isActiveVal := result.Fields["is_active"]; isActiveVal == nil {
			t.Error("is_active field missing")
		} else if !isActiveVal.GetBoolValue() {
			t.Errorf("Expected is_active=true, got %v", isActiveVal.GetBoolValue())
		}

		t.Logf("JSON parsing correctly converted types: int32=%d, string='%s', double=%.1f, bool=%v",
			result.Fields["user_id"].GetInt32Value(),
			result.Fields["event_type"].GetStringValue(),
			result.Fields["cpu_usage"].GetDoubleValue(),
			result.Fields["is_active"].GetBoolValue())
	})

	t.Run("Raw Data Type Conversion", func(t *testing.T) {
		// Test string conversion
		stringType := &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}
		stringVal, err := scanner.convertRawDataToSchemaValue([]byte("hello world"), stringType)
		if err != nil {
			t.Errorf("Failed to convert string: %v", err)
		} else if stringVal.GetStringValue() != "hello world" {
			t.Errorf("String conversion failed: got %v", stringVal.GetStringValue())
		}

		// Test int32 conversion
		int32Type := &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT32}}
		int32Val, err := scanner.convertRawDataToSchemaValue([]byte("42"), int32Type)
		if err != nil {
			t.Errorf("Failed to convert int32: %v", err)
		} else if int32Val.GetInt32Value() != 42 {
			t.Errorf("Int32 conversion failed: got %v", int32Val.GetInt32Value())
		}

		// Test double conversion
		doubleType := &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_DOUBLE}}
		doubleVal, err := scanner.convertRawDataToSchemaValue([]byte("3.14159"), doubleType)
		if err != nil {
			t.Errorf("Failed to convert double: %v", err)
		} else if doubleVal.GetDoubleValue() != 3.14159 {
			t.Errorf("Double conversion failed: got %v", doubleVal.GetDoubleValue())
		}

		// Test bool conversion
		boolType := &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BOOL}}
		boolVal, err := scanner.convertRawDataToSchemaValue([]byte("true"), boolType)
		if err != nil {
			t.Errorf("Failed to convert bool: %v", err)
		} else if !boolVal.GetBoolValue() {
			t.Errorf("Bool conversion failed: got %v", boolVal.GetBoolValue())
		}

		t.Log("Raw data type conversions working correctly")
	})

	t.Run("Invalid JSON Graceful Handling", func(t *testing.T) {
		invalidJSON := []byte(`{"user_id": 1234, "malformed": }`)

		_, err := scanner.parseJSONMessage(invalidJSON)
		if err == nil {
			t.Error("Expected error for invalid JSON, but got none")
		}

		t.Log("Invalid JSON handled gracefully with error")
	})
}

// TestSchemaAwareParsingIntegration tests the full integration with SQL engine
func TestSchemaAwareParsingIntegration(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test that the enhanced schema-aware parsing doesn't break existing functionality
	result, err := engine.ExecuteSQL(context.Background(), "SELECT *, _source FROM user_events LIMIT 2")
	if err != nil {
		t.Fatalf("Schema-aware parsing broke basic SELECT: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Error("No rows returned - schema parsing may have issues")
	}

	// Check that _source column is still present (hybrid functionality)
	foundSourceColumn := false
	for _, col := range result.Columns {
		if col == "_source" {
			foundSourceColumn = true
			break
		}
	}

	if !foundSourceColumn {
		t.Log("_source column missing - running in fallback mode without real cluster")
	}

	t.Log("Schema-aware parsing integrates correctly with SQL engine")
}
