package engine

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
)

// formatAggregationResult formats an aggregation result into a SQL value
func (e *SQLEngine) formatAggregationResult(spec AggregationSpec, result AggregationResult) sqltypes.Value {
	switch spec.Function {
	case "COUNT":
		return sqltypes.NewInt64(result.Count)
	case "SUM":
		return sqltypes.NewFloat64(result.Sum)
	case "AVG":
		return sqltypes.NewFloat64(result.Sum) // Sum contains the average for AVG
	case "MIN":
		if result.Min != nil {
			return e.convertRawValueToSQL(result.Min)
		}
		return sqltypes.NULL
	case "MAX":
		if result.Max != nil {
			return e.convertRawValueToSQL(result.Max)
		}
		return sqltypes.NULL
	}
	return sqltypes.NULL
}

// convertRawValueToSQL converts a raw Go value to a SQL value
func (e *SQLEngine) convertRawValueToSQL(value interface{}) sqltypes.Value {
	switch v := value.(type) {
	case int32:
		return sqltypes.NewInt32(v)
	case int64:
		return sqltypes.NewInt64(v)
	case float32:
		return sqltypes.NewFloat32(v)
	case float64:
		return sqltypes.NewFloat64(v)
	case string:
		return sqltypes.NewVarChar(v)
	case bool:
		if v {
			return sqltypes.NewVarChar("1")
		}
		return sqltypes.NewVarChar("0")
	}
	return sqltypes.NULL
}

// extractRawValue extracts the raw Go value from a schema_pb.Value
func (e *SQLEngine) extractRawValue(value *schema_pb.Value) interface{} {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		return v.Int32Value
	case *schema_pb.Value_Int64Value:
		return v.Int64Value
	case *schema_pb.Value_FloatValue:
		return v.FloatValue
	case *schema_pb.Value_DoubleValue:
		return v.DoubleValue
	case *schema_pb.Value_StringValue:
		return v.StringValue
	case *schema_pb.Value_BoolValue:
		return v.BoolValue
	case *schema_pb.Value_BytesValue:
		return string(v.BytesValue) // Convert bytes to string for comparison
	}
	return nil
}

// compareValues compares two schema_pb.Value objects
func (e *SQLEngine) compareValues(value1 *schema_pb.Value, value2 *schema_pb.Value) int {
	if value2 == nil {
		return 1 // value1 > nil
	}
	raw1 := e.extractRawValue(value1)
	raw2 := e.extractRawValue(value2)
	if raw1 == nil {
		return -1
	}
	if raw2 == nil {
		return 1
	}

	// Simple comparison - in a full implementation this would handle type coercion
	switch v1 := raw1.(type) {
	case int32:
		if v2, ok := raw2.(int32); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case int64:
		if v2, ok := raw2.(int64); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case float32:
		if v2, ok := raw2.(float32); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case float64:
		if v2, ok := raw2.(float64); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case string:
		if v2, ok := raw2.(string); ok {
			if v1 < v2 {
				return -1
			} else if v1 > v2 {
				return 1
			}
			return 0
		}
	case bool:
		if v2, ok := raw2.(bool); ok {
			if v1 == v2 {
				return 0
			} else if v1 && !v2 {
				return 1
			}
			return -1
		}
	}
	return 0
}

// convertRawValueToSchemaValue converts raw Go values back to schema_pb.Value for comparison
func (e *SQLEngine) convertRawValueToSchemaValue(rawValue interface{}) *schema_pb.Value {
	switch v := rawValue.(type) {
	case int32:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: v}}
	case int64:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v}}
	case float32:
		return &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: v}}
	case float64:
		return &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: v}}
	case string:
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v}}
	case bool:
		return &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: v}}
	case []byte:
		return &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: v}}
	default:
		// Convert other types to string as fallback
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: fmt.Sprintf("%v", v)}}
	}
}

// convertJSONValueToSchemaValue converts JSON values to schema_pb.Value
func (e *SQLEngine) convertJSONValueToSchemaValue(jsonValue interface{}) *schema_pb.Value {
	switch v := jsonValue.(type) {
	case string:
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v}}
	case float64:
		// JSON numbers are always float64, try to detect if it's actually an integer
		if v == float64(int64(v)) {
			return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: int64(v)}}
		}
		return &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: v}}
	case bool:
		return &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: v}}
	case nil:
		return nil
	default:
		// Convert other types to string
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: fmt.Sprintf("%v", v)}}
	}
}

// Helper functions for aggregation processing

// isNullValue checks if a schema_pb.Value is null or empty
func (e *SQLEngine) isNullValue(value *schema_pb.Value) bool {
	return value == nil || value.Kind == nil
}

// convertToNumber converts a schema_pb.Value to a float64 for numeric operations
func (e *SQLEngine) convertToNumber(value *schema_pb.Value) *float64 {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		result := float64(v.Int32Value)
		return &result
	case *schema_pb.Value_Int64Value:
		result := float64(v.Int64Value)
		return &result
	case *schema_pb.Value_FloatValue:
		result := float64(v.FloatValue)
		return &result
	case *schema_pb.Value_DoubleValue:
		return &v.DoubleValue
	}
	return nil
}
