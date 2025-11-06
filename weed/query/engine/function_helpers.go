package engine

import (
	"fmt"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// Helper function to convert schema_pb.Value to float64
func (e *SQLEngine) valueToFloat64(value *schema_pb.Value) (float64, error) {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		return float64(v.Int32Value), nil
	case *schema_pb.Value_Int64Value:
		return float64(v.Int64Value), nil
	case *schema_pb.Value_FloatValue:
		return float64(v.FloatValue), nil
	case *schema_pb.Value_DoubleValue:
		return v.DoubleValue, nil
	case *schema_pb.Value_StringValue:
		// Try to parse string as number
		if f, err := strconv.ParseFloat(v.StringValue, 64); err == nil {
			return f, nil
		}
		return 0, fmt.Errorf("cannot convert string '%s' to number", v.StringValue)
	case *schema_pb.Value_BoolValue:
		if v.BoolValue {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert value type to number")
	}
}

// Helper function to check if a value is an integer type
func (e *SQLEngine) isIntegerValue(value *schema_pb.Value) bool {
	switch value.Kind.(type) {
	case *schema_pb.Value_Int32Value, *schema_pb.Value_Int64Value:
		return true
	default:
		return false
	}
}

// Helper function to convert schema_pb.Value to string
func (e *SQLEngine) valueToString(value *schema_pb.Value) (string, error) {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_StringValue:
		return v.StringValue, nil
	case *schema_pb.Value_Int32Value:
		return strconv.FormatInt(int64(v.Int32Value), 10), nil
	case *schema_pb.Value_Int64Value:
		return strconv.FormatInt(v.Int64Value, 10), nil
	case *schema_pb.Value_FloatValue:
		return strconv.FormatFloat(float64(v.FloatValue), 'g', -1, 32), nil
	case *schema_pb.Value_DoubleValue:
		return strconv.FormatFloat(v.DoubleValue, 'g', -1, 64), nil
	case *schema_pb.Value_BoolValue:
		if v.BoolValue {
			return "true", nil
		}
		return "false", nil
	case *schema_pb.Value_BytesValue:
		return string(v.BytesValue), nil
	default:
		return "", fmt.Errorf("cannot convert value type to string")
	}
}

// Helper function to convert schema_pb.Value to int64
func (e *SQLEngine) valueToInt64(value *schema_pb.Value) (int64, error) {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		return int64(v.Int32Value), nil
	case *schema_pb.Value_Int64Value:
		return v.Int64Value, nil
	case *schema_pb.Value_FloatValue:
		return int64(v.FloatValue), nil
	case *schema_pb.Value_DoubleValue:
		return int64(v.DoubleValue), nil
	case *schema_pb.Value_StringValue:
		if i, err := strconv.ParseInt(v.StringValue, 10, 64); err == nil {
			return i, nil
		}
		return 0, fmt.Errorf("cannot convert string '%s' to integer", v.StringValue)
	default:
		return 0, fmt.Errorf("cannot convert value type to integer")
	}
}

// Helper function to convert schema_pb.Value to time.Time
func (e *SQLEngine) valueToTime(value *schema_pb.Value) (time.Time, error) {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_TimestampValue:
		if v.TimestampValue == nil {
			return time.Time{}, fmt.Errorf("null timestamp value")
		}
		return time.UnixMicro(v.TimestampValue.TimestampMicros), nil
	case *schema_pb.Value_StringValue:
		// Try to parse various date/time string formats
		dateFormats := []struct {
			format string
			tz     *time.Location
		}{
			{"2006-01-02 15:04:05", time.Local}, // Local time assumed for non-timezone formats
			{"2006-01-02T15:04:05Z", time.UTC},  // UTC format
			{"2006-01-02T15:04:05", time.Local}, // Local time assumed
			{"2006-01-02", time.Local},          // Local time assumed for date only
			{"15:04:05", time.Local},            // Local time assumed for time only
		}

		for _, formatSpec := range dateFormats {
			if t, err := time.ParseInLocation(formatSpec.format, v.StringValue, formatSpec.tz); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("unable to parse date/time string: %s", v.StringValue)
	case *schema_pb.Value_Int64Value:
		// Assume Unix timestamp (seconds)
		return time.Unix(v.Int64Value, 0), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert value type to date/time")
	}
}
