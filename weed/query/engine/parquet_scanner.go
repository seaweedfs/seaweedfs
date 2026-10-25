package engine

import (
	"fmt"
	"math/big"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
)

// convertSchemaValueToSQL converts schema_pb.Value to sqltypes.Value
func convertSchemaValueToSQL(value *schema_pb.Value) sqltypes.Value {
	if value == nil {
		return sqltypes.NULL
	}

	switch v := value.Kind.(type) {
	case *schema_pb.Value_BoolValue:
		if v.BoolValue {
			return sqltypes.NewInt32(1)
		}
		return sqltypes.NewInt32(0)
	case *schema_pb.Value_Int32Value:
		return sqltypes.NewInt32(v.Int32Value)
	case *schema_pb.Value_Int64Value:
		return sqltypes.NewInt64(v.Int64Value)
	case *schema_pb.Value_FloatValue:
		return sqltypes.NewFloat32(v.FloatValue)
	case *schema_pb.Value_DoubleValue:
		return sqltypes.NewFloat64(v.DoubleValue)
	case *schema_pb.Value_BytesValue:
		return sqltypes.NewVarBinary(string(v.BytesValue))
	case *schema_pb.Value_StringValue:
		return sqltypes.NewVarChar(v.StringValue)
	// Parquet logical types
	case *schema_pb.Value_TimestampValue:
		timestampValue := value.GetTimestampValue()
		if timestampValue == nil {
			return sqltypes.NULL
		}
		// Convert microseconds to time.Time and format as datetime string
		timestamp := time.UnixMicro(timestampValue.TimestampMicros)
		return sqltypes.MakeTrusted(sqltypes.Datetime, []byte(timestamp.Format("2006-01-02 15:04:05")))
	case *schema_pb.Value_DateValue:
		dateValue := value.GetDateValue()
		if dateValue == nil {
			return sqltypes.NULL
		}
		// Convert days since epoch to date string
		date := time.Unix(int64(dateValue.DaysSinceEpoch)*86400, 0).UTC()
		return sqltypes.MakeTrusted(sqltypes.Date, []byte(date.Format("2006-01-02")))
	case *schema_pb.Value_DecimalValue:
		decimalValue := value.GetDecimalValue()
		if decimalValue == nil {
			return sqltypes.NULL
		}
		// Convert decimal bytes to string representation
		decimalStr := decimalToStringHelper(decimalValue)
		return sqltypes.MakeTrusted(sqltypes.Decimal, []byte(decimalStr))
	case *schema_pb.Value_TimeValue:
		timeValue := value.GetTimeValue()
		if timeValue == nil {
			return sqltypes.NULL
		}
		// Convert microseconds since midnight to time string
		duration := time.Duration(timeValue.TimeMicros) * time.Microsecond
		timeOfDay := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).Add(duration)
		return sqltypes.MakeTrusted(sqltypes.Time, []byte(timeOfDay.Format("15:04:05")))
	default:
		return sqltypes.NewVarChar(fmt.Sprintf("%v", value))
	}
}

// decimalToStringHelper converts a DecimalValue to string representation
// This is a standalone version of the engine's decimalToString method
func decimalToStringHelper(decimalValue *schema_pb.DecimalValue) string {
	if decimalValue == nil || decimalValue.Value == nil {
		return "0"
	}

	// Convert bytes back to big.Int
	intValue := new(big.Int).SetBytes(decimalValue.Value)

	// Convert to string with proper decimal placement
	str := intValue.String()

	// Handle decimal placement based on scale
	scale := int(decimalValue.Scale)
	if scale > 0 && len(str) > scale {
		// Insert decimal point
		decimalPos := len(str) - scale
		return str[:decimalPos] + "." + str[decimalPos:]
	}

	return str
}
