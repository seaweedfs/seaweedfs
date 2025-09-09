package schema

import (
	"fmt"
	"strconv"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func rowBuilderVisit(rowBuilder *parquet.RowBuilder, fieldType *schema_pb.Type, levels *ParquetLevels, fieldValue *schema_pb.Value) (err error) {
	switch fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		// If value is missing, write NULL at the correct column to keep rows aligned
		if fieldValue == nil || fieldValue.Kind == nil {
			rowBuilder.Add(levels.startColumnIndex, parquet.NullValue())
			return nil
		}
		var parquetValue parquet.Value
		parquetValue, err = toParquetValueForType(fieldType, fieldValue)
		if err != nil {
			return
		}

		// Safety check: prevent nil byte arrays from reaching parquet library
		if parquetValue.Kind() == parquet.ByteArray {
			byteData := parquetValue.ByteArray()
			if byteData == nil {
				parquetValue = parquet.ByteArrayValue([]byte{})
			}
		}

		rowBuilder.Add(levels.startColumnIndex, parquetValue)
	case *schema_pb.Type_ListType:
		// Advance to list position even if value is missing
		rowBuilder.Next(levels.startColumnIndex)
		if fieldValue == nil || fieldValue.GetListValue() == nil {
			return nil
		}

		elementType := fieldType.GetListType().ElementType
		for _, value := range fieldValue.GetListValue().Values {
			if err = rowBuilderVisit(rowBuilder, elementType, levels, value); err != nil {
				return
			}
		}
	}
	return
}

func AddRecordValue(rowBuilder *parquet.RowBuilder, recordType *schema_pb.RecordType, parquetLevels *ParquetLevels, recordValue *schema_pb.RecordValue) error {
	visitor := func(fieldType *schema_pb.Type, levels *ParquetLevels, fieldValue *schema_pb.Value) (err error) {
		return rowBuilderVisit(rowBuilder, fieldType, levels, fieldValue)
	}
	fieldType := &schema_pb.Type{Kind: &schema_pb.Type_RecordType{RecordType: recordType}}
	fieldValue := &schema_pb.Value{Kind: &schema_pb.Value_RecordValue{RecordValue: recordValue}}
	return doVisitValue(fieldType, parquetLevels, fieldValue, visitor)
}

// typeValueVisitor is a function that is called for each value in a schema_pb.Value
// Find the column index.
// intended to be used in RowBuilder.Add(columnIndex, value)
type typeValueVisitor func(fieldType *schema_pb.Type, levels *ParquetLevels, fieldValue *schema_pb.Value) (err error)

// endIndex is exclusive
// same logic as RowBuilder.configure in row_builder.go
func doVisitValue(fieldType *schema_pb.Type, levels *ParquetLevels, fieldValue *schema_pb.Value, visitor typeValueVisitor) (err error) {
	switch fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		return visitor(fieldType, levels, fieldValue)
	case *schema_pb.Type_ListType:
		return visitor(fieldType, levels, fieldValue)
	case *schema_pb.Type_RecordType:
		for _, field := range fieldType.GetRecordType().Fields {
			var fv *schema_pb.Value
			if fieldValue != nil && fieldValue.GetRecordValue() != nil {
				var found bool
				fv, found = fieldValue.GetRecordValue().Fields[field.Name]
				if !found {
					// pass nil so visitor can emit NULL for alignment
					fv = nil
				}
			}
			fieldLevels := levels.levels[field.Name]
			err = doVisitValue(field.Type, fieldLevels, fv, visitor)
			if err != nil {
				return
			}
		}
		return
	}
	return
}

func toParquetValue(value *schema_pb.Value) (parquet.Value, error) {
	// Safety check for nil value
	if value == nil || value.Kind == nil {
		return parquet.NullValue(), fmt.Errorf("nil value or nil value kind")
	}

	switch value.Kind.(type) {
	case *schema_pb.Value_BoolValue:
		return parquet.BooleanValue(value.GetBoolValue()), nil
	case *schema_pb.Value_Int32Value:
		return parquet.Int32Value(value.GetInt32Value()), nil
	case *schema_pb.Value_Int64Value:
		return parquet.Int64Value(value.GetInt64Value()), nil
	case *schema_pb.Value_FloatValue:
		return parquet.FloatValue(value.GetFloatValue()), nil
	case *schema_pb.Value_DoubleValue:
		return parquet.DoubleValue(value.GetDoubleValue()), nil
	case *schema_pb.Value_BytesValue:
		// Handle nil byte slices to prevent growslice panic in parquet-go
		byteData := value.GetBytesValue()
		if byteData == nil {
			byteData = []byte{} // Use empty slice instead of nil
		}
		return parquet.ByteArrayValue(byteData), nil
	case *schema_pb.Value_StringValue:
		// Convert string to bytes, ensuring we never pass nil
		stringData := value.GetStringValue()
		return parquet.ByteArrayValue([]byte(stringData)), nil
	// Parquet logical types with safe conversion (preventing commit 7a4aeec60 panic)
	case *schema_pb.Value_TimestampValue:
		timestampValue := value.GetTimestampValue()
		if timestampValue == nil {
			return parquet.NullValue(), nil
		}
		return parquet.Int64Value(timestampValue.TimestampMicros), nil
	case *schema_pb.Value_DateValue:
		dateValue := value.GetDateValue()
		if dateValue == nil {
			return parquet.NullValue(), nil
		}
		return parquet.Int32Value(dateValue.DaysSinceEpoch), nil
	case *schema_pb.Value_DecimalValue:
		decimalValue := value.GetDecimalValue()
		if decimalValue == nil || decimalValue.Value == nil || len(decimalValue.Value) == 0 {
			return parquet.NullValue(), nil
		}

		// Validate input data - reject unreasonably large values instead of corrupting data
		if len(decimalValue.Value) > 64 {
			// Reject extremely large decimal values (>512 bits) as likely corrupted data
			// Better to fail fast than silently corrupt financial/scientific data
			return parquet.NullValue(), fmt.Errorf("decimal value too large: %d bytes (max 64)", len(decimalValue.Value))
		}

		// Convert to FixedLenByteArray to match schema (DECIMAL with FixedLenByteArray physical type)
		// This accommodates any precision up to 38 digits (16 bytes = 128 bits)

		// Pad or truncate to exactly 16 bytes for FixedLenByteArray
		fixedBytes := make([]byte, 16)
		if len(decimalValue.Value) <= 16 {
			// Right-align the value (big-endian)
			copy(fixedBytes[16-len(decimalValue.Value):], decimalValue.Value)
		} else {
			// Truncate if too large, taking the least significant bytes
			copy(fixedBytes, decimalValue.Value[len(decimalValue.Value)-16:])
		}

		return parquet.FixedLenByteArrayValue(fixedBytes), nil
	case *schema_pb.Value_TimeValue:
		timeValue := value.GetTimeValue()
		if timeValue == nil {
			return parquet.NullValue(), nil
		}
		return parquet.Int64Value(timeValue.TimeMicros), nil
	default:
		return parquet.NullValue(), fmt.Errorf("unknown value type: %T", value.Kind)
	}
}

// toParquetValueForType coerces a schema_pb.Value into a parquet.Value that matches the declared field type.
func toParquetValueForType(fieldType *schema_pb.Type, value *schema_pb.Value) (parquet.Value, error) {
	switch t := fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		switch t.ScalarType {
		case schema_pb.ScalarType_BOOL:
			switch v := value.Kind.(type) {
			case *schema_pb.Value_BoolValue:
				return parquet.BooleanValue(v.BoolValue), nil
			case *schema_pb.Value_StringValue:
				if b, err := strconv.ParseBool(v.StringValue); err == nil {
					return parquet.BooleanValue(b), nil
				}
				return parquet.BooleanValue(false), nil
			default:
				return parquet.BooleanValue(false), nil
			}

		case schema_pb.ScalarType_INT32:
			switch v := value.Kind.(type) {
			case *schema_pb.Value_Int32Value:
				return parquet.Int32Value(v.Int32Value), nil
			case *schema_pb.Value_Int64Value:
				return parquet.Int32Value(int32(v.Int64Value)), nil
			case *schema_pb.Value_DoubleValue:
				return parquet.Int32Value(int32(v.DoubleValue)), nil
			case *schema_pb.Value_StringValue:
				if i, err := strconv.ParseInt(v.StringValue, 10, 32); err == nil {
					return parquet.Int32Value(int32(i)), nil
				}
				return parquet.Int32Value(0), nil
			default:
				return parquet.Int32Value(0), nil
			}

		case schema_pb.ScalarType_INT64:
			switch v := value.Kind.(type) {
			case *schema_pb.Value_Int64Value:
				return parquet.Int64Value(v.Int64Value), nil
			case *schema_pb.Value_Int32Value:
				return parquet.Int64Value(int64(v.Int32Value)), nil
			case *schema_pb.Value_DoubleValue:
				return parquet.Int64Value(int64(v.DoubleValue)), nil
			case *schema_pb.Value_StringValue:
				if i, err := strconv.ParseInt(v.StringValue, 10, 64); err == nil {
					return parquet.Int64Value(i), nil
				}
				return parquet.Int64Value(0), nil
			default:
				return parquet.Int64Value(0), nil
			}

		case schema_pb.ScalarType_FLOAT:
			switch v := value.Kind.(type) {
			case *schema_pb.Value_FloatValue:
				return parquet.FloatValue(v.FloatValue), nil
			case *schema_pb.Value_DoubleValue:
				return parquet.FloatValue(float32(v.DoubleValue)), nil
			case *schema_pb.Value_Int64Value:
				return parquet.FloatValue(float32(v.Int64Value)), nil
			case *schema_pb.Value_StringValue:
				if f, err := strconv.ParseFloat(v.StringValue, 32); err == nil {
					return parquet.FloatValue(float32(f)), nil
				}
				return parquet.FloatValue(0), nil
			default:
				return parquet.FloatValue(0), nil
			}

		case schema_pb.ScalarType_DOUBLE:
			switch v := value.Kind.(type) {
			case *schema_pb.Value_DoubleValue:
				return parquet.DoubleValue(v.DoubleValue), nil
			case *schema_pb.Value_Int64Value:
				return parquet.DoubleValue(float64(v.Int64Value)), nil
			case *schema_pb.Value_Int32Value:
				return parquet.DoubleValue(float64(v.Int32Value)), nil
			case *schema_pb.Value_StringValue:
				if f, err := strconv.ParseFloat(v.StringValue, 64); err == nil {
					return parquet.DoubleValue(f), nil
				}
				return parquet.DoubleValue(0), nil
			default:
				return parquet.DoubleValue(0), nil
			}

		case schema_pb.ScalarType_BYTES:
			switch v := value.Kind.(type) {
			case *schema_pb.Value_BytesValue:
				b := v.BytesValue
				if b == nil {
					b = []byte{}
				}
				return parquet.ByteArrayValue(b), nil
			case *schema_pb.Value_StringValue:
				return parquet.ByteArrayValue([]byte(v.StringValue)), nil
			case *schema_pb.Value_Int64Value:
				return parquet.ByteArrayValue([]byte(strconv.FormatInt(v.Int64Value, 10))), nil
			case *schema_pb.Value_Int32Value:
				return parquet.ByteArrayValue([]byte(strconv.FormatInt(int64(v.Int32Value), 10))), nil
			case *schema_pb.Value_DoubleValue:
				return parquet.ByteArrayValue([]byte(strconv.FormatFloat(v.DoubleValue, 'f', -1, 64))), nil
			case *schema_pb.Value_FloatValue:
				return parquet.ByteArrayValue([]byte(strconv.FormatFloat(float64(v.FloatValue), 'f', -1, 32))), nil
			case *schema_pb.Value_BoolValue:
				if v.BoolValue {
					return parquet.ByteArrayValue([]byte("true")), nil
				}
				return parquet.ByteArrayValue([]byte("false")), nil
			default:
				return parquet.ByteArrayValue([]byte{}), nil
			}

		case schema_pb.ScalarType_STRING:
			// Same as bytes but semantically string
			switch v := value.Kind.(type) {
			case *schema_pb.Value_StringValue:
				return parquet.ByteArrayValue([]byte(v.StringValue)), nil
			default:
				// Fallback through bytes coercion
				b, _ := toParquetValueForType(&schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BYTES}}, value)
				return b, nil
			}

		case schema_pb.ScalarType_TIMESTAMP:
			switch v := value.Kind.(type) {
			case *schema_pb.Value_Int64Value:
				return parquet.Int64Value(v.Int64Value), nil
			case *schema_pb.Value_StringValue:
				if i, err := strconv.ParseInt(v.StringValue, 10, 64); err == nil {
					return parquet.Int64Value(i), nil
				}
				return parquet.Int64Value(0), nil
			default:
				return parquet.Int64Value(0), nil
			}

		case schema_pb.ScalarType_DATE:
			switch v := value.Kind.(type) {
			case *schema_pb.Value_Int32Value:
				return parquet.Int32Value(v.Int32Value), nil
			case *schema_pb.Value_Int64Value:
				return parquet.Int32Value(int32(v.Int64Value)), nil
			case *schema_pb.Value_StringValue:
				if i, err := strconv.ParseInt(v.StringValue, 10, 32); err == nil {
					return parquet.Int32Value(int32(i)), nil
				}
				return parquet.Int32Value(0), nil
			default:
				return parquet.Int32Value(0), nil
			}

		case schema_pb.ScalarType_DECIMAL:
			// Reuse existing conversion path (FixedLenByteArray 16)
			return toParquetValue(value)

		case schema_pb.ScalarType_TIME:
			switch v := value.Kind.(type) {
			case *schema_pb.Value_Int64Value:
				return parquet.Int64Value(v.Int64Value), nil
			case *schema_pb.Value_StringValue:
				if i, err := strconv.ParseInt(v.StringValue, 10, 64); err == nil {
					return parquet.Int64Value(i), nil
				}
				return parquet.Int64Value(0), nil
			default:
				return parquet.Int64Value(0), nil
			}
		}
	}
	// Fallback to generic conversion
	return toParquetValue(value)
}
