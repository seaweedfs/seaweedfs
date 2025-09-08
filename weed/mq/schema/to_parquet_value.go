package schema

import (
	"fmt"
	"math/big"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func rowBuilderVisit(rowBuilder *parquet.RowBuilder, fieldType *schema_pb.Type, levels *ParquetLevels, fieldValue *schema_pb.Value) (err error) {
	switch fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		var parquetValue parquet.Value
		parquetValue, err = toParquetValue(fieldValue)
		if err != nil {
			return
		}
		rowBuilder.Add(levels.startColumnIndex, parquetValue)
		// fmt.Printf("rowBuilder.Add %d %v\n", columnIndex, parquetValue)
	case *schema_pb.Type_ListType:
		rowBuilder.Next(levels.startColumnIndex)
		// fmt.Printf("rowBuilder.Next %d\n", columnIndex)

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
			fieldValue, found := fieldValue.GetRecordValue().Fields[field.Name]
			if !found {
				// TODO check this if no such field found
				continue
			}
			fieldLevels := levels.levels[field.Name]
			err = doVisitValue(field.Type, fieldLevels, fieldValue, visitor)
			if err != nil {
				return
			}
		}
		return
	}
	return
}

func toParquetValue(value *schema_pb.Value) (parquet.Value, error) {
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
		return parquet.ByteArrayValue(value.GetBytesValue()), nil
	case *schema_pb.Value_StringValue:
		return parquet.ByteArrayValue([]byte(value.GetStringValue())), nil
	// Parquet logical types
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

		// Store DECIMAL according to Parquet specification based on precision
		// Spec: unscaledValue * 10^(-scale) where unscaledValue is stored as signed integer
		precision := decimalValue.Precision

		// Handle signed two's complement conversion for proper Parquet DECIMAL
		var unscaledValue *big.Int
		if len(decimalValue.Value) > 0 && decimalValue.Value[0]&0x80 != 0 {
			// Negative number in two's complement - convert properly
			unscaledValue = new(big.Int)
			unscaledValue.SetBytes(decimalValue.Value)
			// Convert from unsigned interpretation to signed by subtracting 2^(8*len)
			bitLen := len(decimalValue.Value) * 8
			maxVal := new(big.Int).Lsh(big.NewInt(1), uint(bitLen))
			unscaledValue.Sub(unscaledValue, maxVal)
		} else {
			// Positive number - SetBytes works correctly
			unscaledValue = new(big.Int).SetBytes(decimalValue.Value)
		}

		if precision <= 9 {
			// Store as INT32 for precision ≤ 9
			if unscaledValue.IsInt64() {
				val := unscaledValue.Int64()
				if val <= 2147483647 && val >= -2147483648 {
					return parquet.Int32Value(int32(val)), nil
				}
			}
			// Fallback to 0 if out of range
			return parquet.Int32Value(0), nil

		} else if precision <= 18 {
			// Store as INT64 for 9 < precision ≤ 18
			if unscaledValue.IsInt64() {
				return parquet.Int64Value(unscaledValue.Int64()), nil
			}
			// Fallback to 0 if out of range
			return parquet.Int64Value(0), nil

		} else {
			// Store as BINARY for precision > 18 (unlimited precision)
			// Ensure proper two's complement big-endian format
			return parquet.ByteArrayValue(decimalValue.Value), nil
		}
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
