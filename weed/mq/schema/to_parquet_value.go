package schema

import (
	"fmt"

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
		return parquet.ByteArrayValue(value.GetBytesValue()), nil
	case *schema_pb.Value_StringValue:
		return parquet.ByteArrayValue([]byte(value.GetStringValue())), nil
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

		// Validate input data before processing
		if len(decimalValue.Value) > 16 {
			// Very large decimal values - truncate for safety
			truncated := make([]byte, 16)
			copy(truncated, decimalValue.Value[:16])
			return parquet.ByteArrayValue(truncated), nil
		}

		// Always store DECIMAL as BINARY for consistency with schema mapping
		// This prevents type mismatches that cause panics in parquet-go
		// CRITICAL FIX: Create a new slice and ensure it's never nil
		if len(decimalValue.Value) == 0 {
			// Return a single zero byte for empty/zero decimals
			return parquet.ByteArrayValue([]byte{0x00}), nil
		}

		// Create a safe copy to prevent nil/malformed byte slice issues
		resultBytes := make([]byte, len(decimalValue.Value))
		copy(resultBytes, decimalValue.Value)
		return parquet.ByteArrayValue(resultBytes), nil
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
