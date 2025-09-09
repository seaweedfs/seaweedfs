package schema

import (
	"bytes"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// ToRecordValue converts a parquet.Row to a schema_pb.RecordValue
// This does not work or did not test with nested structures.
// Using this may fail to convert the parquet.Row to schema_pb.RecordValue
func ToRecordValue(recordType *schema_pb.RecordType, parquetLevels *ParquetLevels, row parquet.Row) (*schema_pb.RecordValue, error) {
	values := []parquet.Value(row)
	recordValue, _, err := toRecordValue(recordType, parquetLevels, values, 0)
	if err != nil {
		return nil, err
	}
	return recordValue.GetRecordValue(), nil
}

func ToValue(t *schema_pb.Type, levels *ParquetLevels, values []parquet.Value, valueIndex int) (value *schema_pb.Value, endValueIndex int, err error) {
	switch t.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		return toScalarValue(t.GetScalarType(), levels, values, valueIndex)
	case *schema_pb.Type_ListType:
		return toListValue(t.GetListType(), levels, values, valueIndex)
	case *schema_pb.Type_RecordType:
		return toRecordValue(t.GetRecordType(), levels, values, valueIndex)
	}
	return nil, valueIndex, fmt.Errorf("unsupported type: %v", t)
}

func toRecordValue(recordType *schema_pb.RecordType, levels *ParquetLevels, values []parquet.Value, valueIndex int) (*schema_pb.Value, int, error) {
	recordValue := schema_pb.RecordValue{Fields: make(map[string]*schema_pb.Value)}
	for _, field := range recordType.Fields {
		fieldLevels := levels.levels[field.Name]
		fieldValue, endValueIndex, err := ToValue(field.Type, fieldLevels, values, valueIndex)
		if err != nil {
			return nil, 0, err
		}
		valueIndex = endValueIndex
		recordValue.Fields[field.Name] = fieldValue
	}
	return &schema_pb.Value{Kind: &schema_pb.Value_RecordValue{RecordValue: &recordValue}}, valueIndex, nil
}

func toListValue(listType *schema_pb.ListType, levels *ParquetLevels, values []parquet.Value, valueIndex int) (listValue *schema_pb.Value, endValueIndex int, err error) {
	listValues := make([]*schema_pb.Value, 0)
	var value *schema_pb.Value
	for valueIndex < len(values) {
		if values[valueIndex].Column() != levels.startColumnIndex {
			break
		}
		value, valueIndex, err = ToValue(listType.ElementType, levels, values, valueIndex)
		if err != nil {
			return nil, valueIndex, err
		}
		listValues = append(listValues, value)
	}
	return &schema_pb.Value{Kind: &schema_pb.Value_ListValue{ListValue: &schema_pb.ListValue{Values: listValues}}}, valueIndex, nil
}

func toScalarValue(scalarType schema_pb.ScalarType, levels *ParquetLevels, values []parquet.Value, valueIndex int) (*schema_pb.Value, int, error) {
	value := values[valueIndex]
	if value.Column() != levels.startColumnIndex {
		return nil, valueIndex, nil
	}
	switch scalarType {
	case schema_pb.ScalarType_BOOL:
		return &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: value.Boolean()}}, valueIndex + 1, nil
	case schema_pb.ScalarType_INT32:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: value.Int32()}}, valueIndex + 1, nil
	case schema_pb.ScalarType_INT64:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: value.Int64()}}, valueIndex + 1, nil
	case schema_pb.ScalarType_FLOAT:
		return &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: value.Float()}}, valueIndex + 1, nil
	case schema_pb.ScalarType_DOUBLE:
		return &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: value.Double()}}, valueIndex + 1, nil
	case schema_pb.ScalarType_BYTES:
		// Handle nil byte arrays from parquet to prevent growslice panic
		byteData := value.ByteArray()
		if byteData == nil {
			byteData = []byte{} // Use empty slice instead of nil
		}
		return &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: byteData}}, valueIndex + 1, nil
	case schema_pb.ScalarType_STRING:
		// Handle nil byte arrays from parquet to prevent string conversion issues
		byteData := value.ByteArray()
		if byteData == nil {
			byteData = []byte{} // Use empty slice instead of nil
		}
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: string(byteData)}}, valueIndex + 1, nil
	// Parquet logical types - convert from their physical storage back to logical values
	case schema_pb.ScalarType_TIMESTAMP:
		// Stored as INT64, convert back to TimestampValue
		return &schema_pb.Value{
			Kind: &schema_pb.Value_TimestampValue{
				TimestampValue: &schema_pb.TimestampValue{
					TimestampMicros: value.Int64(),
					IsUtc:           true, // Default to UTC for compatibility
				},
			},
		}, valueIndex + 1, nil
	case schema_pb.ScalarType_DATE:
		// Stored as INT32, convert back to DateValue
		return &schema_pb.Value{
			Kind: &schema_pb.Value_DateValue{
				DateValue: &schema_pb.DateValue{
					DaysSinceEpoch: value.Int32(),
				},
			},
		}, valueIndex + 1, nil
	case schema_pb.ScalarType_DECIMAL:
		// Stored as FixedLenByteArray, convert back to DecimalValue
		fixedBytes := value.ByteArray() // FixedLenByteArray also uses ByteArray() method
		if fixedBytes == nil {
			fixedBytes = []byte{} // Use empty slice instead of nil
		}
		// Remove leading zeros to get the minimal representation
		trimmedBytes := bytes.TrimLeft(fixedBytes, "\x00")
		if len(trimmedBytes) == 0 {
			trimmedBytes = []byte{0} // Ensure we have at least one byte for zero
		}
		return &schema_pb.Value{
			Kind: &schema_pb.Value_DecimalValue{
				DecimalValue: &schema_pb.DecimalValue{
					Value:     trimmedBytes,
					Precision: 38, // Maximum precision supported by schema
					Scale:     18, // Maximum scale supported by schema
				},
			},
		}, valueIndex + 1, nil
	case schema_pb.ScalarType_TIME:
		// Stored as INT64, convert back to TimeValue
		return &schema_pb.Value{
			Kind: &schema_pb.Value_TimeValue{
				TimeValue: &schema_pb.TimeValue{
					TimeMicros: value.Int64(),
				},
			},
		}, valueIndex + 1, nil
	}
	return nil, valueIndex, fmt.Errorf("unsupported scalar type: %v", scalarType)
}
