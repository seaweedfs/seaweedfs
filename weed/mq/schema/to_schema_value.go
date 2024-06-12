package schema

import (
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
		return &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: value.ByteArray()}}, valueIndex + 1, nil
	case schema_pb.ScalarType_STRING:
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: string(value.ByteArray())}}, valueIndex + 1, nil
	}
	return nil, valueIndex, fmt.Errorf("unsupported scalar type: %v", scalarType)
}
