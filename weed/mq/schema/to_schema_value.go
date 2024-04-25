package schema

import (
	"fmt"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func ToRecordValue(recordType *schema_pb.RecordType, row parquet.Row) (*schema_pb.RecordValue, error) {
	values := []parquet.Value(row)
	recordValue, _, _, err := toRecordValue(recordType, values, 0, 0)
	if err != nil {
		return nil, err
	}
	return recordValue.GetRecordValue(), nil
}

func ToValue(t *schema_pb.Type, values []parquet.Value, valueIndex, columnIndex int) (value *schema_pb.Value, endValueIndex, endColumnIndex int, err error) {
	switch t.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		value, err = toScalarValue(t.GetScalarType(), values, valueIndex, columnIndex)
		return value, valueIndex + 1, columnIndex + 1, err
	case *schema_pb.Type_ListType:
		return toListValue(t.GetListType(), values, valueIndex, columnIndex)
	case *schema_pb.Type_RecordType:
		return toRecordValue(t.GetRecordType(), values, valueIndex, columnIndex)
	}
	return nil, 0, 0, fmt.Errorf("unsupported type: %v", t)
}

func toRecordValue(recordType *schema_pb.RecordType, values []parquet.Value, valueIndex, columnIndex int) (*schema_pb.Value, int, int, error) {
	recordValue := schema_pb.RecordValue{Fields: make(map[string]*schema_pb.Value)}
	for _, field := range recordType.Fields {
		fieldValue, endValueIndex, endColumnIndex, err := ToValue(field.Type, values, valueIndex, columnIndex)
		if err != nil {
			return nil, 0, 0, err
		}
		columnIndex = endColumnIndex
		valueIndex = endValueIndex
		recordValue.Fields[field.Name] = fieldValue
	}
	return &schema_pb.Value{Kind: &schema_pb.Value_RecordValue{RecordValue: &recordValue}}, valueIndex, columnIndex, nil
}

func toListValue(listType *schema_pb.ListType, values []parquet.Value, valueIndex, columnIndex int) (listValue *schema_pb.Value, endValueIndex, endColumnIndex int, err error) {
	listValues := make([]*schema_pb.Value, 0)
	var value *schema_pb.Value
	for ;valueIndex < len(values); {
		if values[valueIndex].Column() != columnIndex {
			break
		}
		value, valueIndex, endColumnIndex, err = ToValue(listType.ElementType, values, valueIndex, columnIndex)
		if err != nil {
			return nil, 0,0, err
		}
		listValues = append(listValues, value)
	}
	return &schema_pb.Value{Kind: &schema_pb.Value_ListValue{ListValue: &schema_pb.ListValue{Values: listValues}}}, valueIndex, endColumnIndex, nil
}

func toScalarValue(scalarType schema_pb.ScalarType, values []parquet.Value, valueIndex, columnIndex int) (*schema_pb.Value, error) {
	value := values[valueIndex]
	if value.Column() != columnIndex {
		return nil, nil
	}
	switch scalarType {
	case schema_pb.ScalarType_BOOLEAN:
		return &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: value.Boolean()}}, nil
	case schema_pb.ScalarType_INTEGER:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: value.Int32()}}, nil
	case schema_pb.ScalarType_LONG:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: value.Int64()}},  nil
	case schema_pb.ScalarType_FLOAT:
		return &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: value.Float()}}, nil
	case schema_pb.ScalarType_DOUBLE:
		return &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: value.Double()}}, nil
	case schema_pb.ScalarType_BYTES:
		return &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: value.ByteArray()}}, nil
	case schema_pb.ScalarType_STRING:
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: string(value.ByteArray())}}, nil
	}
	return nil, fmt.Errorf("unsupported scalar type: %v", scalarType)
}
