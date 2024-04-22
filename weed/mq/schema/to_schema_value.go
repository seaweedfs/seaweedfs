package schema

import (
	"fmt"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func ToRecordValue(recordType *schema_pb.RecordType, row parquet.Row) (*schema_pb.RecordValue, error) {
	values := []parquet.Value(row)
	recordValue, _, err := toRecordValue(recordType, values, 0)
	if err != nil {
		return nil, err
	}
	return recordValue.GetRecordValue(), nil
}

func ToValue(t *schema_pb.Type, values []parquet.Value, columnIndex int) (value *schema_pb.Value, endIndex int, err error) {
	switch t.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		return toScalarValue(t.GetScalarType(), values, columnIndex)
	case *schema_pb.Type_ListType:
		return toListValue(t.GetListType(), values, columnIndex)
	case *schema_pb.Type_RecordType:
		return toRecordValue(t.GetRecordType(), values, columnIndex)
	}
	return nil, 0, fmt.Errorf("unsupported type: %v", t)
}

func toRecordValue(recordType *schema_pb.RecordType, values []parquet.Value, columnIndex int) (*schema_pb.Value, int, error) {
	recordValue := schema_pb.RecordValue{Fields: make(map[string]*schema_pb.Value)}
	for _, field := range recordType.Fields {
		fieldValue, endIndex, err := ToValue(field.Type, values, columnIndex)
		if err != nil {
			return nil, 0, err
		}
		if endIndex == columnIndex {
			continue
		}
		columnIndex = endIndex
		recordValue.Fields[field.Name] = fieldValue
	}
	return &schema_pb.Value{Kind: &schema_pb.Value_RecordValue{RecordValue: &recordValue}}, columnIndex, nil
}

func toListValue(listType *schema_pb.ListType, values []parquet.Value, index int) (listValue *schema_pb.Value, endIndex int, err error) {
	listValues := make([]*schema_pb.Value, 0)
	var value *schema_pb.Value
	for i := index; i < len(values); {
		value, endIndex, err = ToValue(listType.ElementType, values, i)
		if err != nil {
			return nil, 0, err
		}
		if endIndex == i {
			break
		}
		listValues = append(listValues, value)
		i = endIndex
	}
	return &schema_pb.Value{Kind: &schema_pb.Value_ListValue{ListValue: &schema_pb.ListValue{Values: listValues}}}, endIndex, nil
}

func toScalarValue(scalarType schema_pb.ScalarType, values []parquet.Value, columnIndex int) (*schema_pb.Value, int, error) {
	value := values[columnIndex]
	if value.Column() != columnIndex {
		return nil, columnIndex, nil
	}
	switch scalarType {
	case schema_pb.ScalarType_BOOLEAN:
		return &schema_pb.Value{Kind: &schema_pb.Value_BoolValue{BoolValue: value.Boolean()}}, columnIndex + 1, nil
	case schema_pb.ScalarType_INTEGER:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: value.Int32()}}, columnIndex + 1, nil
	case schema_pb.ScalarType_LONG:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: value.Int64()}}, columnIndex + 1, nil
	case schema_pb.ScalarType_FLOAT:
		return &schema_pb.Value{Kind: &schema_pb.Value_FloatValue{FloatValue: value.Float()}}, columnIndex + 1, nil
	case schema_pb.ScalarType_DOUBLE:
		return &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: value.Double()}}, columnIndex + 1, nil
	case schema_pb.ScalarType_BYTES:
		return &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: value.ByteArray()}}, columnIndex + 1, nil
	case schema_pb.ScalarType_STRING:
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: string(value.ByteArray())}}, columnIndex + 1, nil
	}
	return nil, columnIndex, fmt.Errorf("unsupported scalar type: %v", scalarType)
}
