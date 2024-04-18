package schema

import (
	"fmt"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)


func AddRecordValue(rowBuilder *parquet.RowBuilder, fieldType *schema_pb.Type, fieldValue *schema_pb.Value) error {
	visitor := func(fieldType *schema_pb.Type, fieldValue *schema_pb.Value, index int) error {
		switch fieldType.Kind.(type) {
		case *schema_pb.Type_ScalarType:
			parquetValue, err := toParquetValue(fieldValue)
			if err != nil {
				return err
			}
			rowBuilder.Add(index, parquetValue)
		}
		return nil
	}
	return visitValue(fieldType, fieldValue, visitor)
}

// typeValueVisitor is a function that is called for each value in a schema_pb.Value
// Find the column index.
// intended to be used in RowBuilder.Add(columnIndex, value)
type typeValueVisitor func(fieldType *schema_pb.Type, fieldValue *schema_pb.Value, index int) error

func visitValue(fieldType *schema_pb.Type, fieldValue *schema_pb.Value, visitor typeValueVisitor) (err error) {
	_, err = doVisitValue(fieldType, fieldValue, 0, visitor)
	return
}

// endIndex is exclusive
// same logic as RowBuilder.configure in row_builder.go
func doVisitValue(fieldType *schema_pb.Type, fieldValue *schema_pb.Value, columnIndex int, visitor typeValueVisitor) (endIndex int, err error) {
	switch fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		return columnIndex+1, visitor(fieldType, fieldValue, columnIndex)
	case *schema_pb.Type_ListType:
		for _, value := range fieldValue.GetListValue().Values {
			err = visitor(fieldType, value, columnIndex)
			if err != nil {
				return
			}
		}
		return columnIndex+1, nil
	case *schema_pb.Type_RecordType:
		for _, field := range fieldType.GetRecordType().Fields {
			fieldValue, found := fieldValue.GetRecordValue().Fields[field.Name]
			if !found {
				// TODO check this if no such field found
				return columnIndex, nil
			}
			endIndex, err = doVisitValue(field.Type, fieldValue, columnIndex, visitor)
			if err != nil {
				return
			}
			columnIndex = endIndex
		}
		return
	}
	return
}

func toParquetValue(value *schema_pb.Value) (parquet.Value,  error) {
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
	default:
		return parquet.NullValue(), fmt.Errorf("unknown value type: %T", value.Kind)
	}
}
