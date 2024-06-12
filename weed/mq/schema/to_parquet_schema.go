package schema

import (
	"fmt"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func ToParquetSchema(topicName string, recordType *schema_pb.RecordType) (*parquet.Schema, error) {
	rootNode, err := toParquetFieldTypeRecord(recordType)
	if err != nil {
		return nil, fmt.Errorf("failed to convert record type to parquet schema: %v", err)
	}

	// Fields are sorted by name, so the value should be sorted also
	// the sorting is inside parquet.`func (g Group) Fields() []Field`
	return parquet.NewSchema(topicName, rootNode), nil
}

func toParquetFieldType(fieldType *schema_pb.Type) (dataType parquet.Node, err error) {
	switch fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		dataType, err = toParquetFieldTypeScalar(fieldType.GetScalarType())
		dataType = parquet.Optional(dataType)
	case *schema_pb.Type_RecordType:
		dataType, err = toParquetFieldTypeRecord(fieldType.GetRecordType())
		dataType = parquet.Optional(dataType)
	case *schema_pb.Type_ListType:
		dataType, err = toParquetFieldTypeList(fieldType.GetListType())
	default:
		return nil, fmt.Errorf("unknown field type: %T", fieldType.Kind)
	}

	return dataType, err
}

func toParquetFieldTypeList(listType *schema_pb.ListType) (parquet.Node, error) {
	elementType, err := toParquetFieldType(listType.ElementType)
	if err != nil {
		return nil, err
	}
	return parquet.Repeated(elementType), nil
}

func toParquetFieldTypeScalar(scalarType schema_pb.ScalarType) (parquet.Node, error) {
	switch scalarType {
	case schema_pb.ScalarType_BOOL:
		return parquet.Leaf(parquet.BooleanType), nil
	case schema_pb.ScalarType_INT32:
		return parquet.Leaf(parquet.Int32Type), nil
	case schema_pb.ScalarType_INT64:
		return parquet.Leaf(parquet.Int64Type), nil
	case schema_pb.ScalarType_FLOAT:
		return parquet.Leaf(parquet.FloatType), nil
	case schema_pb.ScalarType_DOUBLE:
		return parquet.Leaf(parquet.DoubleType), nil
	case schema_pb.ScalarType_BYTES:
		return parquet.Leaf(parquet.ByteArrayType), nil
	case schema_pb.ScalarType_STRING:
		return parquet.Leaf(parquet.ByteArrayType), nil
	default:
		return nil, fmt.Errorf("unknown scalar type: %v", scalarType)
	}
}
func toParquetFieldTypeRecord(recordType *schema_pb.RecordType) (parquet.Node, error) {
	recordNode := parquet.Group{}
	for _, field := range recordType.Fields {
		parquetFieldType, err := toParquetFieldType(field.Type)
		if err != nil {
			return nil, err
		}
		recordNode[field.Name] = parquetFieldType
	}
	return recordNode, nil
}
