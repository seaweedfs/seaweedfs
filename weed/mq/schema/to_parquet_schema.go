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

	return parquet.NewSchema(topicName, rootNode), nil
}

func toParquetFieldType(field *schema_pb.Field) (parquet.Node, error) {
	var (
		dataType parquet.Node
		err      error
	)
	switch field.Type.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		dataType, err = toParquetFieldTypeScalar(field.Type.GetScalarType())
	case *schema_pb.Type_RecordType:
		dataType, err = toParquetFieldTypeRecord(field.Type.GetRecordType())
	default:
		return nil, fmt.Errorf("unknown field type: %T", field.Type.Kind)
	}

	return dataType, err
}

func toParquetFieldTypeScalar(scalarType schema_pb.ScalarType) (parquet.Node, error) {
	switch scalarType {
	case schema_pb.ScalarType_BOOLEAN:
		return parquet.Leaf(parquet.BooleanType), nil
	case schema_pb.ScalarType_INTEGER:
		return parquet.Leaf(parquet.Int32Type), nil
	case schema_pb.ScalarType_LONG:
		return parquet.Leaf(parquet.Int64Type), nil
	case schema_pb.ScalarType_FLOAT:
		return parquet.Leaf(parquet.FloatType), nil
	case schema_pb.ScalarType_DOUBLE:
		return parquet.Leaf(parquet.DoubleType), nil
	case schema_pb.ScalarType_BYTES:
		return parquet.Leaf(parquet.ByteArrayType), nil
	case schema_pb.ScalarType_STRING:
		return parquet.String(), nil
	default:
		return nil, fmt.Errorf("unknown scalar type: %v", scalarType)
	}
}
func toParquetFieldTypeRecord(recordType *schema_pb.RecordType) (parquet.Node, error) {
	recordNode := parquet.Group{}
	for _, field := range recordType.Fields {
		parquetFieldType, err := toParquetFieldType(field)
		if err != nil {
			return nil, err
		}
		recordNode[field.Name] = parquetFieldType
	}
	return recordNode, nil
}
