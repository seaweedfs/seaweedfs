package schema

import (
	"fmt"

	parquet "github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func ToParquetSchema(topicName string, recordType *schema_pb.RecordType) (*parquet.Schema, error) {
	rootNode, err := toParquetFieldTypeRecord(recordType)
	if err != nil {
		return nil, fmt.Errorf("failed to convert record type to parquet schema: %w", err)
	}

	// Fields are sorted by name, so the value should be sorted also
	// the sorting is inside parquet.`func (g Group) Fields() []Field`
	return parquet.NewSchema(topicName, rootNode), nil
}

func toParquetFieldType(fieldType *schema_pb.Type) (dataType parquet.Node, err error) {
	// This is the old function - now defaults to Optional for backward compatibility
	return toParquetFieldTypeWithRequirement(fieldType, false)
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
	// Parquet logical types - map to their physical storage types
	case schema_pb.ScalarType_TIMESTAMP:
		// Stored as INT64 (microseconds since Unix epoch)
		return parquet.Leaf(parquet.Int64Type), nil
	case schema_pb.ScalarType_DATE:
		// Stored as INT32 (days since Unix epoch)
		return parquet.Leaf(parquet.Int32Type), nil
	case schema_pb.ScalarType_DECIMAL:
		// Use maximum precision/scale to accommodate any decimal value
		// Per Parquet spec: precision ≤9→INT32, ≤18→INT64, >18→FixedLenByteArray
		// Using precision=38 (max for most systems), scale=18 for flexibility
		// Individual values can have smaller precision/scale, but schema supports maximum
		return parquet.Decimal(18, 38, parquet.FixedLenByteArrayType(16)), nil
	case schema_pb.ScalarType_TIME:
		// Stored as INT64 (microseconds since midnight)
		return parquet.Leaf(parquet.Int64Type), nil
	default:
		return nil, fmt.Errorf("unknown scalar type: %v", scalarType)
	}
}
func toParquetFieldTypeRecord(recordType *schema_pb.RecordType) (parquet.Node, error) {
	recordNode := parquet.Group{}
	for _, field := range recordType.Fields {
		parquetFieldType, err := toParquetFieldTypeWithRequirement(field.Type, field.IsRequired)
		if err != nil {
			return nil, err
		}
		recordNode[field.Name] = parquetFieldType
	}
	return recordNode, nil
}

// toParquetFieldTypeWithRequirement creates parquet field type respecting required/optional constraints
func toParquetFieldTypeWithRequirement(fieldType *schema_pb.Type, isRequired bool) (dataType parquet.Node, err error) {
	switch fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		dataType, err = toParquetFieldTypeScalar(fieldType.GetScalarType())
		if err != nil {
			return nil, err
		}
		if isRequired {
			// Required fields are NOT wrapped in Optional
			return dataType, nil
		} else {
			// Optional fields are wrapped in Optional
			return parquet.Optional(dataType), nil
		}
	case *schema_pb.Type_RecordType:
		dataType, err = toParquetFieldTypeRecord(fieldType.GetRecordType())
		if err != nil {
			return nil, err
		}
		if isRequired {
			return dataType, nil
		} else {
			return parquet.Optional(dataType), nil
		}
	case *schema_pb.Type_ListType:
		dataType, err = toParquetFieldTypeList(fieldType.GetListType())
		if err != nil {
			return nil, err
		}
		// Lists are typically optional by nature
		return dataType, nil
	default:
		return nil, fmt.Errorf("unknown field type: %T", fieldType.Kind)
	}
}
