package schema

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

type ParquetLevels struct {
	startColumnIndex int
	endColumnIndex   int
	definitionDepth  int
	levels           map[string]*ParquetLevels
}

func ToParquetLevels(recordType *schema_pb.RecordType) (*ParquetLevels, error) {
	return toRecordTypeLevels(recordType, 0, 0)
}

func toFieldTypeLevels(fieldType *schema_pb.Type, startColumnIndex, definitionDepth int) (*ParquetLevels, error) {
	switch fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		return toFieldTypeScalarLevels(fieldType.GetScalarType(), startColumnIndex, definitionDepth)
	case *schema_pb.Type_RecordType:
		return toRecordTypeLevels(fieldType.GetRecordType(), startColumnIndex, definitionDepth)
	case *schema_pb.Type_ListType:
		return toFieldTypeListLevels(fieldType.GetListType(), startColumnIndex, definitionDepth)
	}
	return nil, fmt.Errorf("unknown field type: %T", fieldType.Kind)
}

func toFieldTypeListLevels(listType *schema_pb.ListType, startColumnIndex, definitionDepth int) (*ParquetLevels, error) {
	return toFieldTypeLevels(listType.ElementType, startColumnIndex, definitionDepth)
}

func toFieldTypeScalarLevels(scalarType schema_pb.ScalarType, startColumnIndex, definitionDepth int) (*ParquetLevels, error) {
	return &ParquetLevels{
		startColumnIndex: startColumnIndex,
		endColumnIndex:   startColumnIndex + 1,
		definitionDepth:  definitionDepth,
	}, nil
}
func toRecordTypeLevels(recordType *schema_pb.RecordType, startColumnIndex, definitionDepth int) (*ParquetLevels, error) {
	recordTypeLevels := &ParquetLevels{
		startColumnIndex: startColumnIndex,
		definitionDepth:  definitionDepth,
		levels:           make(map[string]*ParquetLevels),
	}
	for _, field := range recordType.Fields {
		fieldTypeLevels, err := toFieldTypeLevels(field.Type, startColumnIndex, definitionDepth+1)
		if err != nil {
			return nil, err
		}
		recordTypeLevels.levels[field.Name] = fieldTypeLevels
		startColumnIndex = fieldTypeLevels.endColumnIndex
	}
	recordTypeLevels.endColumnIndex = startColumnIndex
	return recordTypeLevels, nil
}
