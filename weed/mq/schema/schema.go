package schema

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

type Schema struct {
	RecordType *schema_pb.RecordType
	indexedFields []*schema_pb.Field
}

func NewSchema(recordType *schema_pb.RecordType) (*Schema, error) {
	var indexedFields []*schema_pb.Field
	var largestIndex int32
	for _, field := range recordType.Fields {
		if field.Index > largestIndex {
			largestIndex = field.Index
		}
		if field.Index < 0 {
			return nil, fmt.Errorf("field %s index %d is negative", field.Name, field.Index)
		}
	}
	indexedFields = make([]*schema_pb.Field, largestIndex+1)
	for _, field := range recordType.Fields {
		indexedFields[field.Index] = field
	}
	return &Schema{
		RecordType: recordType,
		indexedFields: indexedFields,
	}, nil
}
