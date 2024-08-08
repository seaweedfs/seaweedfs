package schema

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

type Schema struct {
	RecordType *schema_pb.RecordType
	fieldMap   map[string]*schema_pb.Field
}

func NewSchema(recordType *schema_pb.RecordType) (*Schema, error) {
	fieldMap := make(map[string]*schema_pb.Field)
	for _, field := range recordType.Fields {
		fieldMap[field.Name] = field
	}
	return &Schema{
		RecordType: recordType,
		fieldMap:   fieldMap,
	}, nil
}

func (s *Schema) GetField(name string) (*schema_pb.Field, bool) {
	field, ok := s.fieldMap[name]
	return field, ok
}
