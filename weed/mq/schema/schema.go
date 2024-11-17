package schema

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

type Schema struct {
	Namespace  string
	Name       string
	RevisionId uint32
	RecordType *schema_pb.RecordType
	fieldMap   map[string]*schema_pb.Field
}

func NewSchema(namespace string, name string, recordType *schema_pb.RecordType) *Schema {
	fieldMap := make(map[string]*schema_pb.Field)
	for _, field := range recordType.Fields {
		fieldMap[field.Name] = field
	}
	return &Schema{
		Namespace:  namespace,
		Name:       name,
		RecordType: recordType,
		fieldMap:   fieldMap,
	}
}

func (s *Schema) GetField(name string) (*schema_pb.Field, bool) {
	field, ok := s.fieldMap[name]
	return field, ok
}

func TypeToString(t *schema_pb.Type) string {
	switch t.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		switch t.GetScalarType() {
		case schema_pb.ScalarType_BOOL:
			return "bool"
		case schema_pb.ScalarType_INT32:
			return "int32"
		case schema_pb.ScalarType_INT64:
			return "int64"
		case schema_pb.ScalarType_FLOAT:
			return "float"
		case schema_pb.ScalarType_DOUBLE:
			return "double"
		case schema_pb.ScalarType_BYTES:
			return "bytes"
		case schema_pb.ScalarType_STRING:
			return "string"
		}
	case *schema_pb.Type_ListType:
		return "list"
	case *schema_pb.Type_RecordType:
		return "record"
	}
	return "unknown"
}
