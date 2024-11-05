package schema

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"sort"
)

var (
	TypeBoolean = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_BOOL}}
	TypeInt32   = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_INT32}}
	TypeInt64   = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_INT64}}
	TypeFloat   = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_FLOAT}}
	TypeDouble  = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_DOUBLE}}
	TypeBytes   = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_BYTES}}
	TypeString  = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_STRING}}
)

type RecordTypeBuilder struct {
	recordType *schema_pb.RecordType
}

// RecordTypeBegin creates a new RecordTypeBuilder, it should be followed by a series of WithField methods and RecordTypeEnd
func RecordTypeBegin() *RecordTypeBuilder {
	return &RecordTypeBuilder{recordType: &schema_pb.RecordType{}}
}

// RecordTypeEnd finishes the building of a RecordValue
func (rtb *RecordTypeBuilder) RecordTypeEnd() *schema_pb.RecordType {
	// be consistent with parquet.node.go `func (g Group) Fields() []Field`
	sort.Slice(rtb.recordType.Fields, func(i, j int) bool {
		return rtb.recordType.Fields[i].Name < rtb.recordType.Fields[j].Name
	})
	return rtb.recordType
}

// NewRecordTypeBuilder creates a new RecordTypeBuilder from an existing RecordType, it should be followed by a series of WithField methods and RecordTypeEnd
func NewRecordTypeBuilder(recordType *schema_pb.RecordType) (rtb *RecordTypeBuilder) {
	return &RecordTypeBuilder{recordType: recordType}
}

func (rtb *RecordTypeBuilder) WithField(name string, scalarType *schema_pb.Type) *RecordTypeBuilder {
	rtb.recordType.Fields = append(rtb.recordType.Fields, &schema_pb.Field{
		Name: name,
		Type: scalarType,
	})
	return rtb
}

func (rtb *RecordTypeBuilder) WithRecordField(name string, recordType *schema_pb.RecordType) *RecordTypeBuilder {
	rtb.recordType.Fields = append(rtb.recordType.Fields, &schema_pb.Field{
		Name: name,
		Type: &schema_pb.Type{Kind: &schema_pb.Type_RecordType{RecordType: recordType}},
	})
	return rtb
}

func ListOf(elementType *schema_pb.Type) *schema_pb.Type {
	return &schema_pb.Type{Kind: &schema_pb.Type_ListType{ListType: &schema_pb.ListType{ElementType: elementType}}}
}
