package schema

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"sort"
)

var (
	TypeBoolean = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_BOOLEAN}}
	TypeInteger = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_INTEGER}}
	TypeLong    = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_LONG}}
	TypeFloat   = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_FLOAT}}
	TypeDouble  = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_DOUBLE}}
	TypeBytes   = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_BYTES}}
	TypeString  = &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{schema_pb.ScalarType_STRING}}
)

type RecordTypeBuilder struct {
	recordType *schema_pb.RecordType
}

func NewRecordTypeBuilder() *RecordTypeBuilder {
	return &RecordTypeBuilder{recordType: &schema_pb.RecordType{}}
}

func (rtb *RecordTypeBuilder) Build() *schema_pb.RecordType {
	// be consistent with parquet.node.go `func (g Group) Fields() []Field`
	sort.Slice(rtb.recordType.Fields, func(i, j int) bool {
		return rtb.recordType.Fields[i].Name < rtb.recordType.Fields[j].Name
	})
	return rtb.recordType
}

func (rtb *RecordTypeBuilder) addField(name string, scalarType *schema_pb.Type) *RecordTypeBuilder {
	rtb.recordType.Fields = append(rtb.recordType.Fields, &schema_pb.Field{
		Name: name,
		Type: scalarType,
	})
	return rtb
}

func (rtb *RecordTypeBuilder) AddBoolField(name string) *RecordTypeBuilder {
	return rtb.addField(name, TypeBoolean)
}
func (rtb *RecordTypeBuilder) AddIntegerField(name string) *RecordTypeBuilder {
	return rtb.addField(name, TypeInteger)
}
func (rtb *RecordTypeBuilder) AddLongField(name string) *RecordTypeBuilder {
	return rtb.addField(name, TypeLong)
}
func (rtb *RecordTypeBuilder) AddFloatField(name string) *RecordTypeBuilder {
	return rtb.addField(name, TypeFloat)
}
func (rtb *RecordTypeBuilder) AddDoubleField(name string) *RecordTypeBuilder {
	return rtb.addField(name, TypeDouble)
}
func (rtb *RecordTypeBuilder) AddBytesField(name string) *RecordTypeBuilder {
	return rtb.addField(name, TypeBytes)
}
func (rtb *RecordTypeBuilder) AddStringField(name string) *RecordTypeBuilder {
	return rtb.addField(name, TypeString)
}

func (rtb *RecordTypeBuilder) AddRecordField(name string, recordTypeBuilder *RecordTypeBuilder) *RecordTypeBuilder {
	rtb.recordType.Fields = append(rtb.recordType.Fields, &schema_pb.Field{
		Name: name,
		Type: &schema_pb.Type{Kind: &schema_pb.Type_RecordType{RecordType: recordTypeBuilder.recordType}},
	})
	return rtb
}

func (rtb *RecordTypeBuilder) AddListField(name string, elementType *schema_pb.Type) *RecordTypeBuilder {
	rtb.recordType.Fields = append(rtb.recordType.Fields, &schema_pb.Field{
		Name: name,
		Type: &schema_pb.Type{Kind: &schema_pb.Type_ListType{ListType: &schema_pb.ListType{ElementType: elementType}}},
	})
	return rtb
}
