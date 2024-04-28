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

func (rtb *RecordTypeBuilder) SetField(name string, scalarType *schema_pb.Type) *RecordTypeBuilder {
	rtb.recordType.Fields = append(rtb.recordType.Fields, &schema_pb.Field{
		Name: name,
		Type: scalarType,
	})
	return rtb
}

func (rtb *RecordTypeBuilder) SetBoolField(name string) *RecordTypeBuilder {
	return rtb.SetField(name, TypeBoolean)
}
func (rtb *RecordTypeBuilder) SetIntegerField(name string) *RecordTypeBuilder {
	return rtb.SetField(name, TypeInteger)
}
func (rtb *RecordTypeBuilder) SetLongField(name string) *RecordTypeBuilder {
	return rtb.SetField(name, TypeLong)
}
func (rtb *RecordTypeBuilder) SetFloatField(name string) *RecordTypeBuilder {
	return rtb.SetField(name, TypeFloat)
}
func (rtb *RecordTypeBuilder) SetDoubleField(name string) *RecordTypeBuilder {
	return rtb.SetField(name, TypeDouble)
}
func (rtb *RecordTypeBuilder) SetBytesField(name string) *RecordTypeBuilder {
	return rtb.SetField(name, TypeBytes)
}
func (rtb *RecordTypeBuilder) SetStringField(name string) *RecordTypeBuilder {
	return rtb.SetField(name, TypeString)
}

func (rtb *RecordTypeBuilder) SetRecordField(name string, recordTypeBuilder *RecordTypeBuilder) *RecordTypeBuilder {
	rtb.recordType.Fields = append(rtb.recordType.Fields, &schema_pb.Field{
		Name: name,
		Type: &schema_pb.Type{Kind: &schema_pb.Type_RecordType{RecordType: recordTypeBuilder.Build()}},
	})
	return rtb
}

func (rtb *RecordTypeBuilder) SetListField(name string, elementType *schema_pb.Type) *RecordTypeBuilder {
	rtb.recordType.Fields = append(rtb.recordType.Fields, &schema_pb.Field{
		Name: name,
		Type: &schema_pb.Type{Kind: &schema_pb.Type_ListType{ListType: &schema_pb.ListType{ElementType: elementType}}},
	})
	return rtb
}

func List(elementType *schema_pb.Type) *schema_pb.Type {
	return &schema_pb.Type{Kind: &schema_pb.Type_ListType{ListType: &schema_pb.ListType{ElementType: elementType}}}
}
