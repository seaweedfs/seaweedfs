package schema

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"reflect"
)

func StructToSchema(instance any) *RecordTypeBuilder {
	rtb := NewRecordTypeBuilder()
	myType := reflect.TypeOf(instance)
	for i := 0; i < myType.NumField(); i++ {
		field := myType.Field(i)
		fieldType := field.Type
		fieldName := field.Name
		schemaField := reflectTypeToSchemaType(fieldType)
		if schemaField == nil {
			continue
		}
		rtb.setField(fieldName, schemaField)
	}
	return rtb
}

func reflectTypeToSchemaType(t reflect.Type) *schema_pb.Type {
	switch t.Kind() {
	case reflect.Bool:
		return TypeBoolean
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return TypeInteger
	case reflect.Int64:
		return TypeLong
	case reflect.Float32:
		return TypeFloat
	case reflect.Float64:
		return TypeDouble
	case reflect.String:
		return TypeString
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Uint8:
			return TypeBytes
		default:
			if st := reflectTypeToSchemaType(t.Elem()); st != nil {
				return &schema_pb.Type{
					Kind: &schema_pb.Type_ListType{
						ListType: &schema_pb.ListType{
							ElementType: st,
						},
					},
				}
			}
		}
	}
	return nil
}
