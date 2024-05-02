package schema

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"reflect"
)

func StructToSchema(instance any) *schema_pb.RecordType {
	myType := reflect.TypeOf(instance)
	if myType.Kind() != reflect.Struct {
		return nil
	}
	st := reflectTypeToSchemaType(myType)
	return st.GetRecordType()
}

func reflectTypeToSchemaType(t reflect.Type) *schema_pb.Type {
	switch t.Kind() {
	case reflect.Bool:
		return TypeBoolean
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return TypeInt32
	case reflect.Int64:
		return TypeInt64
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
	case reflect.Struct:
		recordType := &schema_pb.RecordType{}
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			fieldType := field.Type
			fieldName := field.Name
			schemaField := reflectTypeToSchemaType(fieldType)
			if schemaField == nil {
				return nil
			}
			recordType.Fields = append(recordType.Fields, &schema_pb.Field{
				Name: fieldName,
				Type: schemaField,
			})
		}
		return &schema_pb.Type{
			Kind: &schema_pb.Type_RecordType{
				RecordType: recordType,
			},
		}
	}
	return nil
}
