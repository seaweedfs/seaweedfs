package schema

import (
	"reflect"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func StructToSchema(instance any) *schema_pb.RecordType {
	myType := reflect.TypeOf(instance)
	if myType.Kind() != reflect.Struct {
		return nil
	}
	st := reflectTypeToSchemaType(myType)
	return st.GetRecordType()
}

// CreateCombinedRecordType creates a combined RecordType that includes fields from both key and value schemas
// Key fields are prefixed with "key_" to distinguish them from value fields
func CreateCombinedRecordType(keyRecordType *schema_pb.RecordType, valueRecordType *schema_pb.RecordType) *schema_pb.RecordType {
	var combinedFields []*schema_pb.Field

	// Add key fields with "key_" prefix
	if keyRecordType != nil {
		for _, field := range keyRecordType.Fields {
			keyField := &schema_pb.Field{
				Name:       "key_" + field.Name,
				FieldIndex: field.FieldIndex, // Will be reindexed later
				Type:       field.Type,
				IsRepeated: field.IsRepeated,
				IsRequired: field.IsRequired,
			}
			combinedFields = append(combinedFields, keyField)
		}
	}

	// Add value fields (no prefix)
	if valueRecordType != nil {
		for _, field := range valueRecordType.Fields {
			combinedFields = append(combinedFields, field)
		}
	}

	// Reindex all fields to have sequential indices
	for i, field := range combinedFields {
		field.FieldIndex = int32(i)
	}

	return &schema_pb.RecordType{
		Fields: combinedFields,
	}
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
