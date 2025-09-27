package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// AvroDecoder handles Avro schema decoding and conversion to SeaweedMQ format
type AvroDecoder struct {
	codec *goavro.Codec
}

// NewAvroDecoder creates a new Avro decoder from a schema string
func NewAvroDecoder(schemaStr string) (*AvroDecoder, error) {
	codec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro codec: %w", err)
	}

	return &AvroDecoder{
		codec: codec,
	}, nil
}

// Decode decodes Avro binary data to a Go map
func (ad *AvroDecoder) Decode(data []byte) (map[string]interface{}, error) {
	native, _, err := ad.codec.NativeFromBinary(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro data: %w", err)
	}

	// Convert to map[string]interface{} for easier processing
	result, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected Avro record, got %T", native)
	}

	return result, nil
}

// DecodeToRecordValue decodes Avro data directly to SeaweedMQ RecordValue
func (ad *AvroDecoder) DecodeToRecordValue(data []byte) (*schema_pb.RecordValue, error) {
	nativeMap, err := ad.Decode(data)
	if err != nil {
		return nil, err
	}

	return MapToRecordValue(nativeMap), nil
}

// InferRecordType infers a SeaweedMQ RecordType from an Avro schema
func (ad *AvroDecoder) InferRecordType() (*schema_pb.RecordType, error) {
	schema := ad.codec.Schema()
	return avroSchemaToRecordType(schema)
}

// MapToRecordValue converts a Go map to SeaweedMQ RecordValue
func MapToRecordValue(m map[string]interface{}) *schema_pb.RecordValue {
	fields := make(map[string]*schema_pb.Value)

	for key, value := range m {
		fields[key] = goValueToSchemaValue(value)
	}

	return &schema_pb.RecordValue{
		Fields: fields,
	}
}

// goValueToSchemaValue converts a Go value to a SeaweedMQ Value
func goValueToSchemaValue(value interface{}) *schema_pb.Value {
	if value == nil {
		// For null values, use an empty string as default
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: ""},
		}
	}

	switch v := value.(type) {
	case bool:
		return &schema_pb.Value{
			Kind: &schema_pb.Value_BoolValue{BoolValue: v},
		}
	case int32:
		return &schema_pb.Value{
			Kind: &schema_pb.Value_Int32Value{Int32Value: v},
		}
	case int64:
		return &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: v},
		}
	case int:
		return &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: int64(v)},
		}
	case float32:
		return &schema_pb.Value{
			Kind: &schema_pb.Value_FloatValue{FloatValue: v},
		}
	case float64:
		return &schema_pb.Value{
			Kind: &schema_pb.Value_DoubleValue{DoubleValue: v},
		}
	case string:
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: v},
		}
	case []byte:
		return &schema_pb.Value{
			Kind: &schema_pb.Value_BytesValue{BytesValue: v},
		}
	case time.Time:
		return &schema_pb.Value{
			Kind: &schema_pb.Value_TimestampValue{
				TimestampValue: &schema_pb.TimestampValue{
					TimestampMicros: v.UnixMicro(),
					IsUtc:           true,
				},
			},
		}
	case []interface{}:
		// Handle arrays
		listValues := make([]*schema_pb.Value, len(v))
		for i, item := range v {
			listValues[i] = goValueToSchemaValue(item)
		}
		return &schema_pb.Value{
			Kind: &schema_pb.Value_ListValue{
				ListValue: &schema_pb.ListValue{
					Values: listValues,
				},
			},
		}
	case map[string]interface{}:
		// Check if this is an Avro union type (single key-value pair with type name as key)
		// Union types have keys that are typically Avro type names like "int", "string", etc.
		// Regular nested records would have meaningful field names like "inner", "name", etc.
		if len(v) == 1 {
			for unionType, unionValue := range v {
				// Handle common Avro union type patterns (only if key looks like a type name)
				switch unionType {
				case "int":
					if intVal, ok := unionValue.(int32); ok {
						// Store union as a record with the union type as field name
						// This preserves the union information for re-encoding
						return &schema_pb.Value{
							Kind: &schema_pb.Value_RecordValue{
								RecordValue: &schema_pb.RecordValue{
									Fields: map[string]*schema_pb.Value{
										"int": {
											Kind: &schema_pb.Value_Int32Value{Int32Value: intVal},
										},
									},
								},
							},
						}
					}
				case "long":
					if longVal, ok := unionValue.(int64); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_RecordValue{
								RecordValue: &schema_pb.RecordValue{
									Fields: map[string]*schema_pb.Value{
										"long": {
											Kind: &schema_pb.Value_Int64Value{Int64Value: longVal},
										},
									},
								},
							},
						}
					}
				case "float":
					if floatVal, ok := unionValue.(float32); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_RecordValue{
								RecordValue: &schema_pb.RecordValue{
									Fields: map[string]*schema_pb.Value{
										"float": {
											Kind: &schema_pb.Value_FloatValue{FloatValue: floatVal},
										},
									},
								},
							},
						}
					}
				case "double":
					if doubleVal, ok := unionValue.(float64); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_RecordValue{
								RecordValue: &schema_pb.RecordValue{
									Fields: map[string]*schema_pb.Value{
										"double": {
											Kind: &schema_pb.Value_DoubleValue{DoubleValue: doubleVal},
										},
									},
								},
							},
						}
					}
				case "string":
					if strVal, ok := unionValue.(string); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_RecordValue{
								RecordValue: &schema_pb.RecordValue{
									Fields: map[string]*schema_pb.Value{
										"string": {
											Kind: &schema_pb.Value_StringValue{StringValue: strVal},
										},
									},
								},
							},
						}
					}
				case "boolean":
					if boolVal, ok := unionValue.(bool); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_RecordValue{
								RecordValue: &schema_pb.RecordValue{
									Fields: map[string]*schema_pb.Value{
										"boolean": {
											Kind: &schema_pb.Value_BoolValue{BoolValue: boolVal},
										},
									},
								},
							},
						}
					}
				}
				// If it's not a recognized union type, fall through to treat as nested record
			}
		}

		// Handle nested records (both single-field and multi-field maps)
		fields := make(map[string]*schema_pb.Value)
		for key, val := range v {
			fields[key] = goValueToSchemaValue(val)
		}
		return &schema_pb.Value{
			Kind: &schema_pb.Value_RecordValue{
				RecordValue: &schema_pb.RecordValue{
					Fields: fields,
				},
			},
		}
	default:
		// Handle other types by converting to string
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{
				StringValue: fmt.Sprintf("%v", v),
			},
		}
	}
}

// avroSchemaToRecordType converts an Avro schema to SeaweedMQ RecordType
func avroSchemaToRecordType(schemaStr string) (*schema_pb.RecordType, error) {
	// Validate the Avro schema by creating a codec (this ensures it's valid)
	_, err := goavro.NewCodec(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Avro schema: %w", err)
	}

	// Parse the schema JSON to extract field definitions
	var avroSchema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaStr), &avroSchema); err != nil {
		return nil, fmt.Errorf("failed to parse Avro schema JSON: %w", err)
	}

	// Extract fields from the Avro schema
	fields, err := extractAvroFields(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to extract Avro fields: %w", err)
	}

	return &schema_pb.RecordType{
		Fields: fields,
	}, nil
}

// extractAvroFields extracts field definitions from parsed Avro schema JSON
func extractAvroFields(avroSchema map[string]interface{}) ([]*schema_pb.Field, error) {
	// Check if this is a record type
	schemaType, ok := avroSchema["type"].(string)
	if !ok || schemaType != "record" {
		return nil, fmt.Errorf("expected record type, got %v", schemaType)
	}

	// Extract fields array
	fieldsInterface, ok := avroSchema["fields"]
	if !ok {
		return nil, fmt.Errorf("no fields found in Avro record schema")
	}

	fieldsArray, ok := fieldsInterface.([]interface{})
	if !ok {
		return nil, fmt.Errorf("fields must be an array")
	}

	// Convert each Avro field to SeaweedMQ field
	fields := make([]*schema_pb.Field, 0, len(fieldsArray))
	for i, fieldInterface := range fieldsArray {
		fieldMap, ok := fieldInterface.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("field %d is not a valid object", i)
		}

		field, err := convertAvroFieldToSeaweedMQ(fieldMap, int32(i))
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %d: %w", i, err)
		}

		fields = append(fields, field)
	}

	return fields, nil
}

// convertAvroFieldToSeaweedMQ converts a single Avro field to SeaweedMQ Field
func convertAvroFieldToSeaweedMQ(avroField map[string]interface{}, fieldIndex int32) (*schema_pb.Field, error) {
	// Extract field name
	name, ok := avroField["name"].(string)
	if !ok {
		return nil, fmt.Errorf("field name is required")
	}

	// Extract field type and check if it's an array
	fieldType, isRepeated, err := convertAvroTypeToSeaweedMQWithRepeated(avroField["type"])
	if err != nil {
		return nil, fmt.Errorf("failed to convert field type for %s: %w", name, err)
	}

	// Check if field has a default value (indicates it's optional)
	_, hasDefault := avroField["default"]
	isRequired := !hasDefault

	return &schema_pb.Field{
		Name:       name,
		FieldIndex: fieldIndex,
		Type:       fieldType,
		IsRequired: isRequired,
		IsRepeated: isRepeated,
	}, nil
}

// convertAvroTypeToSeaweedMQ converts Avro type to SeaweedMQ Type
func convertAvroTypeToSeaweedMQ(avroType interface{}) (*schema_pb.Type, error) {
	fieldType, _, err := convertAvroTypeToSeaweedMQWithRepeated(avroType)
	return fieldType, err
}

// convertAvroTypeToSeaweedMQWithRepeated converts Avro type to SeaweedMQ Type and returns if it's repeated
func convertAvroTypeToSeaweedMQWithRepeated(avroType interface{}) (*schema_pb.Type, bool, error) {
	switch t := avroType.(type) {
	case string:
		// Simple type
		fieldType, err := convertAvroSimpleType(t)
		return fieldType, false, err

	case map[string]interface{}:
		// Complex type (record, enum, array, map, fixed)
		return convertAvroComplexTypeWithRepeated(t)

	case []interface{}:
		// Union type
		fieldType, err := convertAvroUnionType(t)
		return fieldType, false, err

	default:
		return nil, false, fmt.Errorf("unsupported Avro type: %T", avroType)
	}
}

// convertAvroSimpleType converts simple Avro types to SeaweedMQ types
func convertAvroSimpleType(avroType string) (*schema_pb.Type, error) {
	switch avroType {
	case "null":
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_BYTES, // Use bytes for null
			},
		}, nil
	case "boolean":
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_BOOL,
			},
		}, nil
	case "int":
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT32,
			},
		}, nil
	case "long":
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT64,
			},
		}, nil
	case "float":
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_FLOAT,
			},
		}, nil
	case "double":
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_DOUBLE,
			},
		}, nil
	case "bytes":
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_BYTES,
			},
		}, nil
	case "string":
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_STRING,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported simple Avro type: %s", avroType)
	}
}

// convertAvroComplexType converts complex Avro types to SeaweedMQ types
func convertAvroComplexType(avroType map[string]interface{}) (*schema_pb.Type, error) {
	fieldType, _, err := convertAvroComplexTypeWithRepeated(avroType)
	return fieldType, err
}

// convertAvroComplexTypeWithRepeated converts complex Avro types to SeaweedMQ types and returns if it's repeated
func convertAvroComplexTypeWithRepeated(avroType map[string]interface{}) (*schema_pb.Type, bool, error) {
	typeStr, ok := avroType["type"].(string)
	if !ok {
		return nil, false, fmt.Errorf("complex type must have a type field")
	}

	// Handle logical types - they are based on underlying primitive types
	if _, hasLogicalType := avroType["logicalType"]; hasLogicalType {
		// For logical types, use the underlying primitive type
		return convertAvroSimpleTypeWithLogical(typeStr, avroType)
	}

	switch typeStr {
	case "record":
		// Nested record type
		fields, err := extractAvroFields(avroType)
		if err != nil {
			return nil, false, fmt.Errorf("failed to extract nested record fields: %w", err)
		}
		return &schema_pb.Type{
			Kind: &schema_pb.Type_RecordType{
				RecordType: &schema_pb.RecordType{
					Fields: fields,
				},
			},
		}, false, nil

	case "enum":
		// Enum type - treat as string for now
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_STRING,
			},
		}, false, nil

	case "array":
		// Array type
		itemsType, err := convertAvroTypeToSeaweedMQ(avroType["items"])
		if err != nil {
			return nil, false, fmt.Errorf("failed to convert array items type: %w", err)
		}
		// For arrays, we return the item type and set IsRepeated=true
		return itemsType, true, nil

	case "map":
		// Map type - treat as record with dynamic fields
		return &schema_pb.Type{
			Kind: &schema_pb.Type_RecordType{
				RecordType: &schema_pb.RecordType{
					Fields: []*schema_pb.Field{}, // Dynamic fields
				},
			},
		}, false, nil

	case "fixed":
		// Fixed-length bytes
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_BYTES,
			},
		}, false, nil

	default:
		return nil, false, fmt.Errorf("unsupported complex Avro type: %s", typeStr)
	}
}

// convertAvroSimpleTypeWithLogical handles logical types based on their underlying primitive types
func convertAvroSimpleTypeWithLogical(primitiveType string, avroType map[string]interface{}) (*schema_pb.Type, bool, error) {
	logicalType, _ := avroType["logicalType"].(string)

	// Map logical types to appropriate SeaweedMQ types
	switch logicalType {
	case "decimal":
		// Decimal logical type - use bytes for precision
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_BYTES,
			},
		}, false, nil
	case "uuid":
		// UUID logical type - use string
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_STRING,
			},
		}, false, nil
	case "date":
		// Date logical type (int) - use int32
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT32,
			},
		}, false, nil
	case "time-millis":
		// Time in milliseconds (int) - use int32
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT32,
			},
		}, false, nil
	case "time-micros":
		// Time in microseconds (long) - use int64
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT64,
			},
		}, false, nil
	case "timestamp-millis":
		// Timestamp in milliseconds (long) - use int64
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT64,
			},
		}, false, nil
	case "timestamp-micros":
		// Timestamp in microseconds (long) - use int64
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT64,
			},
		}, false, nil
	default:
		// For unknown logical types, fall back to the underlying primitive type
		fieldType, err := convertAvroSimpleType(primitiveType)
		return fieldType, false, err
	}
}

// convertAvroUnionType converts Avro union types to SeaweedMQ types
func convertAvroUnionType(unionTypes []interface{}) (*schema_pb.Type, error) {
	// For unions, we'll use the first non-null type
	// This is a simplification - in a full implementation, we might want to create a union type
	for _, unionType := range unionTypes {
		if typeStr, ok := unionType.(string); ok && typeStr == "null" {
			continue // Skip null types
		}

		// Use the first non-null type
		return convertAvroTypeToSeaweedMQ(unionType)
	}

	// If all types are null, return bytes type
	return &schema_pb.Type{
		Kind: &schema_pb.Type_ScalarType{
			ScalarType: schema_pb.ScalarType_BYTES,
		},
	}, nil
}

// InferRecordTypeFromMap infers a RecordType from a decoded map
// This is useful when we don't have the original Avro schema
func InferRecordTypeFromMap(m map[string]interface{}) *schema_pb.RecordType {
	fields := make([]*schema_pb.Field, 0, len(m))
	fieldIndex := int32(0)

	for key, value := range m {
		fieldType := inferTypeFromValue(value)

		field := &schema_pb.Field{
			Name:       key,
			FieldIndex: fieldIndex,
			Type:       fieldType,
			IsRequired: value != nil, // Non-nil values are considered required
			IsRepeated: false,
		}

		// Check if it's an array
		if reflect.TypeOf(value).Kind() == reflect.Slice {
			field.IsRepeated = true
		}

		fields = append(fields, field)
		fieldIndex++
	}

	return &schema_pb.RecordType{
		Fields: fields,
	}
}

// inferTypeFromValue infers a SeaweedMQ Type from a Go value
func inferTypeFromValue(value interface{}) *schema_pb.Type {
	if value == nil {
		// Default to string for null values
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_STRING,
			},
		}
	}

	switch v := value.(type) {
	case bool:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_BOOL,
			},
		}
	case int32:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT32,
			},
		}
	case int64, int:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_INT64,
			},
		}
	case float32:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_FLOAT,
			},
		}
	case float64:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_DOUBLE,
			},
		}
	case string:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_STRING,
			},
		}
	case []byte:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_BYTES,
			},
		}
	case time.Time:
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_TIMESTAMP,
			},
		}
	case []interface{}:
		// Handle arrays - infer element type from first element
		var elementType *schema_pb.Type
		if len(v) > 0 {
			elementType = inferTypeFromValue(v[0])
		} else {
			// Default to string for empty arrays
			elementType = &schema_pb.Type{
				Kind: &schema_pb.Type_ScalarType{
					ScalarType: schema_pb.ScalarType_STRING,
				},
			}
		}

		return &schema_pb.Type{
			Kind: &schema_pb.Type_ListType{
				ListType: &schema_pb.ListType{
					ElementType: elementType,
				},
			},
		}
	case map[string]interface{}:
		// Handle nested records
		nestedRecordType := InferRecordTypeFromMap(v)
		return &schema_pb.Type{
			Kind: &schema_pb.Type_RecordType{
				RecordType: nestedRecordType,
			},
		}
	default:
		// Default to string for unknown types
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_STRING,
			},
		}
	}
}
