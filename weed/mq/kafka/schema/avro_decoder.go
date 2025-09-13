package schema

import (
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
		// Check if this is an Avro union type (single key-value pair)
		if len(v) == 1 {
			for unionType, unionValue := range v {
				// Handle common union type patterns
				switch unionType {
				case "int":
					if intVal, ok := unionValue.(int32); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_Int64Value{Int64Value: int64(intVal)},
						}
					}
				case "long":
					if longVal, ok := unionValue.(int64); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_Int64Value{Int64Value: longVal},
						}
					}
				case "float":
					if floatVal, ok := unionValue.(float32); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_FloatValue{FloatValue: floatVal},
						}
					}
				case "double":
					if doubleVal, ok := unionValue.(float64); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_DoubleValue{DoubleValue: doubleVal},
						}
					}
				case "string":
					if strVal, ok := unionValue.(string); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_StringValue{StringValue: strVal},
						}
					}
				case "boolean":
					if boolVal, ok := unionValue.(bool); ok {
						return &schema_pb.Value{
							Kind: &schema_pb.Value_BoolValue{BoolValue: boolVal},
						}
					}
				}
				// If it's not a recognized union type, recurse on the value
				return goValueToSchemaValue(unionValue)
			}
		}

		// Handle nested records (not union types)
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
	// Parse the Avro schema JSON
	codec, err := goavro.NewCodec(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Avro schema: %w", err)
	}

	// For now, we'll create a simplified RecordType
	// In a full implementation, we would parse the Avro schema JSON
	// and extract field definitions to create proper SeaweedMQ field types

	// This is a placeholder implementation that creates a flexible schema
	// allowing any field types (which will be determined at runtime)
	fields := []*schema_pb.Field{
		{
			Name:       "avro_data",
			FieldIndex: 0,
			Type: &schema_pb.Type{
				Kind: &schema_pb.Type_RecordType{
					RecordType: &schema_pb.RecordType{
						Fields: []*schema_pb.Field{}, // Dynamic fields
					},
				},
			},
			IsRequired: false,
			IsRepeated: false,
		},
	}

	// TODO: In Phase 4, we'll implement proper Avro schema parsing
	// to extract field definitions and create accurate SeaweedMQ types
	_ = codec // Use the codec to avoid unused variable warning

	return &schema_pb.RecordType{
		Fields: fields,
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
