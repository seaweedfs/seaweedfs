package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/xeipuuv/gojsonschema"
)

// JSONSchemaDecoder handles JSON Schema validation and conversion to SeaweedMQ format
type JSONSchemaDecoder struct {
	schema     *gojsonschema.Schema
	schemaDoc  map[string]interface{} // Parsed schema document for type inference
	schemaJSON string                 // Original schema JSON
}

// NewJSONSchemaDecoder creates a new JSON Schema decoder from a schema string
func NewJSONSchemaDecoder(schemaJSON string) (*JSONSchemaDecoder, error) {
	// Parse the schema JSON
	var schemaDoc map[string]interface{}
	if err := json.Unmarshal([]byte(schemaJSON), &schemaDoc); err != nil {
		return nil, fmt.Errorf("failed to parse JSON schema: %w", err)
	}

	// Create JSON Schema validator
	schemaLoader := gojsonschema.NewStringLoader(schemaJSON)
	schema, err := gojsonschema.NewSchema(schemaLoader)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON schema validator: %w", err)
	}

	return &JSONSchemaDecoder{
		schema:     schema,
		schemaDoc:  schemaDoc,
		schemaJSON: schemaJSON,
	}, nil
}

// Decode decodes and validates JSON data against the schema, returning a Go map
// Uses json.Number to preserve integer precision (important for large int64 like timestamps)
func (jsd *JSONSchemaDecoder) Decode(data []byte) (map[string]interface{}, error) {
	// Parse JSON data with Number support to preserve large integers
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	var jsonData interface{}
	if err := decoder.Decode(&jsonData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON data: %w", err)
	}

	// Validate against schema
	documentLoader := gojsonschema.NewGoLoader(jsonData)
	result, err := jsd.schema.Validate(documentLoader)
	if err != nil {
		return nil, fmt.Errorf("failed to validate JSON data: %w", err)
	}

	if !result.Valid() {
		// Collect validation errors
		var errorMsgs []string
		for _, desc := range result.Errors() {
			errorMsgs = append(errorMsgs, desc.String())
		}
		return nil, fmt.Errorf("JSON data validation failed: %v", errorMsgs)
	}

	// Convert to map[string]interface{} for consistency
	switch v := jsonData.(type) {
	case map[string]interface{}:
		return v, nil
	case []interface{}:
		// Handle array at root level by wrapping in a map
		return map[string]interface{}{"items": v}, nil
	default:
		// Handle primitive values at root level
		return map[string]interface{}{"value": v}, nil
	}
}

// DecodeToRecordValue decodes JSON data directly to SeaweedMQ RecordValue
// Preserves large integers (like nanosecond timestamps) with full precision
func (jsd *JSONSchemaDecoder) DecodeToRecordValue(data []byte) (*schema_pb.RecordValue, error) {
	// Decode with json.Number for precision
	jsonMap, err := jsd.Decode(data)
	if err != nil {
		return nil, err
	}

	// Convert with schema-aware type conversion
	return jsd.mapToRecordValueWithSchema(jsonMap), nil
}

// mapToRecordValueWithSchema converts a map to RecordValue using schema type information
func (jsd *JSONSchemaDecoder) mapToRecordValueWithSchema(m map[string]interface{}) *schema_pb.RecordValue {
	fields := make(map[string]*schema_pb.Value)
	properties, _ := jsd.schemaDoc["properties"].(map[string]interface{})

	for key, value := range m {
		// Check if we have schema information for this field
		if fieldSchema, exists := properties[key]; exists {
			if fieldSchemaMap, ok := fieldSchema.(map[string]interface{}); ok {
				fields[key] = jsd.goValueToSchemaValueWithType(value, fieldSchemaMap)
				continue
			}
		}
		// Fallback to default conversion
		fields[key] = goValueToSchemaValue(value)
	}

	return &schema_pb.RecordValue{
		Fields: fields,
	}
}

// goValueToSchemaValueWithType converts a Go value to SchemaValue using schema type hints
func (jsd *JSONSchemaDecoder) goValueToSchemaValueWithType(value interface{}, schemaDoc map[string]interface{}) *schema_pb.Value {
	if value == nil {
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: ""},
		}
	}

	schemaType, _ := schemaDoc["type"].(string)

	// Handle numbers from JSON that should be integers
	if schemaType == "integer" {
		switch v := value.(type) {
		case json.Number:
			// Preserve precision by parsing as int64
			if intVal, err := v.Int64(); err == nil {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_Int64Value{Int64Value: intVal},
				}
			}
			// Fallback to float conversion if int64 parsing fails
			if floatVal, err := v.Float64(); err == nil {
				return &schema_pb.Value{
					Kind: &schema_pb.Value_Int64Value{Int64Value: int64(floatVal)},
				}
			}
		case float64:
			// JSON unmarshals all numbers as float64, convert to int64 for integer types
			return &schema_pb.Value{
				Kind: &schema_pb.Value_Int64Value{Int64Value: int64(v)},
			}
		case int64:
			return &schema_pb.Value{
				Kind: &schema_pb.Value_Int64Value{Int64Value: v},
			}
		case int:
			return &schema_pb.Value{
				Kind: &schema_pb.Value_Int64Value{Int64Value: int64(v)},
			}
		}
	}

	// Handle json.Number for other numeric types
	if numVal, ok := value.(json.Number); ok {
		// Try int64 first
		if intVal, err := numVal.Int64(); err == nil {
			return &schema_pb.Value{
				Kind: &schema_pb.Value_Int64Value{Int64Value: intVal},
			}
		}
		// Fallback to float64
		if floatVal, err := numVal.Float64(); err == nil {
			return &schema_pb.Value{
				Kind: &schema_pb.Value_DoubleValue{DoubleValue: floatVal},
			}
		}
	}

	// Handle nested objects
	if schemaType == "object" {
		if nestedMap, ok := value.(map[string]interface{}); ok {
			nestedProperties, _ := schemaDoc["properties"].(map[string]interface{})
			nestedFields := make(map[string]*schema_pb.Value)

			for key, val := range nestedMap {
				if fieldSchema, exists := nestedProperties[key]; exists {
					if fieldSchemaMap, ok := fieldSchema.(map[string]interface{}); ok {
						nestedFields[key] = jsd.goValueToSchemaValueWithType(val, fieldSchemaMap)
						continue
					}
				}
				// Fallback
				nestedFields[key] = goValueToSchemaValue(val)
			}

			return &schema_pb.Value{
				Kind: &schema_pb.Value_RecordValue{
					RecordValue: &schema_pb.RecordValue{
						Fields: nestedFields,
					},
				},
			}
		}
	}

	// For other types, use default conversion
	return goValueToSchemaValue(value)
}

// InferRecordType infers a SeaweedMQ RecordType from the JSON Schema
func (jsd *JSONSchemaDecoder) InferRecordType() (*schema_pb.RecordType, error) {
	return jsd.jsonSchemaToRecordType(jsd.schemaDoc), nil
}

// ValidateOnly validates JSON data against the schema without decoding
func (jsd *JSONSchemaDecoder) ValidateOnly(data []byte) error {
	_, err := jsd.Decode(data)
	return err
}

// jsonSchemaToRecordType converts a JSON Schema to SeaweedMQ RecordType
func (jsd *JSONSchemaDecoder) jsonSchemaToRecordType(schemaDoc map[string]interface{}) *schema_pb.RecordType {
	schemaType, _ := schemaDoc["type"].(string)

	if schemaType == "object" {
		return jsd.objectSchemaToRecordType(schemaDoc)
	}

	// For non-object schemas, create a wrapper record
	return &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "value",
				FieldIndex: 0,
				Type:       jsd.jsonSchemaTypeToType(schemaDoc),
				IsRequired: true,
				IsRepeated: false,
			},
		},
	}
}

// objectSchemaToRecordType converts an object JSON Schema to RecordType
func (jsd *JSONSchemaDecoder) objectSchemaToRecordType(schemaDoc map[string]interface{}) *schema_pb.RecordType {
	properties, _ := schemaDoc["properties"].(map[string]interface{})
	required, _ := schemaDoc["required"].([]interface{})

	// Create set of required fields for quick lookup
	requiredFields := make(map[string]bool)
	for _, req := range required {
		if reqStr, ok := req.(string); ok {
			requiredFields[reqStr] = true
		}
	}

	fields := make([]*schema_pb.Field, 0, len(properties))
	fieldIndex := int32(0)

	for fieldName, fieldSchema := range properties {
		fieldSchemaMap, ok := fieldSchema.(map[string]interface{})
		if !ok {
			continue
		}

		field := &schema_pb.Field{
			Name:       fieldName,
			FieldIndex: fieldIndex,
			Type:       jsd.jsonSchemaTypeToType(fieldSchemaMap),
			IsRequired: requiredFields[fieldName],
			IsRepeated: jsd.isArrayType(fieldSchemaMap),
		}

		fields = append(fields, field)
		fieldIndex++
	}

	return &schema_pb.RecordType{
		Fields: fields,
	}
}

// jsonSchemaTypeToType converts a JSON Schema type to SeaweedMQ Type
func (jsd *JSONSchemaDecoder) jsonSchemaTypeToType(schemaDoc map[string]interface{}) *schema_pb.Type {
	schemaType, _ := schemaDoc["type"].(string)

	switch schemaType {
	case "boolean":
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_BOOL,
			},
		}
	case "integer":
		// Check for format hints
		format, _ := schemaDoc["format"].(string)
		switch format {
		case "int32":
			return &schema_pb.Type{
				Kind: &schema_pb.Type_ScalarType{
					ScalarType: schema_pb.ScalarType_INT32,
				},
			}
		default:
			return &schema_pb.Type{
				Kind: &schema_pb.Type_ScalarType{
					ScalarType: schema_pb.ScalarType_INT64,
				},
			}
		}
	case "number":
		// Check for format hints
		format, _ := schemaDoc["format"].(string)
		switch format {
		case "float":
			return &schema_pb.Type{
				Kind: &schema_pb.Type_ScalarType{
					ScalarType: schema_pb.ScalarType_FLOAT,
				},
			}
		default:
			return &schema_pb.Type{
				Kind: &schema_pb.Type_ScalarType{
					ScalarType: schema_pb.ScalarType_DOUBLE,
				},
			}
		}
	case "string":
		// Check for format hints
		format, _ := schemaDoc["format"].(string)
		switch format {
		case "date-time":
			return &schema_pb.Type{
				Kind: &schema_pb.Type_ScalarType{
					ScalarType: schema_pb.ScalarType_TIMESTAMP,
				},
			}
		case "byte", "binary":
			return &schema_pb.Type{
				Kind: &schema_pb.Type_ScalarType{
					ScalarType: schema_pb.ScalarType_BYTES,
				},
			}
		default:
			return &schema_pb.Type{
				Kind: &schema_pb.Type_ScalarType{
					ScalarType: schema_pb.ScalarType_STRING,
				},
			}
		}
	case "array":
		items, _ := schemaDoc["items"].(map[string]interface{})
		elementType := jsd.jsonSchemaTypeToType(items)
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ListType{
				ListType: &schema_pb.ListType{
					ElementType: elementType,
				},
			},
		}
	case "object":
		nestedRecordType := jsd.objectSchemaToRecordType(schemaDoc)
		return &schema_pb.Type{
			Kind: &schema_pb.Type_RecordType{
				RecordType: nestedRecordType,
			},
		}
	default:
		// Handle union types (oneOf, anyOf, allOf)
		if oneOf, exists := schemaDoc["oneOf"].([]interface{}); exists && len(oneOf) > 0 {
			// For unions, use the first type as default
			if firstType, ok := oneOf[0].(map[string]interface{}); ok {
				return jsd.jsonSchemaTypeToType(firstType)
			}
		}

		// Default to string for unknown types
		return &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{
				ScalarType: schema_pb.ScalarType_STRING,
			},
		}
	}
}

// isArrayType checks if a JSON Schema represents an array type
func (jsd *JSONSchemaDecoder) isArrayType(schemaDoc map[string]interface{}) bool {
	schemaType, _ := schemaDoc["type"].(string)
	return schemaType == "array"
}

// EncodeFromRecordValue encodes a RecordValue back to JSON format
func (jsd *JSONSchemaDecoder) EncodeFromRecordValue(recordValue *schema_pb.RecordValue) ([]byte, error) {
	// Convert RecordValue back to Go map
	goMap := recordValueToMap(recordValue)

	// Encode to JSON
	jsonData, err := json.Marshal(goMap)
	if err != nil {
		return nil, fmt.Errorf("failed to encode to JSON: %w", err)
	}

	// Validate the generated JSON against the schema
	if err := jsd.ValidateOnly(jsonData); err != nil {
		return nil, fmt.Errorf("generated JSON failed schema validation: %w", err)
	}

	return jsonData, nil
}

// GetSchemaInfo returns information about the JSON Schema
func (jsd *JSONSchemaDecoder) GetSchemaInfo() map[string]interface{} {
	info := make(map[string]interface{})

	if title, exists := jsd.schemaDoc["title"]; exists {
		info["title"] = title
	}

	if description, exists := jsd.schemaDoc["description"]; exists {
		info["description"] = description
	}

	if schemaVersion, exists := jsd.schemaDoc["$schema"]; exists {
		info["schema_version"] = schemaVersion
	}

	if schemaType, exists := jsd.schemaDoc["type"]; exists {
		info["type"] = schemaType
	}

	return info
}

// Enhanced JSON value conversion with better type handling
func (jsd *JSONSchemaDecoder) convertJSONValue(value interface{}, expectedType string) interface{} {
	if value == nil {
		return nil
	}

	switch expectedType {
	case "integer":
		switch v := value.(type) {
		case float64:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
	case "number":
		switch v := value.(type) {
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
	case "boolean":
		switch v := value.(type) {
		case string:
			if b, err := strconv.ParseBool(v); err == nil {
				return b
			}
		}
	case "string":
		// Handle date-time format conversion
		if str, ok := value.(string); ok {
			// Try to parse as RFC3339 timestamp
			if t, err := time.Parse(time.RFC3339, str); err == nil {
				return t
			}
		}
	}

	return value
}

// ValidateAndNormalize validates JSON data and normalizes types according to schema
func (jsd *JSONSchemaDecoder) ValidateAndNormalize(data []byte) ([]byte, error) {
	// First decode normally
	jsonMap, err := jsd.Decode(data)
	if err != nil {
		return nil, err
	}

	// Normalize types based on schema
	normalized := jsd.normalizeMapTypes(jsonMap, jsd.schemaDoc)

	// Re-encode with normalized types
	return json.Marshal(normalized)
}

// normalizeMapTypes normalizes map values according to JSON Schema types
func (jsd *JSONSchemaDecoder) normalizeMapTypes(data map[string]interface{}, schemaDoc map[string]interface{}) map[string]interface{} {
	properties, _ := schemaDoc["properties"].(map[string]interface{})
	result := make(map[string]interface{})

	for key, value := range data {
		if fieldSchema, exists := properties[key]; exists {
			if fieldSchemaMap, ok := fieldSchema.(map[string]interface{}); ok {
				fieldType, _ := fieldSchemaMap["type"].(string)
				result[key] = jsd.convertJSONValue(value, fieldType)
				continue
			}
		}
		result[key] = value
	}

	return result
}
