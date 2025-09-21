package schema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// SplitFlatSchemaToKeyValue takes a flat RecordType and key column names,
// returns separate key and value RecordTypes
func SplitFlatSchemaToKeyValue(flatSchema *schema_pb.RecordType, keyColumns []string) (*schema_pb.RecordType, *schema_pb.RecordType, error) {
	if flatSchema == nil {
		return nil, nil, nil
	}

	// Create maps for fast lookup
	keyColumnSet := make(map[string]bool)
	for _, col := range keyColumns {
		keyColumnSet[col] = true
	}

	var keyFields []*schema_pb.Field
	var valueFields []*schema_pb.Field

	// Split fields based on key columns
	for _, field := range flatSchema.Fields {
		if keyColumnSet[field.Name] {
			// Create key field with reindexed field index
			keyField := &schema_pb.Field{
				Name:       field.Name,
				FieldIndex: int32(len(keyFields)),
				Type:       field.Type,
				IsRepeated: field.IsRepeated,
				IsRequired: field.IsRequired,
			}
			keyFields = append(keyFields, keyField)
		} else {
			// Create value field with reindexed field index
			valueField := &schema_pb.Field{
				Name:       field.Name,
				FieldIndex: int32(len(valueFields)),
				Type:       field.Type,
				IsRepeated: field.IsRepeated,
				IsRequired: field.IsRequired,
			}
			valueFields = append(valueFields, valueField)
		}
	}

	// Validate that all key columns were found
	if len(keyFields) != len(keyColumns) {
		missingCols := []string{}
		for _, col := range keyColumns {
			found := false
			for _, field := range keyFields {
				if field.Name == col {
					found = true
					break
				}
			}
			if !found {
				missingCols = append(missingCols, col)
			}
		}
		if len(missingCols) > 0 {
			return nil, nil, fmt.Errorf("key columns not found in schema: %v", missingCols)
		}
	}

	var keyRecordType *schema_pb.RecordType
	if len(keyFields) > 0 {
		keyRecordType = &schema_pb.RecordType{Fields: keyFields}
	}

	var valueRecordType *schema_pb.RecordType
	if len(valueFields) > 0 {
		valueRecordType = &schema_pb.RecordType{Fields: valueFields}
	}

	return keyRecordType, valueRecordType, nil
}

// CombineFlatSchemaFromKeyValue creates a flat RecordType by combining key and value schemas
// Key fields are placed first, then value fields
func CombineFlatSchemaFromKeyValue(keySchema *schema_pb.RecordType, valueSchema *schema_pb.RecordType) (*schema_pb.RecordType, []string) {
	var combinedFields []*schema_pb.Field
	var keyColumns []string

	// Add key fields first
	if keySchema != nil {
		for _, field := range keySchema.Fields {
			combinedField := &schema_pb.Field{
				Name:       field.Name,
				FieldIndex: int32(len(combinedFields)),
				Type:       field.Type,
				IsRepeated: field.IsRepeated,
				IsRequired: field.IsRequired,
			}
			combinedFields = append(combinedFields, combinedField)
			keyColumns = append(keyColumns, field.Name)
		}
	}

	// Add value fields
	if valueSchema != nil {
		for _, field := range valueSchema.Fields {
			// Check for name conflicts
			fieldName := field.Name
			for _, keyCol := range keyColumns {
				if fieldName == keyCol {
					// This shouldn't happen in well-formed schemas, but handle gracefully
					fieldName = "value_" + fieldName
					break
				}
			}

			combinedField := &schema_pb.Field{
				Name:       fieldName,
				FieldIndex: int32(len(combinedFields)),
				Type:       field.Type,
				IsRepeated: field.IsRepeated,
				IsRequired: field.IsRequired,
			}
			combinedFields = append(combinedFields, combinedField)
		}
	}

	if len(combinedFields) == 0 {
		return nil, keyColumns
	}

	return &schema_pb.RecordType{Fields: combinedFields}, keyColumns
}

// ExtractKeyColumnsFromCombinedSchema tries to infer key columns from a combined schema
// that was created using CreateCombinedRecordType (with key_ prefixes)
func ExtractKeyColumnsFromCombinedSchema(combinedSchema *schema_pb.RecordType) (flatSchema *schema_pb.RecordType, keyColumns []string) {
	if combinedSchema == nil {
		return nil, nil
	}

	var flatFields []*schema_pb.Field
	var keyColumns_ []string

	for _, field := range combinedSchema.Fields {
		if strings.HasPrefix(field.Name, "key_") {
			// This is a key field - remove the prefix
			originalName := strings.TrimPrefix(field.Name, "key_")
			flatField := &schema_pb.Field{
				Name:       originalName,
				FieldIndex: int32(len(flatFields)),
				Type:       field.Type,
				IsRepeated: field.IsRepeated,
				IsRequired: field.IsRequired,
			}
			flatFields = append(flatFields, flatField)
			keyColumns_ = append(keyColumns_, originalName)
		} else {
			// This is a value field
			flatField := &schema_pb.Field{
				Name:       field.Name,
				FieldIndex: int32(len(flatFields)),
				Type:       field.Type,
				IsRepeated: field.IsRepeated,
				IsRequired: field.IsRequired,
			}
			flatFields = append(flatFields, flatField)
		}
	}

	// Sort key columns to ensure deterministic order
	sort.Strings(keyColumns_)

	if len(flatFields) == 0 {
		return nil, keyColumns_
	}

	return &schema_pb.RecordType{Fields: flatFields}, keyColumns_
}

// ValidateKeyColumns checks that all key columns exist in the schema
func ValidateKeyColumns(schema *schema_pb.RecordType, keyColumns []string) error {
	if schema == nil || len(keyColumns) == 0 {
		return nil
	}

	fieldNames := make(map[string]bool)
	for _, field := range schema.Fields {
		fieldNames[field.Name] = true
	}

	var missingColumns []string
	for _, keyCol := range keyColumns {
		if !fieldNames[keyCol] {
			missingColumns = append(missingColumns, keyCol)
		}
	}

	if len(missingColumns) > 0 {
		return fmt.Errorf("key columns not found in schema: %v", missingColumns)
	}

	return nil
}
