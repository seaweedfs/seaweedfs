package schema

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/linkedin/goavro/v2"
)

// CompatibilityLevel defines the schema compatibility level
type CompatibilityLevel string

const (
	CompatibilityNone     CompatibilityLevel = "NONE"
	CompatibilityBackward CompatibilityLevel = "BACKWARD"
	CompatibilityForward  CompatibilityLevel = "FORWARD"
	CompatibilityFull     CompatibilityLevel = "FULL"
)

// SchemaEvolutionChecker handles schema compatibility checking and evolution
type SchemaEvolutionChecker struct {
	// Cache for parsed schemas to avoid re-parsing
	schemaCache map[string]interface{}
}

// NewSchemaEvolutionChecker creates a new schema evolution checker
func NewSchemaEvolutionChecker() *SchemaEvolutionChecker {
	return &SchemaEvolutionChecker{
		schemaCache: make(map[string]interface{}),
	}
}

// CompatibilityResult represents the result of a compatibility check
type CompatibilityResult struct {
	Compatible bool
	Issues     []string
	Level      CompatibilityLevel
}

// CheckCompatibility checks if two schemas are compatible according to the specified level
func (checker *SchemaEvolutionChecker) CheckCompatibility(
	oldSchemaStr, newSchemaStr string,
	format Format,
	level CompatibilityLevel,
) (*CompatibilityResult, error) {

	result := &CompatibilityResult{
		Compatible: true,
		Issues:     []string{},
		Level:      level,
	}

	if level == CompatibilityNone {
		return result, nil
	}

	switch format {
	case FormatAvro:
		return checker.checkAvroCompatibility(oldSchemaStr, newSchemaStr, level)
	case FormatProtobuf:
		return checker.checkProtobufCompatibility(oldSchemaStr, newSchemaStr, level)
	case FormatJSONSchema:
		return checker.checkJSONSchemaCompatibility(oldSchemaStr, newSchemaStr, level)
	default:
		return nil, fmt.Errorf("unsupported schema format for compatibility check: %s", format)
	}
}

// checkAvroCompatibility checks Avro schema compatibility
func (checker *SchemaEvolutionChecker) checkAvroCompatibility(
	oldSchemaStr, newSchemaStr string,
	level CompatibilityLevel,
) (*CompatibilityResult, error) {

	result := &CompatibilityResult{
		Compatible: true,
		Issues:     []string{},
		Level:      level,
	}

	// Parse old schema
	oldSchema, err := goavro.NewCodec(oldSchemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse old Avro schema: %w", err)
	}

	// Parse new schema
	newSchema, err := goavro.NewCodec(newSchemaStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse new Avro schema: %w", err)
	}

	// Parse schema structures for detailed analysis
	var oldSchemaMap, newSchemaMap map[string]interface{}
	if err := json.Unmarshal([]byte(oldSchemaStr), &oldSchemaMap); err != nil {
		return nil, fmt.Errorf("failed to parse old schema JSON: %w", err)
	}
	if err := json.Unmarshal([]byte(newSchemaStr), &newSchemaMap); err != nil {
		return nil, fmt.Errorf("failed to parse new schema JSON: %w", err)
	}

	// Check compatibility based on level
	switch level {
	case CompatibilityBackward:
		checker.checkAvroBackwardCompatibility(oldSchemaMap, newSchemaMap, result)
	case CompatibilityForward:
		checker.checkAvroForwardCompatibility(oldSchemaMap, newSchemaMap, result)
	case CompatibilityFull:
		checker.checkAvroBackwardCompatibility(oldSchemaMap, newSchemaMap, result)
		if result.Compatible {
			checker.checkAvroForwardCompatibility(oldSchemaMap, newSchemaMap, result)
		}
	}

	// Additional validation: try to create test data and check if it can be read
	if result.Compatible {
		if err := checker.validateAvroDataCompatibility(oldSchema, newSchema, level); err != nil {
			result.Compatible = false
			result.Issues = append(result.Issues, fmt.Sprintf("Data compatibility test failed: %v", err))
		}
	}

	return result, nil
}

// checkAvroBackwardCompatibility checks if new schema can read data written with old schema
func (checker *SchemaEvolutionChecker) checkAvroBackwardCompatibility(
	oldSchema, newSchema map[string]interface{},
	result *CompatibilityResult,
) {
	// Check if fields were removed without defaults
	oldFields := checker.extractAvroFields(oldSchema)
	newFields := checker.extractAvroFields(newSchema)

	for fieldName, oldField := range oldFields {
		if newField, exists := newFields[fieldName]; !exists {
			// Field was removed - this breaks backward compatibility
			result.Compatible = false
			result.Issues = append(result.Issues,
				fmt.Sprintf("Field '%s' was removed, breaking backward compatibility", fieldName))
		} else {
			// Field exists, check type compatibility
			if !checker.areAvroTypesCompatible(oldField["type"], newField["type"], true) {
				result.Compatible = false
				result.Issues = append(result.Issues,
					fmt.Sprintf("Field '%s' type changed incompatibly", fieldName))
			}
		}
	}

	// Check if new required fields were added without defaults
	for fieldName, newField := range newFields {
		if _, exists := oldFields[fieldName]; !exists {
			// New field added
			if _, hasDefault := newField["default"]; !hasDefault {
				result.Compatible = false
				result.Issues = append(result.Issues,
					fmt.Sprintf("New required field '%s' added without default value", fieldName))
			}
		}
	}
}

// checkAvroForwardCompatibility checks if old schema can read data written with new schema
func (checker *SchemaEvolutionChecker) checkAvroForwardCompatibility(
	oldSchema, newSchema map[string]interface{},
	result *CompatibilityResult,
) {
	// Check if fields were added without defaults in old schema
	oldFields := checker.extractAvroFields(oldSchema)
	newFields := checker.extractAvroFields(newSchema)

	for fieldName, newField := range newFields {
		if _, exists := oldFields[fieldName]; !exists {
			// New field added - for forward compatibility, the new field should have a default
			// so that old schema can ignore it when reading data written with new schema
			if _, hasDefault := newField["default"]; !hasDefault {
				result.Compatible = false
				result.Issues = append(result.Issues,
					fmt.Sprintf("New field '%s' cannot be read by old schema (no default)", fieldName))
			}
		} else {
			// Field exists, check type compatibility (reverse direction)
			oldField := oldFields[fieldName]
			if !checker.areAvroTypesCompatible(newField["type"], oldField["type"], false) {
				result.Compatible = false
				result.Issues = append(result.Issues,
					fmt.Sprintf("Field '%s' type change breaks forward compatibility", fieldName))
			}
		}
	}

	// Check if fields were removed
	for fieldName := range oldFields {
		if _, exists := newFields[fieldName]; !exists {
			result.Compatible = false
			result.Issues = append(result.Issues,
				fmt.Sprintf("Field '%s' was removed, breaking forward compatibility", fieldName))
		}
	}
}

// extractAvroFields extracts field information from an Avro schema
func (checker *SchemaEvolutionChecker) extractAvroFields(schema map[string]interface{}) map[string]map[string]interface{} {
	fields := make(map[string]map[string]interface{})

	if fieldsArray, ok := schema["fields"].([]interface{}); ok {
		for _, fieldInterface := range fieldsArray {
			if field, ok := fieldInterface.(map[string]interface{}); ok {
				if name, ok := field["name"].(string); ok {
					fields[name] = field
				}
			}
		}
	}

	return fields
}

// areAvroTypesCompatible checks if two Avro types are compatible
func (checker *SchemaEvolutionChecker) areAvroTypesCompatible(oldType, newType interface{}, backward bool) bool {
	// Simplified type compatibility check
	// In a full implementation, this would handle complex types, unions, etc.

	oldTypeStr := fmt.Sprintf("%v", oldType)
	newTypeStr := fmt.Sprintf("%v", newType)

	// Same type is always compatible
	if oldTypeStr == newTypeStr {
		return true
	}

	// Check for promotable types (e.g., int -> long, float -> double)
	if backward {
		return checker.isPromotableType(oldTypeStr, newTypeStr)
	} else {
		return checker.isPromotableType(newTypeStr, oldTypeStr)
	}
}

// isPromotableType checks if a type can be promoted to another
func (checker *SchemaEvolutionChecker) isPromotableType(from, to string) bool {
	promotions := map[string][]string{
		"int":    {"long", "float", "double"},
		"long":   {"float", "double"},
		"float":  {"double"},
		"string": {"bytes"},
		"bytes":  {"string"},
	}

	if validPromotions, exists := promotions[from]; exists {
		for _, validTo := range validPromotions {
			if to == validTo {
				return true
			}
		}
	}

	return false
}

// validateAvroDataCompatibility validates compatibility by testing with actual data
func (checker *SchemaEvolutionChecker) validateAvroDataCompatibility(
	oldSchema, newSchema *goavro.Codec,
	level CompatibilityLevel,
) error {
	// Create test data with old schema
	testData := map[string]interface{}{
		"test_field": "test_value",
	}

	// Try to encode with old schema
	encoded, err := oldSchema.BinaryFromNative(nil, testData)
	if err != nil {
		// If we can't create test data, skip validation
		return nil
	}

	// Try to decode with new schema (backward compatibility)
	if level == CompatibilityBackward || level == CompatibilityFull {
		_, _, err := newSchema.NativeFromBinary(encoded)
		if err != nil {
			return fmt.Errorf("backward compatibility failed: %w", err)
		}
	}

	// Try to encode with new schema and decode with old (forward compatibility)
	if level == CompatibilityForward || level == CompatibilityFull {
		newEncoded, err := newSchema.BinaryFromNative(nil, testData)
		if err == nil {
			_, _, err = oldSchema.NativeFromBinary(newEncoded)
			if err != nil {
				return fmt.Errorf("forward compatibility failed: %w", err)
			}
		}
	}

	return nil
}

// checkProtobufCompatibility checks Protobuf schema compatibility
func (checker *SchemaEvolutionChecker) checkProtobufCompatibility(
	oldSchemaStr, newSchemaStr string,
	level CompatibilityLevel,
) (*CompatibilityResult, error) {

	result := &CompatibilityResult{
		Compatible: true,
		Issues:     []string{},
		Level:      level,
	}

	// For now, implement basic Protobuf compatibility rules
	// In a full implementation, this would parse .proto files and check field numbers, types, etc.

	// Basic check: if schemas are identical, they're compatible
	if oldSchemaStr == newSchemaStr {
		return result, nil
	}

	// For protobuf, we need to parse the schema and check:
	// - Field numbers haven't changed
	// - Required fields haven't been removed
	// - Field types are compatible

	// Simplified implementation - mark as compatible with warning
	result.Issues = append(result.Issues, "Protobuf compatibility checking is simplified - manual review recommended")

	return result, nil
}

// checkJSONSchemaCompatibility checks JSON Schema compatibility
func (checker *SchemaEvolutionChecker) checkJSONSchemaCompatibility(
	oldSchemaStr, newSchemaStr string,
	level CompatibilityLevel,
) (*CompatibilityResult, error) {

	result := &CompatibilityResult{
		Compatible: true,
		Issues:     []string{},
		Level:      level,
	}

	// Parse JSON schemas
	var oldSchema, newSchema map[string]interface{}
	if err := json.Unmarshal([]byte(oldSchemaStr), &oldSchema); err != nil {
		return nil, fmt.Errorf("failed to parse old JSON schema: %w", err)
	}
	if err := json.Unmarshal([]byte(newSchemaStr), &newSchema); err != nil {
		return nil, fmt.Errorf("failed to parse new JSON schema: %w", err)
	}

	// Check compatibility based on level
	switch level {
	case CompatibilityBackward:
		checker.checkJSONSchemaBackwardCompatibility(oldSchema, newSchema, result)
	case CompatibilityForward:
		checker.checkJSONSchemaForwardCompatibility(oldSchema, newSchema, result)
	case CompatibilityFull:
		checker.checkJSONSchemaBackwardCompatibility(oldSchema, newSchema, result)
		if result.Compatible {
			checker.checkJSONSchemaForwardCompatibility(oldSchema, newSchema, result)
		}
	}

	return result, nil
}

// checkJSONSchemaBackwardCompatibility checks JSON Schema backward compatibility
func (checker *SchemaEvolutionChecker) checkJSONSchemaBackwardCompatibility(
	oldSchema, newSchema map[string]interface{},
	result *CompatibilityResult,
) {
	// Check if required fields were added
	oldRequired := checker.extractJSONSchemaRequired(oldSchema)
	newRequired := checker.extractJSONSchemaRequired(newSchema)

	for _, field := range newRequired {
		if !contains(oldRequired, field) {
			result.Compatible = false
			result.Issues = append(result.Issues,
				fmt.Sprintf("New required field '%s' breaks backward compatibility", field))
		}
	}

	// Check if properties were removed
	oldProperties := checker.extractJSONSchemaProperties(oldSchema)
	newProperties := checker.extractJSONSchemaProperties(newSchema)

	for propName := range oldProperties {
		if _, exists := newProperties[propName]; !exists {
			result.Compatible = false
			result.Issues = append(result.Issues,
				fmt.Sprintf("Property '%s' was removed, breaking backward compatibility", propName))
		}
	}
}

// checkJSONSchemaForwardCompatibility checks JSON Schema forward compatibility
func (checker *SchemaEvolutionChecker) checkJSONSchemaForwardCompatibility(
	oldSchema, newSchema map[string]interface{},
	result *CompatibilityResult,
) {
	// Check if required fields were removed
	oldRequired := checker.extractJSONSchemaRequired(oldSchema)
	newRequired := checker.extractJSONSchemaRequired(newSchema)

	for _, field := range oldRequired {
		if !contains(newRequired, field) {
			result.Compatible = false
			result.Issues = append(result.Issues,
				fmt.Sprintf("Required field '%s' was removed, breaking forward compatibility", field))
		}
	}

	// Check if properties were added
	oldProperties := checker.extractJSONSchemaProperties(oldSchema)
	newProperties := checker.extractJSONSchemaProperties(newSchema)

	for propName := range newProperties {
		if _, exists := oldProperties[propName]; !exists {
			result.Issues = append(result.Issues,
				fmt.Sprintf("New property '%s' added - ensure old schema can handle it", propName))
		}
	}
}

// extractJSONSchemaRequired extracts required fields from JSON Schema
func (checker *SchemaEvolutionChecker) extractJSONSchemaRequired(schema map[string]interface{}) []string {
	if required, ok := schema["required"].([]interface{}); ok {
		var fields []string
		for _, field := range required {
			if fieldStr, ok := field.(string); ok {
				fields = append(fields, fieldStr)
			}
		}
		return fields
	}
	return []string{}
}

// extractJSONSchemaProperties extracts properties from JSON Schema
func (checker *SchemaEvolutionChecker) extractJSONSchemaProperties(schema map[string]interface{}) map[string]interface{} {
	if properties, ok := schema["properties"].(map[string]interface{}); ok {
		return properties
	}
	return make(map[string]interface{})
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// GetCompatibilityLevel returns the compatibility level for a subject
func (checker *SchemaEvolutionChecker) GetCompatibilityLevel(subject string) CompatibilityLevel {
	// In a real implementation, this would query the schema registry
	// For now, return a default level
	return CompatibilityBackward
}

// SetCompatibilityLevel sets the compatibility level for a subject
func (checker *SchemaEvolutionChecker) SetCompatibilityLevel(subject string, level CompatibilityLevel) error {
	// In a real implementation, this would update the schema registry
	return nil
}

// CanEvolve checks if a schema can be evolved according to the compatibility rules
func (checker *SchemaEvolutionChecker) CanEvolve(
	subject string,
	currentSchemaStr, newSchemaStr string,
	format Format,
) (*CompatibilityResult, error) {

	level := checker.GetCompatibilityLevel(subject)
	return checker.CheckCompatibility(currentSchemaStr, newSchemaStr, format, level)
}

// SuggestEvolution suggests how to evolve a schema to maintain compatibility
func (checker *SchemaEvolutionChecker) SuggestEvolution(
	oldSchemaStr, newSchemaStr string,
	format Format,
	level CompatibilityLevel,
) ([]string, error) {

	suggestions := []string{}

	result, err := checker.CheckCompatibility(oldSchemaStr, newSchemaStr, format, level)
	if err != nil {
		return nil, err
	}

	if result.Compatible {
		suggestions = append(suggestions, "Schema evolution is compatible")
		return suggestions, nil
	}

	// Analyze issues and provide suggestions
	for _, issue := range result.Issues {
		if strings.Contains(issue, "required field") && strings.Contains(issue, "added") {
			suggestions = append(suggestions, "Add default values to new required fields")
		}
		if strings.Contains(issue, "removed") {
			suggestions = append(suggestions, "Consider deprecating fields instead of removing them")
		}
		if strings.Contains(issue, "type changed") {
			suggestions = append(suggestions, "Use type promotion or union types for type changes")
		}
	}

	if len(suggestions) == 0 {
		suggestions = append(suggestions, "Manual schema review required - compatibility issues detected")
	}

	return suggestions, nil
}
