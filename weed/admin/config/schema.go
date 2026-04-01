package config

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// ConfigWithDefaults defines an interface for configurations that can apply their own defaults
type ConfigWithDefaults interface {
	// ApplySchemaDefaults applies default values using the provided schema
	ApplySchemaDefaults(schema *Schema) error
	// Validate validates the configuration
	Validate() error
}

// FieldType defines the type of a configuration field
type FieldType string

const (
	FieldTypeBool     FieldType = "bool"
	FieldTypeInt      FieldType = "int"
	FieldTypeDuration FieldType = "duration"
	FieldTypeInterval FieldType = "interval"
	FieldTypeString   FieldType = "string"
	FieldTypeFloat    FieldType = "float"
)

// FieldUnit defines the unit for display purposes
type FieldUnit string

const (
	UnitSeconds FieldUnit = "seconds"
	UnitMinutes FieldUnit = "minutes"
	UnitHours   FieldUnit = "hours"
	UnitDays    FieldUnit = "days"
	UnitCount   FieldUnit = "count"
	UnitNone    FieldUnit = ""
)

// Field defines a configuration field with all its metadata
type Field struct {
	// Field identification
	Name     string    `json:"name"`
	JSONName string    `json:"json_name"`
	Type     FieldType `json:"type"`

	// Default value and validation
	DefaultValue interface{} `json:"default_value"`
	MinValue     interface{} `json:"min_value,omitempty"`
	MaxValue     interface{} `json:"max_value,omitempty"`
	Required     bool        `json:"required"`

	// UI display
	DisplayName string    `json:"display_name"`
	Description string    `json:"description"`
	HelpText    string    `json:"help_text"`
	Placeholder string    `json:"placeholder"`
	Unit        FieldUnit `json:"unit"`

	// Form rendering
	InputType  string `json:"input_type"` // "checkbox", "number", "text", "interval", etc.
	CSSClasses string `json:"css_classes,omitempty"`
}

// GetDisplayValue returns the value formatted for display in the specified unit
func (f *Field) GetDisplayValue(value interface{}) interface{} {
	if (f.Type == FieldTypeDuration || f.Type == FieldTypeInterval) && f.Unit != UnitSeconds {
		if duration, ok := value.(time.Duration); ok {
			switch f.Unit {
			case UnitMinutes:
				return int(duration.Minutes())
			case UnitHours:
				return int(duration.Hours())
			case UnitDays:
				return int(duration.Hours() / 24)
			}
		}
		if seconds, ok := value.(int); ok {
			switch f.Unit {
			case UnitMinutes:
				return seconds / 60
			case UnitHours:
				return seconds / 3600
			case UnitDays:
				return seconds / (24 * 3600)
			}
		}
	}
	return value
}

// GetIntervalDisplayValue returns the value and unit for interval fields
func (f *Field) GetIntervalDisplayValue(value interface{}) (int, string) {
	if f.Type != FieldTypeInterval {
		return 0, "minutes"
	}

	seconds := 0
	if duration, ok := value.(time.Duration); ok {
		seconds = int(duration.Seconds())
	} else if s, ok := value.(int); ok {
		seconds = s
	}

	return SecondsToIntervalValueUnit(seconds)
}

// SecondsToIntervalValueUnit converts seconds to the most appropriate interval unit
func SecondsToIntervalValueUnit(totalSeconds int) (int, string) {
	if totalSeconds == 0 {
		return 0, "minutes"
	}

	// Preserve seconds when not divisible by minutes
	if totalSeconds < 60 || totalSeconds%60 != 0 {
		return totalSeconds, "seconds"
	}

	// Check if it's evenly divisible by days
	if totalSeconds%(24*3600) == 0 {
		return totalSeconds / (24 * 3600), "days"
	}

	// Check if it's evenly divisible by hours
	if totalSeconds%3600 == 0 {
		return totalSeconds / 3600, "hours"
	}

	// Default to minutes
	return totalSeconds / 60, "minutes"
}

// IntervalValueUnitToSeconds converts interval value and unit to seconds
func IntervalValueUnitToSeconds(value int, unit string) int {
	switch unit {
	case "days":
		return value * 24 * 3600
	case "hours":
		return value * 3600
	case "minutes":
		return value * 60
	case "seconds":
		return value
	default:
		return value * 60 // Default to minutes
	}
}

// ParseDisplayValue converts a display value back to the storage format
func (f *Field) ParseDisplayValue(displayValue interface{}) interface{} {
	if (f.Type == FieldTypeDuration || f.Type == FieldTypeInterval) && f.Unit != UnitSeconds {
		if val, ok := displayValue.(int); ok {
			switch f.Unit {
			case UnitMinutes:
				return val * 60
			case UnitHours:
				return val * 3600
			case UnitDays:
				return val * 24 * 3600
			}
		}
	}
	return displayValue
}

// ParseIntervalFormData parses form data for interval fields (value + unit)
func (f *Field) ParseIntervalFormData(valueStr, unitStr string) (int, error) {
	if f.Type != FieldTypeInterval {
		return 0, fmt.Errorf("field %s is not an interval field", f.Name)
	}

	value := 0
	if valueStr != "" {
		var err error
		value, err = fmt.Sscanf(valueStr, "%d", &value)
		if err != nil {
			return 0, fmt.Errorf("invalid interval value: %s", valueStr)
		}
	}

	return IntervalValueUnitToSeconds(value, unitStr), nil
}

// ValidateValue validates a value against the field constraints
func (f *Field) ValidateValue(value interface{}) error {
	if f.Required && (value == nil || value == "" || value == 0) {
		return fmt.Errorf("%s is required", f.DisplayName)
	}

	if f.MinValue != nil {
		if !f.compareValues(value, f.MinValue, ">=") {
			return fmt.Errorf("%s must be >= %v", f.DisplayName, f.MinValue)
		}
	}

	if f.MaxValue != nil {
		if !f.compareValues(value, f.MaxValue, "<=") {
			return fmt.Errorf("%s must be <= %v", f.DisplayName, f.MaxValue)
		}
	}

	return nil
}

// compareValues compares two values based on the operator
func (f *Field) compareValues(a, b interface{}, op string) bool {
	switch f.Type {
	case FieldTypeInt:
		aVal, aOk := a.(int)
		bVal, bOk := b.(int)
		if !aOk || !bOk {
			return false
		}
		switch op {
		case ">=":
			return aVal >= bVal
		case "<=":
			return aVal <= bVal
		}
	case FieldTypeFloat:
		aVal, aOk := a.(float64)
		bVal, bOk := b.(float64)
		if !aOk || !bOk {
			return false
		}
		switch op {
		case ">=":
			return aVal >= bVal
		case "<=":
			return aVal <= bVal
		}
	}
	return true
}

// Schema provides common functionality for configuration schemas
type Schema struct {
	Fields []*Field `json:"fields"`
}

// GetFieldByName returns a field by its JSON name
func (s *Schema) GetFieldByName(jsonName string) *Field {
	for _, field := range s.Fields {
		if field.JSONName == jsonName {
			return field
		}
	}
	return nil
}

// ApplyDefaultsToConfig applies defaults to a configuration that implements ConfigWithDefaults
func (s *Schema) ApplyDefaultsToConfig(config ConfigWithDefaults) error {
	return config.ApplySchemaDefaults(s)
}

// ApplyDefaultsToProtobuf applies defaults to protobuf types using reflection
func (s *Schema) ApplyDefaultsToProtobuf(config interface{}) error {
	return s.applyDefaultsReflection(config)
}

// applyDefaultsReflection applies default values using reflection (internal use only)
// Used for protobuf types and embedded struct handling
func (s *Schema) applyDefaultsReflection(config interface{}) error {
	configValue := reflect.ValueOf(config)
	if configValue.Kind() == reflect.Ptr {
		configValue = configValue.Elem()
	}

	if configValue.Kind() != reflect.Struct {
		return fmt.Errorf("config must be a struct or pointer to struct")
	}

	configType := configValue.Type()

	for i := 0; i < configValue.NumField(); i++ {
		field := configValue.Field(i)
		fieldType := configType.Field(i)

		// Handle embedded structs recursively (before JSON tag check)
		if field.Kind() == reflect.Struct && fieldType.Anonymous {
			if !field.CanAddr() {
				return fmt.Errorf("embedded struct %s is not addressable - config must be a pointer", fieldType.Name)
			}
			err := s.applyDefaultsReflection(field.Addr().Interface())
			if err != nil {
				return fmt.Errorf("failed to apply defaults to embedded struct %s: %v", fieldType.Name, err)
			}
			continue
		}

		// Get JSON tag name
		jsonTag := fieldType.Tag.Get("json")
		if jsonTag == "" {
			continue
		}

		// Remove options like ",omitempty"
		if commaIdx := strings.Index(jsonTag, ","); commaIdx >= 0 {
			jsonTag = jsonTag[:commaIdx]
		}

		// Find corresponding schema field
		schemaField := s.GetFieldByName(jsonTag)
		if schemaField == nil {
			continue
		}

		// Apply default if field is zero value
		if field.CanSet() && field.IsZero() {
			defaultValue := reflect.ValueOf(schemaField.DefaultValue)
			if defaultValue.Type().ConvertibleTo(field.Type()) {
				field.Set(defaultValue.Convert(field.Type()))
			}
		}
	}

	return nil
}

// ValidateConfig validates a configuration against the schema
func (s *Schema) ValidateConfig(config interface{}) []error {
	var errors []error

	configValue := reflect.ValueOf(config)
	if configValue.Kind() == reflect.Ptr {
		configValue = configValue.Elem()
	}

	if configValue.Kind() != reflect.Struct {
		errors = append(errors, fmt.Errorf("config must be a struct or pointer to struct"))
		return errors
	}

	configType := configValue.Type()

	for i := 0; i < configValue.NumField(); i++ {
		field := configValue.Field(i)
		fieldType := configType.Field(i)

		// Get JSON tag name
		jsonTag := fieldType.Tag.Get("json")
		if jsonTag == "" {
			continue
		}

		// Remove options like ",omitempty"
		if commaIdx := strings.Index(jsonTag, ","); commaIdx > 0 {
			jsonTag = jsonTag[:commaIdx]
		}

		// Find corresponding schema field
		schemaField := s.GetFieldByName(jsonTag)
		if schemaField == nil {
			continue
		}

		// Validate field value
		fieldValue := field.Interface()
		if err := schemaField.ValidateValue(fieldValue); err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}
