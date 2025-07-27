package maintenance

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// ConfigFieldType defines the type of a configuration field
type ConfigFieldType string

const (
	FieldTypeBool     ConfigFieldType = "bool"
	FieldTypeInt      ConfigFieldType = "int"
	FieldTypeDuration ConfigFieldType = "duration"
	FieldTypeInterval ConfigFieldType = "interval"
	FieldTypeString   ConfigFieldType = "string"
	FieldTypeFloat    ConfigFieldType = "float"
)

// ConfigFieldUnit defines the unit for display purposes
type ConfigFieldUnit string

const (
	UnitSeconds ConfigFieldUnit = "seconds"
	UnitMinutes ConfigFieldUnit = "minutes"
	UnitHours   ConfigFieldUnit = "hours"
	UnitDays    ConfigFieldUnit = "days"
	UnitCount   ConfigFieldUnit = "count"
	UnitNone    ConfigFieldUnit = ""
)

// ConfigField defines a configuration field with all its metadata
type ConfigField struct {
	// Field identification
	Name     string          `json:"name"`
	JSONName string          `json:"json_name"`
	Type     ConfigFieldType `json:"type"`

	// Default value and validation
	DefaultValue interface{} `json:"default_value"`
	MinValue     interface{} `json:"min_value,omitempty"`
	MaxValue     interface{} `json:"max_value,omitempty"`
	Required     bool        `json:"required"`

	// UI display
	DisplayName string          `json:"display_name"`
	Description string          `json:"description"`
	HelpText    string          `json:"help_text"`
	Placeholder string          `json:"placeholder"`
	Unit        ConfigFieldUnit `json:"unit"`

	// Form rendering
	InputType  string `json:"input_type"` // "checkbox", "number", "text", etc.
	CSSClasses string `json:"css_classes,omitempty"`
}

// GetDisplayValue returns the value formatted for display in the specified unit
func (cf *ConfigField) GetDisplayValue(value interface{}) interface{} {
	if (cf.Type == FieldTypeDuration || cf.Type == FieldTypeInterval) && cf.Unit != UnitSeconds {
		if duration, ok := value.(time.Duration); ok {
			switch cf.Unit {
			case UnitMinutes:
				return int(duration.Minutes())
			case UnitHours:
				return int(duration.Hours())
			case UnitDays:
				return int(duration.Hours() / 24)
			}
		}
		if seconds, ok := value.(int); ok {
			switch cf.Unit {
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
func (cf *ConfigField) GetIntervalDisplayValue(value interface{}) (int, string) {
	if cf.Type != FieldTypeInterval {
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
	default:
		return value * 60 // Default to minutes
	}
}

// ParseDisplayValue converts a display value back to the storage format
func (cf *ConfigField) ParseDisplayValue(displayValue interface{}) interface{} {
	if (cf.Type == FieldTypeDuration || cf.Type == FieldTypeInterval) && cf.Unit != UnitSeconds {
		if val, ok := displayValue.(int); ok {
			switch cf.Unit {
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
func (cf *ConfigField) ParseIntervalFormData(valueStr, unitStr string) (int, error) {
	if cf.Type != FieldTypeInterval {
		return 0, fmt.Errorf("field %s is not an interval field", cf.Name)
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
func (cf *ConfigField) ValidateValue(value interface{}) error {
	if cf.Required && (value == nil || value == "" || value == 0) {
		return fmt.Errorf("%s is required", cf.DisplayName)
	}

	if cf.MinValue != nil {
		if !cf.compareValues(value, cf.MinValue, ">=") {
			return fmt.Errorf("%s must be >= %v", cf.DisplayName, cf.MinValue)
		}
	}

	if cf.MaxValue != nil {
		if !cf.compareValues(value, cf.MaxValue, "<=") {
			return fmt.Errorf("%s must be <= %v", cf.DisplayName, cf.MaxValue)
		}
	}

	return nil
}

// compareValues compares two values based on the operator
func (cf *ConfigField) compareValues(a, b interface{}, op string) bool {
	switch cf.Type {
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

// MaintenanceConfigSchema defines the schema for maintenance configuration
type MaintenanceConfigSchema struct {
	Fields map[string]*ConfigField `json:"fields"`
}

// GetMaintenanceConfigSchema returns the schema for maintenance configuration
func GetMaintenanceConfigSchema() *MaintenanceConfigSchema {
	return &MaintenanceConfigSchema{
		Fields: map[string]*ConfigField{
			"enabled": {
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         FieldTypeBool,
				DefaultValue: false,
				Required:     false,
				DisplayName:  "Enable Maintenance System",
				Description:  "When enabled, the system will automatically scan for and execute maintenance tasks",
				HelpText:     "Toggle this to enable or disable the entire maintenance system",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			"scan_interval_seconds": {
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         FieldTypeInterval,
				DefaultValue: 30 * 60,      // 30 minutes in seconds
				MinValue:     1 * 60,       // 1 minute
				MaxValue:     24 * 60 * 60, // 24 hours
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for maintenance tasks",
				HelpText:     "The system will check for new maintenance tasks at this interval",
				Placeholder:  "30",
				Unit:         UnitMinutes,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			"worker_timeout_seconds": {
				Name:         "worker_timeout_seconds",
				JSONName:     "worker_timeout_seconds",
				Type:         FieldTypeInterval,
				DefaultValue: 5 * 60,  // 5 minutes
				MinValue:     1 * 60,  // 1 minute
				MaxValue:     60 * 60, // 1 hour
				Required:     true,
				DisplayName:  "Worker Timeout",
				Description:  "How long to wait for worker heartbeat before considering it inactive",
				HelpText:     "Workers that don't send heartbeats within this time are considered offline",
				Placeholder:  "5",
				Unit:         UnitMinutes,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			"task_timeout_seconds": {
				Name:         "task_timeout_seconds",
				JSONName:     "task_timeout_seconds",
				Type:         FieldTypeInterval,
				DefaultValue: 2 * 60 * 60,  // 2 hours
				MinValue:     1 * 60 * 60,  // 1 hour
				MaxValue:     24 * 60 * 60, // 24 hours
				Required:     true,
				DisplayName:  "Task Timeout",
				Description:  "Maximum time allowed for a single task to complete",
				HelpText:     "Tasks that run longer than this will be considered failed and may be retried",
				Placeholder:  "2",
				Unit:         UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			"retry_delay_seconds": {
				Name:         "retry_delay_seconds",
				JSONName:     "retry_delay_seconds",
				Type:         FieldTypeInterval,
				DefaultValue: 15 * 60,     // 15 minutes
				MinValue:     1 * 60,      // 1 minute
				MaxValue:     2 * 60 * 60, // 2 hours
				Required:     true,
				DisplayName:  "Retry Delay",
				Description:  "Time to wait before retrying failed tasks",
				HelpText:     "Failed tasks will wait this long before being retried",
				Placeholder:  "15",
				Unit:         UnitMinutes,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			"max_retries": {
				Name:         "max_retries",
				JSONName:     "max_retries",
				Type:         FieldTypeInt,
				DefaultValue: 3,
				MinValue:     0,
				MaxValue:     10,
				Required:     false,
				DisplayName:  "Default Max Retries",
				Description:  "Default number of times to retry failed tasks",
				HelpText:     "Tasks will be retried this many times before being marked as permanently failed",
				Placeholder:  "3 (default)",
				Unit:         UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			"cleanup_interval_seconds": {
				Name:         "cleanup_interval_seconds",
				JSONName:     "cleanup_interval_seconds",
				Type:         FieldTypeInterval,
				DefaultValue: 24 * 60 * 60,     // 24 hours
				MinValue:     1 * 60 * 60,      // 1 hour
				MaxValue:     7 * 24 * 60 * 60, // 7 days
				Required:     true,
				DisplayName:  "Cleanup Interval",
				Description:  "How often to clean up old task records",
				HelpText:     "The system will remove old completed/failed tasks at this interval",
				Placeholder:  "24",
				Unit:         UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			"task_retention_seconds": {
				Name:         "task_retention_seconds",
				JSONName:     "task_retention_seconds",
				Type:         FieldTypeInterval,
				DefaultValue: 7 * 24 * 60 * 60,  // 7 days
				MinValue:     1 * 24 * 60 * 60,  // 1 day
				MaxValue:     30 * 24 * 60 * 60, // 30 days
				Required:     true,
				DisplayName:  "Task Retention",
				Description:  "How long to keep completed/failed task records",
				HelpText:     "Task records older than this will be automatically deleted",
				Placeholder:  "7",
				Unit:         UnitDays,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			"global_max_concurrent": {
				Name:         "global_max_concurrent",
				JSONName:     "global_max_concurrent",
				Type:         FieldTypeInt,
				DefaultValue: 4,
				MinValue:     1,
				MaxValue:     20,
				Required:     true,
				DisplayName:  "Global Concurrent Limit",
				Description:  "Maximum number of maintenance tasks that can run simultaneously across all workers",
				HelpText:     "This limits the total system load from maintenance operations",
				Placeholder:  "4 (default)",
				Unit:         UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
		},
	}
}

// ApplyDefaults applies default values to a configuration struct using reflection
func (schema *MaintenanceConfigSchema) ApplyDefaults(config interface{}) error {
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
		schemaField, exists := schema.Fields[jsonTag]
		if !exists {
			continue
		}

		// Apply default if field is zero value
		if field.CanSet() && isZeroValue(field) {
			defaultValue := reflect.ValueOf(schemaField.DefaultValue)
			if defaultValue.Type().ConvertibleTo(field.Type()) {
				field.Set(defaultValue.Convert(field.Type()))
			}
		}
	}

	return nil
}

// isZeroValue checks if a reflect.Value represents a zero value
func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.String:
		return v.String() == ""
	case reflect.Slice, reflect.Map, reflect.Array:
		return v.IsNil() || v.Len() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

// ValidateConfig validates a configuration against the schema
func (schema *MaintenanceConfigSchema) ValidateConfig(config interface{}) []error {
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
		schemaField, exists := schema.Fields[jsonTag]
		if !exists {
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
