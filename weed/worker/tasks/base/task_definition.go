package base

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TaskDefinition encapsulates everything needed to define a complete task type
type TaskDefinition struct {
	// Basic task information
	Type         types.TaskType
	Name         string
	DisplayName  string
	Description  string
	Icon         string
	Capabilities []string

	// Task configuration
	Config     TaskConfig
	ConfigSpec ConfigSpec

	// Task creation
	CreateTask func(params *worker_pb.TaskParams) (types.Task, error)

	// Detection logic
	DetectionFunc func(metrics []*types.VolumeHealthMetrics, info *types.ClusterInfo, config TaskConfig) ([]*types.TaskDetectionResult, error)
	ScanInterval  time.Duration

	// Scheduling logic
	SchedulingFunc func(task *types.TaskInput, running []*types.TaskInput, workers []*types.WorkerData, config TaskConfig) bool
	MaxConcurrent  int
	RepeatInterval time.Duration
}

// TaskConfig provides a configuration interface that supports type-safe defaults
type TaskConfig interface {
	config.ConfigWithDefaults // Extends ConfigWithDefaults for type-safe schema operations
	IsEnabled() bool
	SetEnabled(bool)
	ToTaskPolicy() *worker_pb.TaskPolicy
	FromTaskPolicy(policy *worker_pb.TaskPolicy) error
}

// ConfigSpec defines the configuration schema
type ConfigSpec struct {
	Fields []*config.Field
}

// BaseConfig provides common configuration fields with reflection-based serialization
type BaseConfig struct {
	Enabled             bool `json:"enabled"`
	ScanIntervalSeconds int  `json:"scan_interval_seconds"`
	MaxConcurrent       int  `json:"max_concurrent"`
}

// IsEnabled returns whether the task is enabled
func (c *BaseConfig) IsEnabled() bool {
	return c.Enabled
}

// SetEnabled sets whether the task is enabled
func (c *BaseConfig) SetEnabled(enabled bool) {
	c.Enabled = enabled
}

// Validate validates the base configuration
func (c *BaseConfig) Validate() error {
	// Common validation logic
	return nil
}

// StructToMap converts any struct to a map using reflection
func StructToMap(obj interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	val := reflect.ValueOf(obj)

	// Handle pointer to struct
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return result
	}

	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Skip unexported fields
		if !field.CanInterface() {
			continue
		}

		// Handle embedded structs recursively (before JSON tag check)
		if field.Kind() == reflect.Struct && fieldType.Anonymous {
			embeddedMap := StructToMap(field.Interface())
			for k, v := range embeddedMap {
				result[k] = v
			}
			continue
		}

		// Get JSON tag name
		jsonTag := fieldType.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}

		// Remove options like ",omitempty"
		if commaIdx := strings.Index(jsonTag, ","); commaIdx >= 0 {
			jsonTag = jsonTag[:commaIdx]
		}

		result[jsonTag] = field.Interface()
	}
	return result
}

// MapToStruct loads data from map into struct using reflection
func MapToStruct(data map[string]interface{}, obj interface{}) error {
	val := reflect.ValueOf(obj)

	// Must be pointer to struct
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("obj must be pointer to struct")
	}

	val = val.Elem()
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Skip unexported fields
		if !field.CanSet() {
			continue
		}

		// Handle embedded structs recursively (before JSON tag check)
		if field.Kind() == reflect.Struct && fieldType.Anonymous {
			err := MapToStruct(data, field.Addr().Interface())
			if err != nil {
				return err
			}
			continue
		}

		// Get JSON tag name
		jsonTag := fieldType.Tag.Get("json")
		if jsonTag == "" || jsonTag == "-" {
			continue
		}

		// Remove options like ",omitempty"
		if commaIdx := strings.Index(jsonTag, ","); commaIdx >= 0 {
			jsonTag = jsonTag[:commaIdx]
		}

		if value, exists := data[jsonTag]; exists {
			err := setFieldValue(field, value)
			if err != nil {
				return fmt.Errorf("failed to set field %s: %v", jsonTag, err)
			}
		}
	}

	return nil
}

// ToMap converts config to map using reflection
// ToTaskPolicy converts BaseConfig to protobuf (partial implementation)
// Note: Concrete implementations should override this to include task-specific config
func (c *BaseConfig) ToTaskPolicy() *worker_pb.TaskPolicy {
	return &worker_pb.TaskPolicy{
		Enabled:               c.Enabled,
		MaxConcurrent:         int32(c.MaxConcurrent),
		RepeatIntervalSeconds: int32(c.ScanIntervalSeconds),
		CheckIntervalSeconds:  int32(c.ScanIntervalSeconds),
		// TaskConfig field should be set by concrete implementations
	}
}

// FromTaskPolicy loads BaseConfig from protobuf (partial implementation)
// Note: Concrete implementations should override this to handle task-specific config
func (c *BaseConfig) FromTaskPolicy(policy *worker_pb.TaskPolicy) error {
	if policy == nil {
		return fmt.Errorf("policy is nil")
	}
	c.Enabled = policy.Enabled
	c.MaxConcurrent = int(policy.MaxConcurrent)
	c.ScanIntervalSeconds = int(policy.RepeatIntervalSeconds)
	return nil
}

// ApplySchemaDefaults applies default values from schema using reflection
func (c *BaseConfig) ApplySchemaDefaults(schema *config.Schema) error {
	// Use reflection-based approach for BaseConfig since it needs to handle embedded structs
	return schema.ApplyDefaultsToProtobuf(c)
}

// setFieldValue sets a field value with type conversion
func setFieldValue(field reflect.Value, value interface{}) error {
	if value == nil {
		return nil
	}

	valueVal := reflect.ValueOf(value)
	fieldType := field.Type()
	valueType := valueVal.Type()

	// Direct assignment if types match
	if valueType.AssignableTo(fieldType) {
		field.Set(valueVal)
		return nil
	}

	// Type conversion for common cases
	switch fieldType.Kind() {
	case reflect.Bool:
		if b, ok := value.(bool); ok {
			field.SetBool(b)
		} else {
			return fmt.Errorf("cannot convert %T to bool", value)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := value.(type) {
		case int:
			field.SetInt(int64(v))
		case int32:
			field.SetInt(int64(v))
		case int64:
			field.SetInt(v)
		case float64:
			field.SetInt(int64(v))
		default:
			return fmt.Errorf("cannot convert %T to int", value)
		}
	case reflect.Float32, reflect.Float64:
		switch v := value.(type) {
		case float32:
			field.SetFloat(float64(v))
		case float64:
			field.SetFloat(v)
		case int:
			field.SetFloat(float64(v))
		case int64:
			field.SetFloat(float64(v))
		default:
			return fmt.Errorf("cannot convert %T to float", value)
		}
	case reflect.String:
		if s, ok := value.(string); ok {
			field.SetString(s)
		} else {
			return fmt.Errorf("cannot convert %T to string", value)
		}
	default:
		return fmt.Errorf("unsupported field type %s", fieldType.Kind())
	}

	return nil
}
