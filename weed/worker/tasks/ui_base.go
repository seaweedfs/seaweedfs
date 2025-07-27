package tasks

import (
	"reflect"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BaseUIProvider provides common UIProvider functionality for all tasks
type BaseUIProvider struct {
	taskType    types.TaskType
	displayName string
	description string
	icon        string
	schemaFunc  func() *TaskConfigSchema
	configFunc  func() interface{}
	applyFunc   func(config interface{}) error
}

// NewBaseUIProvider creates a new base UI provider
func NewBaseUIProvider(
	taskType types.TaskType,
	displayName string,
	description string,
	icon string,
	schemaFunc func() *TaskConfigSchema,
	configFunc func() interface{},
	applyFunc func(config interface{}) error,
) *BaseUIProvider {
	return &BaseUIProvider{
		taskType:    taskType,
		displayName: displayName,
		description: description,
		icon:        icon,
		schemaFunc:  schemaFunc,
		configFunc:  configFunc,
		applyFunc:   applyFunc,
	}
}

// GetTaskType returns the task type
func (ui *BaseUIProvider) GetTaskType() types.TaskType {
	return ui.taskType
}

// GetDisplayName returns the human-readable name
func (ui *BaseUIProvider) GetDisplayName() string {
	return ui.displayName
}

// GetDescription returns a description of what this task does
func (ui *BaseUIProvider) GetDescription() string {
	return ui.description
}

// GetIcon returns the icon CSS class for this task type
func (ui *BaseUIProvider) GetIcon() string {
	return ui.icon
}

// GetCurrentConfig returns the current configuration
func (ui *BaseUIProvider) GetCurrentConfig() interface{} {
	return ui.configFunc()
}

// ApplyConfig applies the new configuration with common schema-based handling
func (ui *BaseUIProvider) ApplyConfig(config interface{}) error {
	// First try direct application
	if err := ui.applyFunc(config); err == nil {
		return nil
	}

	// Fallback: Try to get the configuration from the schema-based system
	schema := ui.schemaFunc()
	if schema != nil {
		// Apply defaults to ensure we have a complete config
		if err := schema.ApplyDefaults(config); err != nil {
			return err
		}

		// Try again with defaults applied
		if err := ui.applyFunc(config); err == nil {
			return nil
		}

		// Last resort: use current config
		glog.Warningf("Config type conversion failed for %s, using current config", ui.taskType)
		currentConfig := ui.GetCurrentConfig()
		return ui.applyFunc(currentConfig)
	}

	return nil
}

// CommonConfigGetter provides a common pattern for getting current configuration
type CommonConfigGetter[T any] struct {
	defaultConfig T
	detectorFunc  func() T
	schedulerFunc func() T
}

// NewCommonConfigGetter creates a new common config getter
func NewCommonConfigGetter[T any](
	defaultConfig T,
	detectorFunc func() T,
	schedulerFunc func() T,
) *CommonConfigGetter[T] {
	return &CommonConfigGetter[T]{
		defaultConfig: defaultConfig,
		detectorFunc:  detectorFunc,
		schedulerFunc: schedulerFunc,
	}
}

// GetConfig returns the merged configuration
func (cg *CommonConfigGetter[T]) GetConfig() T {
	config := cg.defaultConfig

	// Apply detector values if available
	if cg.detectorFunc != nil {
		detectorConfig := cg.detectorFunc()
		mergeConfigs(&config, detectorConfig)
	}

	// Apply scheduler values if available
	if cg.schedulerFunc != nil {
		schedulerConfig := cg.schedulerFunc()
		mergeConfigs(&config, schedulerConfig)
	}

	return config
}

// mergeConfigs merges non-zero values from source into dest
func mergeConfigs[T any](dest *T, source T) {
	destValue := reflect.ValueOf(dest).Elem()
	sourceValue := reflect.ValueOf(source)

	if destValue.Kind() != reflect.Struct || sourceValue.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < destValue.NumField(); i++ {
		destField := destValue.Field(i)
		sourceField := sourceValue.Field(i)

		if !destField.CanSet() {
			continue
		}

		// Only copy non-zero values
		if !isZeroValue(sourceField) {
			if destField.Type() == sourceField.Type() {
				destField.Set(sourceField)
			}
		}
	}
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

// RegisterUIFunc provides a common registration function signature
type RegisterUIFunc[D, S any] func(uiRegistry *types.UIRegistry, detector D, scheduler S)

// CommonRegisterUI provides a common registration implementation
func CommonRegisterUI[D, S any](
	taskType types.TaskType,
	displayName string,
	uiRegistry *types.UIRegistry,
	detector D,
	scheduler S,
	schemaFunc func() *TaskConfigSchema,
	configFunc func() interface{},
	applyFunc func(config interface{}) error,
) {
	// Get metadata from schema
	schema := schemaFunc()
	description := "Task configuration"
	icon := "fas fa-cog"

	if schema != nil {
		description = schema.Description
		icon = schema.Icon
	}

	uiProvider := NewBaseUIProvider(
		taskType,
		displayName,
		description,
		icon,
		schemaFunc,
		configFunc,
		applyFunc,
	)

	uiRegistry.RegisterUI(uiProvider)
	glog.V(1).Infof("✅ Registered %s task UI provider", taskType)
}
