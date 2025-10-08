package tasks

import (
	"reflect"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BaseUIProvider provides common UIProvider functionality for all tasks
type BaseUIProvider struct {
	taskType            types.TaskType
	displayName         string
	description         string
	icon                string
	schemaFunc          func() *TaskConfigSchema
	configFunc          func() types.TaskConfig
	applyTaskPolicyFunc func(policy *worker_pb.TaskPolicy) error
	applyTaskConfigFunc func(config types.TaskConfig) error
}

// NewBaseUIProvider creates a new base UI provider
func NewBaseUIProvider(
	taskType types.TaskType,
	displayName string,
	description string,
	icon string,
	schemaFunc func() *TaskConfigSchema,
	configFunc func() types.TaskConfig,
	applyTaskPolicyFunc func(policy *worker_pb.TaskPolicy) error,
	applyTaskConfigFunc func(config types.TaskConfig) error,
) *BaseUIProvider {
	return &BaseUIProvider{
		taskType:            taskType,
		displayName:         displayName,
		description:         description,
		icon:                icon,
		schemaFunc:          schemaFunc,
		configFunc:          configFunc,
		applyTaskPolicyFunc: applyTaskPolicyFunc,
		applyTaskConfigFunc: applyTaskConfigFunc,
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

// GetCurrentConfig returns the current configuration as TaskConfig
func (ui *BaseUIProvider) GetCurrentConfig() types.TaskConfig {
	return ui.configFunc()
}

// ApplyTaskPolicy applies protobuf TaskPolicy configuration
func (ui *BaseUIProvider) ApplyTaskPolicy(policy *worker_pb.TaskPolicy) error {
	return ui.applyTaskPolicyFunc(policy)
}

// ApplyTaskConfig applies TaskConfig interface configuration
func (ui *BaseUIProvider) ApplyTaskConfig(config types.TaskConfig) error {
	return ui.applyTaskConfigFunc(config)
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
		if !sourceField.IsZero() {
			if destField.Type() == sourceField.Type() {
				destField.Set(sourceField)
			}
		}
	}
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
	configFunc func() types.TaskConfig,
	applyTaskPolicyFunc func(policy *worker_pb.TaskPolicy) error,
	applyTaskConfigFunc func(config types.TaskConfig) error,
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
		applyTaskPolicyFunc,
		applyTaskConfigFunc,
	)

	uiRegistry.RegisterUI(uiProvider)
	glog.V(1).Infof("Registered %s task UI provider", taskType)
}
