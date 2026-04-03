package tasks

import (
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

// RegisterUIFunc provides a common registration function signature
type RegisterUIFunc[D, S any] func(uiRegistry *types.UIRegistry, detector D, scheduler S)

