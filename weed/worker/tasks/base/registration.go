package base

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// GenericFactory creates task instances using a TaskDefinition
type GenericFactory struct {
	*tasks.BaseTaskFactory
	taskDef *TaskDefinition
}

// NewGenericFactory creates a generic task factory
func NewGenericFactory(taskDef *TaskDefinition) *GenericFactory {
	return &GenericFactory{
		BaseTaskFactory: tasks.NewBaseTaskFactory(
			taskDef.Type,
			taskDef.Capabilities,
			taskDef.Description,
		),
		taskDef: taskDef,
	}
}

// Create creates a task instance using the task definition
func (f *GenericFactory) Create(params *worker_pb.TaskParams) (types.Task, error) {
	if f.taskDef.CreateTask == nil {
		return nil, fmt.Errorf("no task creation function defined for %s", f.taskDef.Type)
	}
	return f.taskDef.CreateTask(params)
}

// Type returns the task type
func (f *GenericFactory) Type() string {
	return string(f.taskDef.Type)
}

// Description returns a description of what this task does
func (f *GenericFactory) Description() string {
	return f.taskDef.Description
}

// Capabilities returns the task capabilities
func (f *GenericFactory) Capabilities() []string {
	return f.taskDef.Capabilities
}

// GenericSchemaProvider provides config schema from TaskDefinition
type GenericSchemaProvider struct {
	taskDef *TaskDefinition
}

// GetConfigSchema returns the schema from task definition
func (p *GenericSchemaProvider) GetConfigSchema() *tasks.TaskConfigSchema {
	return &tasks.TaskConfigSchema{
		TaskName:    string(p.taskDef.Type),
		DisplayName: p.taskDef.DisplayName,
		Description: p.taskDef.Description,
		Icon:        p.taskDef.Icon,
		Schema: config.Schema{
			Fields: p.taskDef.ConfigSpec.Fields,
		},
	}
}

// GenericUIProvider provides UI functionality from TaskDefinition
type GenericUIProvider struct {
	taskDef *TaskDefinition
}

// GetTaskType returns the task type
func (ui *GenericUIProvider) GetTaskType() types.TaskType {
	return ui.taskDef.Type
}

// GetDisplayName returns the human-readable name
func (ui *GenericUIProvider) GetDisplayName() string {
	return ui.taskDef.DisplayName
}

// GetDescription returns a description of what this task does
func (ui *GenericUIProvider) GetDescription() string {
	return ui.taskDef.Description
}

// GetIcon returns the icon CSS class for this task type
func (ui *GenericUIProvider) GetIcon() string {
	return ui.taskDef.Icon
}

// GetCurrentConfig returns current config as TaskConfig
func (ui *GenericUIProvider) GetCurrentConfig() types.TaskConfig {
	return ui.taskDef.Config
}

// ApplyTaskPolicy applies protobuf TaskPolicy configuration
func (ui *GenericUIProvider) ApplyTaskPolicy(policy *worker_pb.TaskPolicy) error {
	return ui.taskDef.Config.FromTaskPolicy(policy)
}

// ApplyTaskConfig applies TaskConfig interface configuration
func (ui *GenericUIProvider) ApplyTaskConfig(config types.TaskConfig) error {
	taskPolicy := config.ToTaskPolicy()
	return ui.taskDef.Config.FromTaskPolicy(taskPolicy)
}

// RegisterTask registers a complete task definition with all registries
func RegisterTask(taskDef *TaskDefinition) {
	// Validate task definition
	if err := validateTaskDefinition(taskDef); err != nil {
		glog.Errorf("Invalid task definition for %s: %v", taskDef.Type, err)
		return
	}

	// Create and register factory
	factory := NewGenericFactory(taskDef)
	tasks.AutoRegister(taskDef.Type, factory)

	// Create and register detector/scheduler
	detector := NewGenericDetector(taskDef)
	scheduler := NewGenericScheduler(taskDef)

	tasks.AutoRegisterTypes(func(registry *types.TaskRegistry) {
		registry.RegisterTask(detector, scheduler)
	})

	// Create and register schema provider
	schemaProvider := &GenericSchemaProvider{taskDef: taskDef}
	tasks.RegisterTaskConfigSchema(string(taskDef.Type), schemaProvider)

	// Create and register UI provider
	uiProvider := &GenericUIProvider{taskDef: taskDef}
	tasks.AutoRegisterUI(func(uiRegistry *types.UIRegistry) {
		baseUIProvider := tasks.NewBaseUIProvider(
			taskDef.Type,
			taskDef.DisplayName,
			taskDef.Description,
			taskDef.Icon,
			schemaProvider.GetConfigSchema,
			uiProvider.GetCurrentConfig,
			uiProvider.ApplyTaskPolicy,
			uiProvider.ApplyTaskConfig,
		)
		uiRegistry.RegisterUI(baseUIProvider)
	})

	glog.V(1).Infof("Registered complete task definition: %s", taskDef.Type)
}

// validateTaskDefinition ensures the task definition is complete
func validateTaskDefinition(taskDef *TaskDefinition) error {
	if taskDef.Type == "" {
		return fmt.Errorf("task type is required")
	}
	if taskDef.Name == "" {
		return fmt.Errorf("task name is required")
	}
	if taskDef.Config == nil {
		return fmt.Errorf("task config is required")
	}
	if taskDef.CreateTask == nil {
		return fmt.Errorf("task creation function is required")
	}
	return nil
}
