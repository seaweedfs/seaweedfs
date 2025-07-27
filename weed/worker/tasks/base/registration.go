package base

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
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
func (f *GenericFactory) Create(params types.TaskParams) (types.TaskInterface, error) {
	if f.taskDef.CreateTask == nil {
		return nil, fmt.Errorf("no task creation function defined for %s", f.taskDef.Type)
	}
	return f.taskDef.CreateTask(params)
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

// GetCurrentConfig returns current config as interface{}
func (ui *GenericUIProvider) GetCurrentConfig() interface{} {
	return ui.taskDef.Config
}

// ApplyConfig applies configuration
func (ui *GenericUIProvider) ApplyConfig(config interface{}) error {
	if configMap, ok := config.(map[string]interface{}); ok {
		return ui.taskDef.Config.FromMap(configMap)
	}
	return fmt.Errorf("invalid config format for %s", ui.taskDef.Type)
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
			uiProvider.ApplyConfig,
		)
		uiRegistry.RegisterUI(baseUIProvider)
	})

	glog.V(1).Infof("✅ Registered complete task definition: %s", taskDef.Type)
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
