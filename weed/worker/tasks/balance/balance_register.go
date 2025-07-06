package balance

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Factory creates balance task instances
type Factory struct {
	*tasks.BaseTaskFactory
}

// NewFactory creates a new balance task factory
func NewFactory() *Factory {
	return &Factory{
		BaseTaskFactory: tasks.NewBaseTaskFactory(
			types.TaskTypeBalance,
			[]string{"balance", "storage", "optimization"},
			"Balance data across volume servers for optimal performance",
		),
	}
}

// Create creates a new balance task instance
func (f *Factory) Create(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	task := NewTask(params.Server, params.VolumeID, params.Collection)
	task.SetEstimatedDuration(task.EstimateTime(params))

	return task, nil
}

// Shared detector and scheduler instances
var (
	sharedDetector  *BalanceDetector
	sharedScheduler *BalanceScheduler
)

// getSharedInstances returns the shared detector and scheduler instances
func getSharedInstances() (*BalanceDetector, *BalanceScheduler) {
	if sharedDetector == nil {
		sharedDetector = NewBalanceDetector()
	}
	if sharedScheduler == nil {
		sharedScheduler = NewBalanceScheduler()
	}
	return sharedDetector, sharedScheduler
}

// GetSharedInstances returns the shared detector and scheduler instances (public access)
func GetSharedInstances() (*BalanceDetector, *BalanceScheduler) {
	return getSharedInstances()
}

// Auto-register this task when the package is imported
func init() {
	factory := NewFactory()
	tasks.AutoRegister(types.TaskTypeBalance, factory)

	// Get shared instances for all registrations
	detector, scheduler := getSharedInstances()

	// Register with types registry
	tasks.AutoRegisterTypes(func(registry *types.TaskRegistry) {
		registry.RegisterTask(detector, scheduler)
	})

	// Register with UI registry using the same instances
	tasks.AutoRegisterUI(func(uiRegistry *types.UIRegistry) {
		RegisterUI(uiRegistry, detector, scheduler)
	})
}
