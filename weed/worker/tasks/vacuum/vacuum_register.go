package vacuum

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Factory creates vacuum task instances
type Factory struct {
	*tasks.BaseTaskFactory
}

// NewFactory creates a new vacuum task factory
func NewFactory() *Factory {
	return &Factory{
		BaseTaskFactory: tasks.NewBaseTaskFactory(
			types.TaskTypeVacuum,
			[]string{"vacuum", "storage"},
			"Vacuum operation to reclaim disk space by removing deleted files",
		),
	}
}

// Create creates a new vacuum task instance
func (f *Factory) Create(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	task := NewTask(params.Server, params.VolumeID)
	task.SetEstimatedDuration(task.EstimateTime(params))

	return task, nil
}

// Shared detector and scheduler instances
var (
	sharedDetector  *VacuumDetector
	sharedScheduler *VacuumScheduler
)

// getSharedInstances returns the shared detector and scheduler instances
func getSharedInstances() (*VacuumDetector, *VacuumScheduler) {
	if sharedDetector == nil {
		sharedDetector = NewVacuumDetector()
	}
	if sharedScheduler == nil {
		sharedScheduler = NewVacuumScheduler()
	}
	return sharedDetector, sharedScheduler
}

// GetSharedInstances returns the shared detector and scheduler instances (public access)
func GetSharedInstances() (*VacuumDetector, *VacuumScheduler) {
	return getSharedInstances()
}

// Auto-register this task when the package is imported
func init() {
	factory := NewFactory()
	tasks.AutoRegister(types.TaskTypeVacuum, factory)

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
