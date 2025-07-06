package erasure_coding

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Factory creates erasure coding task instances
type Factory struct {
	*tasks.BaseTaskFactory
}

// NewFactory creates a new erasure coding task factory
func NewFactory() *Factory {
	return &Factory{
		BaseTaskFactory: tasks.NewBaseTaskFactory(
			types.TaskTypeErasureCoding,
			[]string{"erasure_coding", "storage", "durability"},
			"Convert volumes to erasure coded format for improved durability",
		),
	}
}

// Create creates a new erasure coding task instance
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

// Auto-register this task when the package is imported
func init() {
	factory := NewFactory()
	tasks.AutoRegister(types.TaskTypeErasureCoding, factory)

	// Also register with types registry
	tasks.AutoRegisterTypes(func(registry *types.TaskRegistry) {
		detector := NewEcDetector()
		scheduler := NewScheduler()
		registry.RegisterTask(detector, scheduler)
	})

	// Also register UI provider
	tasks.AutoRegisterUI(func(uiRegistry *types.UIRegistry) {
		detector := NewEcDetector()
		scheduler := NewScheduler()
		RegisterUI(uiRegistry, detector, scheduler)
	})
}
