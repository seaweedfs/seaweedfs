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

	// Extract additional parameters for comprehensive EC
	masterClient := "localhost:9333"    // Default master client
	workDir := "/tmp/seaweedfs_ec_work" // Default work directory

	if mc, ok := params.Parameters["master_client"].(string); ok && mc != "" {
		masterClient = mc
	}
	if wd, ok := params.Parameters["work_dir"].(string); ok && wd != "" {
		workDir = wd
	}

	// Create EC task with comprehensive capabilities
	task := NewTaskWithParams(params.Server, params.VolumeID, masterClient, workDir)
	task.SetEstimatedDuration(task.EstimateTime(params))

	return task, nil
}

// getSharedInstances returns shared detector and scheduler instances
func getSharedInstances() (*EcDetector, *Scheduler) {
	// Create shared instances (singleton pattern)
	detector := NewEcDetector()
	scheduler := NewScheduler()
	return detector, scheduler
}

// GetSharedInstances returns the shared detector and scheduler instances (public API)
func GetSharedInstances() (*EcDetector, *Scheduler) {
	return getSharedInstances()
}

// Auto-register this task when the package is imported
func init() {
	factory := NewFactory()
	tasks.AutoRegister(types.TaskTypeErasureCoding, factory)

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
