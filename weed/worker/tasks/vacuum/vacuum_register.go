package vacuum

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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

// Register registers the vacuum task with the given registry
func Register(registry *tasks.TaskRegistry) {
	factory := NewFactory()
	registry.Register(types.TaskTypeVacuum, factory)
	glog.V(1).Infof("Registered vacuum task type")
}

// RegisterSimple registers the vacuum task components with the simplified registry
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewVacuumDetector()
	scheduler := NewVacuumScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("✅ Registered simplified vacuum task (detector + scheduler)")
}

// Auto-register this task when the package is imported
func init() {
	factory := NewFactory()
	tasks.AutoRegister(types.TaskTypeVacuum, factory)

	// Also register with types registry
	tasks.AutoRegisterTypes(RegisterSimple)
}
