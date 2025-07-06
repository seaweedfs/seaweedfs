package balance

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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

// Register registers the balance task with the given registry
func Register(registry *tasks.TaskRegistry) {
	factory := NewFactory()
	registry.Register(types.TaskTypeBalance, factory)
	glog.V(1).Infof("Registered balance task type")
}

// RegisterSimple registers the balance detector and scheduler with the registry
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewBalanceDetector()
	scheduler := NewBalanceScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("Registered balance task")
}

// Auto-register this task when the package is imported
func init() {
	factory := NewFactory()
	tasks.AutoRegister(types.TaskTypeBalance, factory)

	// Also register with types registry
	tasks.AutoRegisterTypes(RegisterSimple)
}
