package remote_upload

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Factory creates remote upload task instances
type Factory struct {
	*tasks.BaseTaskFactory
}

// NewFactory creates a new remote upload task factory
func NewFactory() *Factory {
	return &Factory{
		BaseTaskFactory: tasks.NewBaseTaskFactory(
			types.TaskTypeRemoteUpload,
			[]string{"remote_upload", "storage", "backup"},
			"Upload volumes to remote storage for backup and disaster recovery",
		),
	}
}

// Create creates a new remote upload task instance
func (f *Factory) Create(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	// Get remote path from parameters
	remotePath := "/remote/backup" // Default path
	if params.Parameters != nil {
		if path, ok := params.Parameters["remote_path"].(string); ok {
			remotePath = path
		}
	}

	task := NewTask(params.Server, params.VolumeID, remotePath)
	task.SetEstimatedDuration(task.EstimateTime(params))

	return task, nil
}

// Register registers the remote upload task with the given registry
func Register(registry *tasks.TaskRegistry) {
	factory := NewFactory()
	registry.Register(types.TaskTypeRemoteUpload, factory)
	glog.V(1).Infof("Registered remote upload task type")
}

// RegisterSimple registers the remote upload detector and scheduler with simplified system
func RegisterSimple(registry *types.TaskRegistry) {
	detector := NewRemoteUploadDetector()
	scheduler := NewRemoteUploadScheduler()

	registry.RegisterTask(detector, scheduler)

	glog.V(1).Infof("Registered simplified remote upload task")
}

// Auto-register this task when the package is imported
func init() {
	factory := NewFactory()
	tasks.AutoRegister(types.TaskTypeRemoteUpload, factory)

	// Also register with types registry
	tasks.AutoRegisterTypes(RegisterSimple)
}
