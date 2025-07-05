package replication

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Task implements replication operation to fix replication issues
type Task struct {
	*tasks.BaseTask
	server   string
	volumeID uint32
}

// NewTask creates a new replication task instance
func NewTask(server string, volumeID uint32) *Task {
	task := &Task{
		BaseTask: tasks.NewBaseTask(types.TaskTypeFixReplication),
		server:   server,
		volumeID: volumeID,
	}
	return task
}

// Execute executes the replication task
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("Starting replication task for volume %d on server %s", t.volumeID, t.server)

	// Simulate replication operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Analyzing replication status", 2 * time.Second, 20},
		{"Identifying missing replicas", 1 * time.Second, 40},
		{"Creating missing replicas", 4 * time.Second, 80},
		{"Verifying replication", 1 * time.Second, 100},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			return fmt.Errorf("replication task cancelled")
		}

		glog.V(1).Infof("Replication task step: %s", step.name)
		t.SetProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.Infof("Replication task completed for volume %d on server %s", t.volumeID, t.server)
	return nil
}

// Validate validates the task parameters
func (t *Task) Validate(params types.TaskParams) error {
	if params.VolumeID == 0 {
		return fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return fmt.Errorf("server is required")
	}
	return nil
}

// EstimateTime estimates the time needed for the task
func (t *Task) EstimateTime(params types.TaskParams) time.Duration {
	// Base time for replication operation
	baseTime := 25 * time.Second

	// Could adjust based on volume size or network conditions
	return baseTime
}

// Factory creates replication task instances
type Factory struct {
	*tasks.BaseTaskFactory
}

// NewFactory creates a new replication task factory
func NewFactory() *Factory {
	return &Factory{
		BaseTaskFactory: tasks.NewBaseTaskFactory(
			types.TaskTypeFixReplication,
			[]string{"replication", "storage", "durability"},
			"Fix replication issues to ensure proper data redundancy",
		),
	}
}

// Create creates a new replication task instance
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

// Register registers the replication task with the given registry
func Register(registry *tasks.TaskRegistry) {
	factory := NewFactory()
	registry.Register(types.TaskTypeFixReplication, factory)
	glog.V(1).Infof("Registered replication task type")
}
