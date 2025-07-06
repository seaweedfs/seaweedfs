package erasure_coding

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Task implements erasure coding operation to convert volumes to EC format
type Task struct {
	*tasks.BaseTask
	server   string
	volumeID uint32
}

// NewTask creates a new erasure coding task instance
func NewTask(server string, volumeID uint32) *Task {
	task := &Task{
		BaseTask: tasks.NewBaseTask(types.TaskTypeErasureCoding),
		server:   server,
		volumeID: volumeID,
	}
	return task
}

// Execute executes the erasure coding task
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("Starting erasure coding task for volume %d on server %s", t.volumeID, t.server)

	// Simulate erasure coding operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Analyzing volume", 2 * time.Second, 15},
		{"Creating EC shards", 5 * time.Second, 50},
		{"Verifying shards", 2 * time.Second, 75},
		{"Finalizing EC volume", 1 * time.Second, 100},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			return fmt.Errorf("erasure coding task cancelled")
		}

		glog.V(1).Infof("Erasure coding task step: %s", step.name)
		t.SetProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.Infof("Erasure coding task completed for volume %d on server %s", t.volumeID, t.server)
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
	// Base time for erasure coding operation
	baseTime := 30 * time.Second

	// Could adjust based on volume size or other factors
	return baseTime
}
