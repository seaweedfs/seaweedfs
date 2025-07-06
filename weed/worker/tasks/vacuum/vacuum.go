package vacuum

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Task implements vacuum operation to reclaim disk space
type Task struct {
	*tasks.BaseTask
	server   string
	volumeID uint32
}

// NewTask creates a new vacuum task instance
func NewTask(server string, volumeID uint32) *Task {
	task := &Task{
		BaseTask: tasks.NewBaseTask(types.TaskTypeVacuum),
		server:   server,
		volumeID: volumeID,
	}
	return task
}

// Execute executes the vacuum task
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("Starting vacuum task for volume %d on server %s", t.volumeID, t.server)

	// Simulate vacuum operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Scanning volume", 1 * time.Second, 20},
		{"Identifying deleted files", 2 * time.Second, 50},
		{"Compacting data", 3 * time.Second, 80},
		{"Finalizing vacuum", 1 * time.Second, 100},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			return fmt.Errorf("vacuum task cancelled")
		}

		glog.V(1).Infof("Vacuum task step: %s", step.name)
		t.SetProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.Infof("Vacuum task completed for volume %d on server %s", t.volumeID, t.server)
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
	// Base time for vacuum operation
	baseTime := 25 * time.Second

	// Could adjust based on volume size or usage patterns
	return baseTime
}
