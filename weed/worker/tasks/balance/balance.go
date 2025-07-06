package balance

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Task implements balance operation to redistribute volumes across volume servers
type Task struct {
	*tasks.BaseTask
	server     string
	volumeID   uint32
	collection string
}

// NewTask creates a new balance task instance
func NewTask(server string, volumeID uint32, collection string) *Task {
	task := &Task{
		BaseTask:   tasks.NewBaseTask(types.TaskTypeBalance),
		server:     server,
		volumeID:   volumeID,
		collection: collection,
	}
	return task
}

// Execute executes the balance task
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("Starting balance task for volume %d on server %s (collection: %s)", t.volumeID, t.server, t.collection)

	// Simulate balance operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Analyzing cluster state", 2 * time.Second, 15},
		{"Identifying optimal placement", 3 * time.Second, 35},
		{"Moving volume data", 6 * time.Second, 75},
		{"Updating cluster metadata", 2 * time.Second, 95},
		{"Verifying balance", 1 * time.Second, 100},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			return fmt.Errorf("balance task cancelled")
		}

		glog.V(1).Infof("Balance task step: %s", step.name)
		t.SetProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.Infof("Balance task completed for volume %d on server %s", t.volumeID, t.server)
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
	// Base time for balance operation
	baseTime := 35 * time.Second

	// Could adjust based on volume size or cluster state
	return baseTime
}
