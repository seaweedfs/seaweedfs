package remote_upload

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Task implements remote upload operation to copy volumes to remote storage
type Task struct {
	*tasks.BaseTask
	server     string
	volumeID   uint32
	remotePath string
}

// Compile-time interface assertions
var (
	_ types.TaskInterface = (*Task)(nil)
)

// NewTask creates a new remote upload task instance
func NewTask(server string, volumeID uint32, remotePath string) *Task {
	task := &Task{
		BaseTask:   tasks.NewBaseTask(types.TaskTypeRemoteUpload),
		server:     server,
		volumeID:   volumeID,
		remotePath: remotePath,
	}
	return task
}

// Execute executes the remote upload task
func (t *Task) Execute(params types.TaskParams) error {
	glog.Infof("Starting remote upload task for volume %d on server %s to %s", t.volumeID, t.server, t.remotePath)

	// Simulate remote upload operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Preparing upload", 1 * time.Second, 10},
		{"Uploading volume data", 8 * time.Second, 70},
		{"Verifying upload", 2 * time.Second, 90},
		{"Finalizing remote copy", 1 * time.Second, 100},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			return fmt.Errorf("remote upload task cancelled")
		}

		glog.V(1).Infof("Remote upload task step: %s", step.name)
		t.SetProgress(step.progress)

		// Simulate work
		time.Sleep(step.duration)
	}

	glog.Infof("Remote upload task completed for volume %d on server %s", t.volumeID, t.server)
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
	// Base time for remote upload operation
	baseTime := 45 * time.Second

	// Could adjust based on volume size or network speed
	return baseTime
}
