package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// VacuumTask implements vacuum operation task
type VacuumTask struct {
	*BaseTask
	server   string
	volumeID uint32
}

// NewVacuumTask creates a new vacuum task
func NewVacuumTask(server string, volumeID uint32) *VacuumTask {
	return &VacuumTask{
		BaseTask: NewBaseTask(types.TaskTypeVacuum),
		server:   server,
		volumeID: volumeID,
	}
}

// Execute executes the vacuum task
func (t *VacuumTask) Execute(params types.TaskParams) error {
	glog.Infof("Starting vacuum task for volume %d on server %s", t.volumeID, t.server)

	// Use the base task execution wrapper
	return t.ExecuteTask(context.Background(), params, t.executeVacuum)
}

// executeVacuum performs the actual vacuum operation
func (t *VacuumTask) executeVacuum(ctx context.Context, params types.TaskParams) error {
	// Validate parameters
	if err := ValidateParams(params, "volume_id", "server"); err != nil {
		return err
	}

	// Update task with parameters
	t.volumeID = params.VolumeID
	t.server = params.Server

	// Simulate vacuum operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Analyzing volume", 2 * time.Second, 10},
		{"Identifying garbage", 3 * time.Second, 30},
		{"Compacting data", 8 * time.Second, 70},
		{"Updating metadata", 2 * time.Second, 90},
		{"Finalizing", 1 * time.Second, 100},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			return fmt.Errorf("task cancelled during %s", step.name)
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		glog.V(2).Infof("Vacuum task %s: %s", t.Type(), step.name)
		time.Sleep(step.duration)
		t.SetProgress(step.progress)
	}

	glog.Infof("Vacuum task completed for volume %d on server %s", t.volumeID, t.server)
	return nil
}

// Validate validates the task parameters
func (t *VacuumTask) Validate(params types.TaskParams) error {
	return ValidateParams(params, "volume_id", "server")
}

// EstimateTime estimates the time needed for the task
func (t *VacuumTask) EstimateTime(params types.TaskParams) time.Duration {
	// Base time for vacuum operation
	baseTime := 15 * time.Second

	// Could adjust based on volume size or other factors
	// For now, return base time
	return baseTime
}

// VacuumTaskFactory creates vacuum task instances
type VacuumTaskFactory struct {
	*BaseTaskFactory
}

// NewVacuumTaskFactory creates a new vacuum task factory
func NewVacuumTaskFactory() *VacuumTaskFactory {
	return &VacuumTaskFactory{
		BaseTaskFactory: NewBaseTaskFactory(
			types.TaskTypeVacuum,
			[]string{"vacuum", "storage"},
			"Vacuum operation to reclaim disk space by removing deleted files",
		),
	}
}

// Create creates a new vacuum task instance
func (f *VacuumTaskFactory) Create(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if err := ValidateParams(params, "volume_id", "server"); err != nil {
		return nil, err
	}

	task := NewVacuumTask(params.Server, params.VolumeID)
	task.SetEstimatedDuration(task.EstimateTime(params))

	return task, nil
}

// RegisterVacuumTask registers the vacuum task with a registry
func RegisterVacuumTask(registry *TaskRegistry) {
	factory := NewVacuumTaskFactory()
	registry.Register(types.TaskTypeVacuum, factory)
}
