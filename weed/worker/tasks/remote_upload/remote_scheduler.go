package remote_upload

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// SimpleScheduler implements remote upload task scheduling
type SimpleScheduler struct {
	maxConcurrent int
	enabled       bool
}

// NewSimpleScheduler creates a new remote upload scheduler
func NewSimpleScheduler() *SimpleScheduler {
	return &SimpleScheduler{
		maxConcurrent: 1,
		enabled:       false,
	}
}

// GetTaskType returns the task type
func (s *SimpleScheduler) GetTaskType() types.TaskType {
	return types.TaskTypeRemoteUpload
}

// CanScheduleNow determines if a remote upload task can be scheduled now
func (s *SimpleScheduler) CanScheduleNow(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker) bool {
	if !s.enabled {
		return false
	}

	// Check if we have available workers
	if len(availableWorkers) == 0 {
		return false
	}

	// Count running remote upload tasks
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeRemoteUpload {
			runningCount++
		}
	}

	// Check concurrency limit
	if runningCount >= s.maxConcurrent {
		glog.V(3).Infof("Remote upload scheduler: at concurrency limit (%d/%d)", runningCount, s.maxConcurrent)
		return false
	}

	// Check if any worker can handle remote upload tasks
	for _, worker := range availableWorkers {
		for _, capability := range worker.Capabilities {
			if capability == types.TaskTypeRemoteUpload {
				glog.V(3).Infof("Remote upload scheduler: can schedule task for volume %d", task.VolumeID)
				return true
			}
		}
	}

	return false
}

// GetMaxConcurrent returns the maximum number of concurrent tasks
func (s *SimpleScheduler) GetMaxConcurrent() int {
	return s.maxConcurrent
}

// GetDefaultRepeatInterval returns the default interval to wait before repeating remote upload tasks
func (s *SimpleScheduler) GetDefaultRepeatInterval() time.Duration {
	return 48 * time.Hour // Don't repeat remote upload for 48 hours
}

// GetPriority returns the priority for this task
func (s *SimpleScheduler) GetPriority(task *types.Task) types.TaskPriority {
	// Check if volume is read-only (higher priority)
	if task.Parameters != nil {
		if readOnly, ok := task.Parameters["read_only"].(bool); ok && readOnly {
			return types.TaskPriorityNormal
		}
	}
	return types.TaskPriorityLow
}

// WasTaskRecentlyCompleted checks if a similar task was recently completed
func (s *SimpleScheduler) WasTaskRecentlyCompleted(task *types.Task, completedTasks []*types.Task, now time.Time) bool {
	// Don't repeat remote upload for 48 hours
	interval := 48 * time.Hour
	cutoff := now.Add(-interval)

	for _, completedTask := range completedTasks {
		if completedTask.Type == types.TaskTypeRemoteUpload &&
			completedTask.VolumeID == task.VolumeID &&
			completedTask.Server == task.Server &&
			completedTask.Status == types.TaskStatusCompleted &&
			completedTask.CompletedAt != nil &&
			completedTask.CompletedAt.After(cutoff) {
			return true
		}
	}
	return false
}

// IsEnabled returns whether this task type is enabled
func (s *SimpleScheduler) IsEnabled() bool {
	return s.enabled
}

// Configuration setters

func (s *SimpleScheduler) SetEnabled(enabled bool) {
	s.enabled = enabled
}

func (s *SimpleScheduler) SetMaxConcurrent(max int) {
	s.maxConcurrent = max
}
