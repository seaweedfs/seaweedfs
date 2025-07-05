package erasure_coding

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Scheduler implements erasure coding task scheduling
type Scheduler struct {
	maxConcurrent int
	enabled       bool
}

// NewScheduler creates a new erasure coding scheduler
func NewScheduler() *Scheduler {
	return &Scheduler{
		maxConcurrent: 1,     // Conservative default
		enabled:       false, // Conservative default
	}
}

// GetTaskType returns the task type
func (s *Scheduler) GetTaskType() types.TaskType {
	return types.TaskTypeErasureCoding
}

// CanScheduleNow determines if an erasure coding task can be scheduled now
func (s *Scheduler) CanScheduleNow(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker) bool {
	if !s.enabled {
		return false
	}

	// Check if we have available workers
	if len(availableWorkers) == 0 {
		return false
	}

	// Count running EC tasks
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeErasureCoding {
			runningCount++
		}
	}

	// Check concurrency limit
	if runningCount >= s.maxConcurrent {
		glog.V(3).Infof("EC scheduler: at concurrency limit (%d/%d)", runningCount, s.maxConcurrent)
		return false
	}

	// Check if any worker can handle EC tasks
	for _, worker := range availableWorkers {
		for _, capability := range worker.Capabilities {
			if capability == types.TaskTypeErasureCoding {
				glog.V(3).Infof("EC scheduler: can schedule task for volume %d", task.VolumeID)
				return true
			}
		}
	}

	return false
}

// GetMaxConcurrent returns the maximum number of concurrent tasks
func (s *Scheduler) GetMaxConcurrent() int {
	return s.maxConcurrent
}

// GetDefaultRepeatInterval returns the default interval to wait before repeating EC tasks
func (s *Scheduler) GetDefaultRepeatInterval() time.Duration {
	return 24 * time.Hour // Don't repeat EC for 24 hours
}

// GetPriority returns the priority for this task
func (s *Scheduler) GetPriority(task *types.Task) types.TaskPriority {
	return types.TaskPriorityLow // EC is not urgent
}

// WasTaskRecentlyCompleted checks if a similar task was recently completed
func (s *Scheduler) WasTaskRecentlyCompleted(task *types.Task, completedTasks []*types.Task, now time.Time) bool {
	// Don't repeat EC for 24 hours
	interval := 24 * time.Hour
	cutoff := now.Add(-interval)

	for _, completedTask := range completedTasks {
		if completedTask.Type == types.TaskTypeErasureCoding &&
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
func (s *Scheduler) IsEnabled() bool {
	return s.enabled
}

// Configuration setters

func (s *Scheduler) SetEnabled(enabled bool) {
	s.enabled = enabled
}

func (s *Scheduler) SetMaxConcurrent(max int) {
	s.maxConcurrent = max
}
