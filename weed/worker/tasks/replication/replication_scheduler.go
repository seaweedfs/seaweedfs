package replication

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// SimpleScheduler implements replication task scheduling
type SimpleScheduler struct {
	maxConcurrent int
	enabled       bool
}

// NewSimpleScheduler creates a new replication scheduler
func NewSimpleScheduler() *SimpleScheduler {
	return &SimpleScheduler{
		maxConcurrent: 2,
		enabled:       true,
	}
}

// GetTaskType returns the task type
func (s *SimpleScheduler) GetTaskType() types.TaskType {
	return types.TaskTypeFixReplication
}

// CanScheduleNow determines if a replication task can be scheduled now
func (s *SimpleScheduler) CanScheduleNow(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker) bool {
	if !s.enabled {
		return false
	}

	// Check if we have available workers
	if len(availableWorkers) == 0 {
		return false
	}

	// Count running replication tasks
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeFixReplication {
			runningCount++
		}
	}

	// Check concurrency limit
	if runningCount >= s.maxConcurrent {
		glog.V(3).Infof("Replication scheduler: at concurrency limit (%d/%d)", runningCount, s.maxConcurrent)
		return false
	}

	// Check if any worker can handle replication tasks
	for _, worker := range availableWorkers {
		for _, capability := range worker.Capabilities {
			if capability == types.TaskTypeFixReplication {
				glog.V(3).Infof("Replication scheduler: can schedule task for volume %d", task.VolumeID)
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

// GetPriority returns the priority for this task
func (s *SimpleScheduler) GetPriority(task *types.Task) types.TaskPriority {
	// Check if this is a critical under-replication case
	if task.Parameters != nil {
		if actualReplicas, ok := task.Parameters["actual_replicas"].(int); ok {
			if expectedReplicas, ok := task.Parameters["expected_replicas"].(int); ok {
				if actualReplicas == 1 && expectedReplicas > 1 {
					return types.TaskPriorityHigh // Single point of failure
				}
				if actualReplicas < expectedReplicas {
					return types.TaskPriorityHigh // Under-replicated
				}
			}
		}
	}
	return types.TaskPriorityNormal // Over-replicated or normal case
}

// WasTaskRecentlyCompleted checks if a similar task was recently completed
func (s *SimpleScheduler) WasTaskRecentlyCompleted(task *types.Task, completedTasks []*types.Task, now time.Time) bool {
	// Allow replication fix retry sooner (2 hours)
	interval := 2 * time.Hour
	cutoff := now.Add(-interval)

	for _, completedTask := range completedTasks {
		if completedTask.Type == types.TaskTypeFixReplication &&
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
