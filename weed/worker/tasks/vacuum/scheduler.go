package vacuum

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// SimpleScheduler implements vacuum task scheduling using code instead of schemas
type SimpleScheduler struct {
	maxConcurrent int
	minInterval   time.Duration
}

// NewSimpleScheduler creates a new simple vacuum scheduler
func NewSimpleScheduler() *SimpleScheduler {
	return &SimpleScheduler{
		maxConcurrent: 2,
		minInterval:   6 * time.Hour,
	}
}

// GetTaskType returns the task type
func (s *SimpleScheduler) GetTaskType() types.TaskType {
	return types.TaskTypeVacuum
}

// CanScheduleNow determines if a vacuum task can be scheduled right now
func (s *SimpleScheduler) CanScheduleNow(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker) bool {
	// Check concurrent limit
	runningVacuumCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeVacuum {
			runningVacuumCount++
		}
	}

	if runningVacuumCount >= s.maxConcurrent {
		return false
	}

	// Check if there's an available worker with vacuum capability
	for _, worker := range availableWorkers {
		if worker.CurrentLoad < worker.MaxConcurrent {
			for _, capability := range worker.Capabilities {
				if capability == types.TaskTypeVacuum {
					return true
				}
			}
		}
	}

	return false
}

// GetPriority returns the priority for this task
func (s *SimpleScheduler) GetPriority(task *types.Task) types.TaskPriority {
	// Could adjust priority based on task parameters
	if params, ok := task.Parameters["garbage_ratio"].(float64); ok {
		if params > 0.8 {
			return types.TaskPriorityHigh
		}
	}
	return task.Priority
}

// GetMaxConcurrent returns max concurrent tasks of this type
func (s *SimpleScheduler) GetMaxConcurrent() int {
	return s.maxConcurrent
}

// Configuration setters

// SetMaxConcurrent sets the maximum number of concurrent vacuum tasks
func (s *SimpleScheduler) SetMaxConcurrent(max int) {
	s.maxConcurrent = max
}

// SetMinInterval sets the minimum interval between vacuum operations on the same volume
func (s *SimpleScheduler) SetMinInterval(interval time.Duration) {
	s.minInterval = interval
}
