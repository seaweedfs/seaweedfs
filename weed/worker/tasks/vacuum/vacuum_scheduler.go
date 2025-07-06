package vacuum

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// VacuumScheduler implements vacuum task scheduling using code instead of schemas
type VacuumScheduler struct {
	enabled       bool
	maxConcurrent int
	minInterval   time.Duration
}

// Compile-time interface assertions
var (
	_ types.TaskScheduler = (*VacuumScheduler)(nil)
)

// NewVacuumScheduler creates a new simple vacuum scheduler
func NewVacuumScheduler() *VacuumScheduler {
	return &VacuumScheduler{
		enabled:       true,
		maxConcurrent: 2,
		minInterval:   6 * time.Hour,
	}
}

// GetTaskType returns the task type
func (s *VacuumScheduler) GetTaskType() types.TaskType {
	return types.TaskTypeVacuum
}

// CanScheduleNow determines if a vacuum task can be scheduled right now
func (s *VacuumScheduler) CanScheduleNow(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker) bool {
	// Check if scheduler is enabled
	if !s.enabled {
		return false
	}

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
func (s *VacuumScheduler) GetPriority(task *types.Task) types.TaskPriority {
	// Could adjust priority based on task parameters
	if params, ok := task.Parameters["garbage_ratio"].(float64); ok {
		if params > 0.8 {
			return types.TaskPriorityHigh
		}
	}
	return task.Priority
}

// GetMaxConcurrent returns max concurrent tasks of this type
func (s *VacuumScheduler) GetMaxConcurrent() int {
	return s.maxConcurrent
}

// GetDefaultRepeatInterval returns the default interval to wait before repeating vacuum tasks
func (s *VacuumScheduler) GetDefaultRepeatInterval() time.Duration {
	return s.minInterval
}

// IsEnabled returns whether this scheduler is enabled
func (s *VacuumScheduler) IsEnabled() bool {
	return s.enabled
}

// Configuration setters

func (s *VacuumScheduler) SetEnabled(enabled bool) {
	s.enabled = enabled
}

func (s *VacuumScheduler) SetMaxConcurrent(max int) {
	s.maxConcurrent = max
}

func (s *VacuumScheduler) SetMinInterval(interval time.Duration) {
	s.minInterval = interval
}

// GetMinInterval returns the minimum interval
func (s *VacuumScheduler) GetMinInterval() time.Duration {
	return s.minInterval
}
