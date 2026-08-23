package base

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// GenericDetector implements TaskDetector using function-based logic
type GenericDetector struct {
	taskDef *TaskDefinition
}

// NewGenericDetector creates a detector from a task definition
func NewGenericDetector(taskDef *TaskDefinition) *GenericDetector {
	return &GenericDetector{taskDef: taskDef}
}

// GetTaskType returns the task type
func (d *GenericDetector) GetTaskType() types.TaskType {
	return d.taskDef.Type
}

// ScanForTasks scans using the task definition's detection function
func (d *GenericDetector) ScanForTasks(volumeMetrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) ([]*types.TaskDetectionResult, error) {
	if d.taskDef.DetectionFunc == nil {
		return nil, nil
	}
	return d.taskDef.DetectionFunc(volumeMetrics, clusterInfo, d.taskDef.Config)
}

// ScanInterval returns the scan interval from task definition
func (d *GenericDetector) ScanInterval() time.Duration {
	if d.taskDef.ScanInterval > 0 {
		return d.taskDef.ScanInterval
	}
	return 30 * time.Minute // Default
}

// IsEnabled returns whether this detector is enabled
func (d *GenericDetector) IsEnabled() bool {
	return d.taskDef.Config.IsEnabled()
}

// SetEnabled turns detection for this task type on or off.
//
// The admin maintenance policy is applied to detectors through an
// interface{ SetEnabled(bool) } type assertion (see
// MaintenanceIntegration.configureDetectorFromPolicy). Every registered task is
// backed by this generic detector, so without this method that assertion failed
// for every task and the policy never reached the flag that
// ScanWithTaskDetectors actually gates on.
func (d *GenericDetector) SetEnabled(enabled bool) {
	d.taskDef.Config.SetEnabled(enabled)
}

// GenericScheduler implements TaskScheduler using function-based logic
type GenericScheduler struct {
	taskDef *TaskDefinition
}

// NewGenericScheduler creates a scheduler from a task definition
func NewGenericScheduler(taskDef *TaskDefinition) *GenericScheduler {
	return &GenericScheduler{taskDef: taskDef}
}

// GetTaskType returns the task type
func (s *GenericScheduler) GetTaskType() types.TaskType {
	return s.taskDef.Type
}

// CanScheduleNow determines if a task can be scheduled using the task definition's function
func (s *GenericScheduler) CanScheduleNow(task *types.TaskInput, runningTasks []*types.TaskInput, availableWorkers []*types.WorkerData) bool {
	if s.taskDef.SchedulingFunc == nil {
		return s.defaultCanSchedule(task, runningTasks, availableWorkers)
	}
	return s.taskDef.SchedulingFunc(task, runningTasks, availableWorkers, s.taskDef.Config)
}

// defaultCanSchedule provides default scheduling logic
func (s *GenericScheduler) defaultCanSchedule(task *types.TaskInput, runningTasks []*types.TaskInput, availableWorkers []*types.WorkerData) bool {
	if !s.taskDef.Config.IsEnabled() {
		return false
	}

	// Count running tasks of this type
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == s.taskDef.Type {
			runningCount++
		}
	}

	// Check concurrency limit
	maxConcurrent := s.taskDef.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 1 // Default
	}
	if runningCount >= maxConcurrent {
		return false
	}

	// Check if we have available workers
	for _, worker := range availableWorkers {
		if worker.CurrentLoad < worker.MaxConcurrent {
			for _, capability := range worker.Capabilities {
				if capability == s.taskDef.Type {
					return true
				}
			}
		}
	}

	return false
}

// GetPriority returns the priority for this task
func (s *GenericScheduler) GetPriority(task *types.TaskInput) types.TaskPriority {
	return task.Priority
}

// GetMaxConcurrent returns max concurrent tasks
func (s *GenericScheduler) GetMaxConcurrent() int {
	if s.taskDef.MaxConcurrent > 0 {
		return s.taskDef.MaxConcurrent
	}
	return 1 // Default
}

// GetDefaultRepeatInterval returns the default repeat interval
func (s *GenericScheduler) GetDefaultRepeatInterval() time.Duration {
	if s.taskDef.RepeatInterval > 0 {
		return s.taskDef.RepeatInterval
	}
	return 24 * time.Hour // Default
}

// IsEnabled returns whether this scheduler is enabled
func (s *GenericScheduler) IsEnabled() bool {
	return s.taskDef.Config.IsEnabled()
}

// SetEnabled turns scheduling for this task type on or off. Detector and scheduler
// share one TaskDefinition, so this is the same flag GenericDetector.SetEnabled sets;
// both setters exist because the maintenance integration configures the two
// independently. See GenericDetector.SetEnabled.
func (s *GenericScheduler) SetEnabled(enabled bool) {
	s.taskDef.Config.SetEnabled(enabled)
}

// SetMaxConcurrent applies the policy's concurrency limit for this task type. It is
// the value GetMaxConcurrent returns, which the maintenance queue uses to decide
// whether another task of this type may start. Non-positive limits are ignored
// rather than turning into the implicit default of 1.
func (s *GenericScheduler) SetMaxConcurrent(maxConcurrent int) {
	if maxConcurrent <= 0 {
		return
	}
	s.taskDef.MaxConcurrent = maxConcurrent
}
