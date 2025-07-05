package types

import "time"

// TaskScheduler defines the interface for task scheduling
type TaskScheduler interface {
	// GetTaskType returns the task type this scheduler handles
	GetTaskType() TaskType

	// CanScheduleNow determines if a task can be scheduled now
	CanScheduleNow(task *Task, runningTasks []*Task, availableWorkers []*Worker) bool

	// GetPriority returns the priority for tasks of this type
	GetPriority(task *Task) TaskPriority

	// GetMaxConcurrent returns the maximum concurrent tasks of this type
	GetMaxConcurrent() int

	// GetDefaultRepeatInterval returns the default interval to wait before repeating tasks of this type
	GetDefaultRepeatInterval() time.Duration

	// IsEnabled returns whether this scheduler is enabled
	IsEnabled() bool
}

// PolicyConfigurableScheduler defines the interface for schedulers that can be configured from policy
type PolicyConfigurableScheduler interface {
	TaskScheduler

	// ConfigureFromPolicy configures the scheduler based on the maintenance policy
	ConfigureFromPolicy(policy interface{})
}
