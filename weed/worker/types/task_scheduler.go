package types

// TaskScheduler defines how a task schedules itself
type TaskScheduler interface {
	// GetTaskType returns the task type this scheduler handles
	GetTaskType() TaskType

	// CanScheduleNow determines if a task can be scheduled right now
	CanScheduleNow(task *Task, runningTasks []*Task, availableWorkers []*Worker) bool

	// GetPriority returns the priority for this task
	GetPriority(task *Task) TaskPriority

	// GetMaxConcurrent returns max concurrent tasks of this type
	GetMaxConcurrent() int
}
