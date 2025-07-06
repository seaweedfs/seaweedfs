package types

// TaskRegistry manages task detectors and schedulers
type TaskRegistry struct {
	detectors  map[TaskType]TaskDetector
	schedulers map[TaskType]TaskScheduler
}

// NewTaskRegistry creates a new simple task registry
func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		detectors:  make(map[TaskType]TaskDetector),
		schedulers: make(map[TaskType]TaskScheduler),
	}
}

// RegisterTask registers both detector and scheduler for a task type
func (r *TaskRegistry) RegisterTask(detector TaskDetector, scheduler TaskScheduler) {
	taskType := detector.GetTaskType()
	if taskType != scheduler.GetTaskType() {
		panic("detector and scheduler task types must match")
	}

	r.detectors[taskType] = detector
	r.schedulers[taskType] = scheduler
}

// GetDetector returns the detector for a task type
func (r *TaskRegistry) GetDetector(taskType TaskType) TaskDetector {
	return r.detectors[taskType]
}

// GetScheduler returns the scheduler for a task type
func (r *TaskRegistry) GetScheduler(taskType TaskType) TaskScheduler {
	return r.schedulers[taskType]
}

// GetAllDetectors returns all registered detectors
func (r *TaskRegistry) GetAllDetectors() map[TaskType]TaskDetector {
	result := make(map[TaskType]TaskDetector)
	for k, v := range r.detectors {
		result[k] = v
	}
	return result
}

// GetAllSchedulers returns all registered schedulers
func (r *TaskRegistry) GetAllSchedulers() map[TaskType]TaskScheduler {
	result := make(map[TaskType]TaskScheduler)
	for k, v := range r.schedulers {
		result[k] = v
	}
	return result
}
