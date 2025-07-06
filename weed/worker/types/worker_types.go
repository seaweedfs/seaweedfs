package types

import (
	"time"
)

// Worker represents a maintenance worker instance
type Worker struct {
	ID            string     `json:"id"`
	Address       string     `json:"address"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	Status        string     `json:"status"` // active, inactive, busy
	CurrentTask   *Task      `json:"current_task,omitempty"`
	Capabilities  []TaskType `json:"capabilities"`
	MaxConcurrent int        `json:"max_concurrent"`
	CurrentLoad   int        `json:"current_load"`
}

// WorkerStatus represents the current status of a worker
type WorkerStatus struct {
	WorkerID       string        `json:"worker_id"`
	Status         string        `json:"status"`
	Capabilities   []TaskType    `json:"capabilities"`
	MaxConcurrent  int           `json:"max_concurrent"`
	CurrentLoad    int           `json:"current_load"`
	LastHeartbeat  time.Time     `json:"last_heartbeat"`
	CurrentTasks   []Task        `json:"current_tasks"`
	Uptime         time.Duration `json:"uptime"`
	TasksCompleted int           `json:"tasks_completed"`
	TasksFailed    int           `json:"tasks_failed"`
}

// WorkerDetailsData represents detailed worker information
type WorkerDetailsData struct {
	Worker       *Worker            `json:"worker"`
	CurrentTasks []*Task            `json:"current_tasks"`
	RecentTasks  []*Task            `json:"recent_tasks"`
	Performance  *WorkerPerformance `json:"performance"`
	LastUpdated  time.Time          `json:"last_updated"`
}

// WorkerPerformance tracks worker performance metrics
type WorkerPerformance struct {
	TasksCompleted  int           `json:"tasks_completed"`
	TasksFailed     int           `json:"tasks_failed"`
	AverageTaskTime time.Duration `json:"average_task_time"`
	Uptime          time.Duration `json:"uptime"`
	SuccessRate     float64       `json:"success_rate"`
}

// RegistryStats represents statistics for the worker registry
type RegistryStats struct {
	TotalWorkers   int           `json:"total_workers"`
	ActiveWorkers  int           `json:"active_workers"`
	BusyWorkers    int           `json:"busy_workers"`
	IdleWorkers    int           `json:"idle_workers"`
	TotalTasks     int           `json:"total_tasks"`
	CompletedTasks int           `json:"completed_tasks"`
	FailedTasks    int           `json:"failed_tasks"`
	StartTime      time.Time     `json:"start_time"`
	Uptime         time.Duration `json:"uptime"`
	LastUpdated    time.Time     `json:"last_updated"`
}

// WorkerSummary represents a summary of all workers
type WorkerSummary struct {
	TotalWorkers int              `json:"total_workers"`
	ByStatus     map[string]int   `json:"by_status"`
	ByCapability map[TaskType]int `json:"by_capability"`
	TotalLoad    int              `json:"total_load"`
	MaxCapacity  int              `json:"max_capacity"`
}

// WorkerFactory creates worker instances
type WorkerFactory interface {
	Create(config WorkerConfig) (WorkerInterface, error)
	Type() string
	Description() string
}

// WorkerInterface defines the interface for all worker implementations
type WorkerInterface interface {
	ID() string
	Start() error
	Stop() error
	RegisterTask(taskType TaskType, factory TaskFactory)
	GetCapabilities() []TaskType
	GetStatus() WorkerStatus
	HandleTask(task *Task) error
	SetCapabilities(capabilities []TaskType)
	SetMaxConcurrent(max int)
	SetHeartbeatInterval(interval time.Duration)
	SetTaskRequestInterval(interval time.Duration)
}

// TaskFactory creates task instances
type TaskFactory interface {
	Create(params TaskParams) (TaskInterface, error)
	Capabilities() []string
	Description() string
}

// TaskInterface defines the interface for all task implementations
type TaskInterface interface {
	Type() TaskType
	Execute(params TaskParams) error
	Validate(params TaskParams) error
	EstimateTime(params TaskParams) time.Duration
	GetProgress() float64
	Cancel() error
}
