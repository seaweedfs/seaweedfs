package types

import (
	"time"
)

// WorkerData represents a maintenance worker instance data
type WorkerData struct {
	ID            string     `json:"id"`
	Address       string     `json:"address"`
	LastHeartbeat time.Time  `json:"last_heartbeat"`
	Status        string     `json:"status"` // active, inactive, busy
	CurrentTask   *TaskInput `json:"current_task,omitempty"`
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
	CurrentTasks   []TaskInput   `json:"current_tasks"`
	Uptime         time.Duration `json:"uptime"`
	TasksCompleted int           `json:"tasks_completed"`
	TasksFailed    int           `json:"tasks_failed"`
}

// WorkerDetailsData represents detailed worker information
type WorkerDetailsData struct {
	Worker       *WorkerData        `json:"worker"`
	CurrentTasks []*TaskInput       `json:"current_tasks"`
	RecentTasks  []*TaskInput       `json:"recent_tasks"`
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
