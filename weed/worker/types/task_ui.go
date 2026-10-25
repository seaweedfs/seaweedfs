package types

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// TaskConfig defines the interface for task configurations
// This matches the interfaces used in base package and handlers
type TaskConfig interface {
	// Common methods from BaseConfig
	IsEnabled() bool
	SetEnabled(enabled bool)
	Validate() error

	// Protobuf serialization methods - no more interface{}!
	ToTaskPolicy() *worker_pb.TaskPolicy
	FromTaskPolicy(policy *worker_pb.TaskPolicy) error
}

// TaskUIProvider defines how tasks provide their configuration UI
// This interface is simplified to work with schema-driven configuration
type TaskUIProvider interface {
	// GetTaskType returns the task type
	GetTaskType() TaskType

	// GetDisplayName returns the human-readable name
	GetDisplayName() string

	// GetDescription returns a description of what this task does
	GetDescription() string

	// GetIcon returns the icon CSS class or HTML for this task type
	GetIcon() string

	// GetCurrentConfig returns the current configuration as TaskConfig
	GetCurrentConfig() TaskConfig

	// ApplyTaskPolicy applies protobuf TaskPolicy configuration
	ApplyTaskPolicy(policy *worker_pb.TaskPolicy) error

	// ApplyTaskConfig applies TaskConfig interface configuration
	ApplyTaskConfig(config TaskConfig) error
}

// TaskStats represents runtime statistics for a task type
type TaskStats struct {
	TaskType       TaskType      `json:"task_type"`
	DisplayName    string        `json:"display_name"`
	Enabled        bool          `json:"enabled"`
	LastScan       time.Time     `json:"last_scan"`
	NextScan       time.Time     `json:"next_scan"`
	PendingTasks   int           `json:"pending_tasks"`
	RunningTasks   int           `json:"running_tasks"`
	CompletedToday int           `json:"completed_today"`
	FailedToday    int           `json:"failed_today"`
	MaxConcurrent  int           `json:"max_concurrent"`
	ScanInterval   time.Duration `json:"scan_interval"`
}

// UIRegistry manages task UI providers
type UIRegistry struct {
	providers map[TaskType]TaskUIProvider
}

// NewUIRegistry creates a new UI registry
func NewUIRegistry() *UIRegistry {
	return &UIRegistry{
		providers: make(map[TaskType]TaskUIProvider),
	}
}

// RegisterUI registers a task UI provider
func (r *UIRegistry) RegisterUI(provider TaskUIProvider) {
	r.providers[provider.GetTaskType()] = provider
}

// GetProvider returns the UI provider for a task type
func (r *UIRegistry) GetProvider(taskType TaskType) TaskUIProvider {
	return r.providers[taskType]
}

// GetAllProviders returns all registered UI providers
func (r *UIRegistry) GetAllProviders() map[TaskType]TaskUIProvider {
	result := make(map[TaskType]TaskUIProvider)
	for k, v := range r.providers {
		result[k] = v
	}
	return result
}

// Common UI data structures for shared components
type TaskListData struct {
	Tasks       []*Task      `json:"tasks"`
	TaskStats   []*TaskStats `json:"task_stats"`
	LastUpdated time.Time    `json:"last_updated"`
}

type TaskDetailsData struct {
	Task        *Task      `json:"task"`
	TaskType    TaskType   `json:"task_type"`
	DisplayName string     `json:"display_name"`
	Description string     `json:"description"`
	Stats       *TaskStats `json:"stats"`
	LastUpdated time.Time  `json:"last_updated"`
}
