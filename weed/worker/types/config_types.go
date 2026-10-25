package types

import (
	"sync"
	"time"

	"google.golang.org/grpc"
)

// WorkerConfig represents the configuration for a worker
type WorkerConfig struct {
	AdminServer         string                 `json:"admin_server"`
	Capabilities        []TaskType             `json:"capabilities"`
	MaxConcurrent       int                    `json:"max_concurrent"`
	HeartbeatInterval   time.Duration          `json:"heartbeat_interval"`
	TaskRequestInterval time.Duration          `json:"task_request_interval"`
	BaseWorkingDir      string                 `json:"base_working_dir,omitempty"`
	CustomParameters    map[string]interface{} `json:"custom_parameters,omitempty"`
	GrpcDialOption      grpc.DialOption        `json:"-"` // Not serializable, for runtime use only
}

// MaintenanceConfig represents the configuration for the maintenance system
type MaintenanceConfig struct {
	Enabled       bool               `json:"enabled"`
	ScanInterval  time.Duration      `json:"scan_interval"`
	CleanInterval time.Duration      `json:"clean_interval"`
	TaskRetention time.Duration      `json:"task_retention"`
	WorkerTimeout time.Duration      `json:"worker_timeout"`
	Policy        *MaintenancePolicy `json:"policy"`
}

// MaintenancePolicy represents policies for maintenance operations
// This is now dynamic - task configurations are stored by task type
type MaintenancePolicy struct {
	// Task-specific configurations indexed by task type
	TaskConfigs map[TaskType]interface{} `json:"task_configs"`

	// Global maintenance settings
	GlobalSettings *GlobalMaintenanceSettings `json:"global_settings"`
}

// GlobalMaintenanceSettings contains settings that apply to all tasks
type GlobalMaintenanceSettings struct {
	DefaultMaxConcurrent int  `json:"default_max_concurrent"`
	MaintenanceEnabled   bool `json:"maintenance_enabled"`

	// Global timing settings
	DefaultScanInterval  time.Duration `json:"default_scan_interval"`
	DefaultTaskTimeout   time.Duration `json:"default_task_timeout"`
	DefaultRetryCount    int           `json:"default_retry_count"`
	DefaultRetryInterval time.Duration `json:"default_retry_interval"`

	// Global thresholds
	DefaultPriorityBoostAge time.Duration `json:"default_priority_boost_age"`
	GlobalConcurrentLimit   int           `json:"global_concurrent_limit"`
}

// MaintenanceStats represents statistics for the maintenance system
type MaintenanceStats struct {
	TotalTasks      int                `json:"total_tasks"`
	CompletedToday  int                `json:"completed_today"`
	FailedToday     int                `json:"failed_today"`
	ActiveWorkers   int                `json:"active_workers"`
	AverageTaskTime time.Duration      `json:"average_task_time"`
	TasksByStatus   map[TaskStatus]int `json:"tasks_by_status"`
	TasksByType     map[TaskType]int   `json:"tasks_by_type"`
	LastScanTime    time.Time          `json:"last_scan_time"`
	NextScanTime    time.Time          `json:"next_scan_time"`
}

// QueueStats represents statistics for the task queue
type QueueStats struct {
	PendingTasks    int `json:"pending_tasks"`
	AssignedTasks   int `json:"assigned_tasks"`
	InProgressTasks int `json:"in_progress_tasks"`
	CompletedTasks  int `json:"completed_tasks"`
	FailedTasks     int `json:"failed_tasks"`
	CancelledTasks  int `json:"cancelled_tasks"`
	ActiveWorkers   int `json:"active_workers"`
}

// MaintenanceConfigData represents the complete maintenance configuration data
type MaintenanceConfigData struct {
	Config       *MaintenanceConfig `json:"config"`
	IsEnabled    bool               `json:"is_enabled"`
	LastScanTime time.Time          `json:"last_scan_time"`
	NextScanTime time.Time          `json:"next_scan_time"`
	SystemStats  *MaintenanceStats  `json:"system_stats"`
}

// MaintenanceQueueData represents data for the maintenance queue UI
type MaintenanceQueueData struct {
	Tasks       []*Task     `json:"tasks"`
	Workers     []*Worker   `json:"workers"`
	Stats       *QueueStats `json:"stats"`
	LastUpdated time.Time   `json:"last_updated"`
}

// MaintenanceWorkersData represents data for the maintenance workers UI
type MaintenanceWorkersData struct {
	Workers       []*WorkerDetailsData `json:"workers"`
	ActiveWorkers int                  `json:"active_workers"`
	BusyWorkers   int                  `json:"busy_workers"`
	TotalLoad     int                  `json:"total_load"`
	LastUpdated   time.Time            `json:"last_updated"`
}

// defaultCapabilities holds the default capabilities for workers
var defaultCapabilities []TaskType
var defaultCapabilitiesMutex sync.RWMutex

// GetDefaultCapabilities returns the default capabilities for workers
func GetDefaultCapabilities() []TaskType {
	defaultCapabilitiesMutex.RLock()
	defer defaultCapabilitiesMutex.RUnlock()

	// Return a copy to prevent modification
	result := make([]TaskType, len(defaultCapabilities))
	copy(result, defaultCapabilities)
	return result
}

// DefaultWorkerConfig returns default worker configuration
func DefaultWorkerConfig() *WorkerConfig {
	// Get dynamic capabilities from registered task types
	capabilities := GetDefaultCapabilities()

	return &WorkerConfig{
		AdminServer:         "localhost:9333",
		MaxConcurrent:       2,
		HeartbeatInterval:   30 * time.Second,
		TaskRequestInterval: 5 * time.Second,
		Capabilities:        capabilities,
	}
}
