package types

import (
	"sync"
	"time"
)

// WorkerConfig represents the configuration for a worker
type WorkerConfig struct {
	AdminServer         string                 `json:"admin_server"`
	Capabilities        []TaskType             `json:"capabilities"`
	MaxConcurrent       int                    `json:"max_concurrent"`
	HeartbeatInterval   time.Duration          `json:"heartbeat_interval"`
	TaskRequestInterval time.Duration          `json:"task_request_interval"`
	CustomParameters    map[string]interface{} `json:"custom_parameters,omitempty"`
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

// SetDefaultCapabilities sets the default capabilities for workers
// This should be called after task registration is complete
func SetDefaultCapabilities(capabilities []TaskType) {
	defaultCapabilitiesMutex.Lock()
	defer defaultCapabilitiesMutex.Unlock()
	defaultCapabilities = make([]TaskType, len(capabilities))
	copy(defaultCapabilities, capabilities)
}

// GetDefaultCapabilities returns the default capabilities for workers
func GetDefaultCapabilities() []TaskType {
	defaultCapabilitiesMutex.RLock()
	defer defaultCapabilitiesMutex.RUnlock()

	// Return a copy to prevent modification
	result := make([]TaskType, len(defaultCapabilities))
	copy(result, defaultCapabilities)
	return result
}

// DefaultMaintenanceConfig returns default maintenance configuration
func DefaultMaintenanceConfig() *MaintenanceConfig {
	return &MaintenanceConfig{
		Enabled:       true,
		ScanInterval:  30 * time.Minute,
		CleanInterval: 6 * time.Hour,
		TaskRetention: 7 * 24 * time.Hour, // 7 days
		WorkerTimeout: 5 * time.Minute,
		Policy:        NewMaintenancePolicy(),
	}
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

// NewMaintenancePolicy creates a new dynamic maintenance policy
func NewMaintenancePolicy() *MaintenancePolicy {
	return &MaintenancePolicy{
		TaskConfigs: make(map[TaskType]interface{}),
		GlobalSettings: &GlobalMaintenanceSettings{
			DefaultMaxConcurrent:    2,
			MaintenanceEnabled:      true,
			DefaultScanInterval:     30 * time.Minute,
			DefaultTaskTimeout:      5 * time.Minute,
			DefaultRetryCount:       3,
			DefaultRetryInterval:    5 * time.Minute,
			DefaultPriorityBoostAge: 24 * time.Hour,
			GlobalConcurrentLimit:   5,
		},
	}
}

// SetTaskConfig sets the configuration for a specific task type
func (p *MaintenancePolicy) SetTaskConfig(taskType TaskType, config interface{}) {
	if p.TaskConfigs == nil {
		p.TaskConfigs = make(map[TaskType]interface{})
	}
	p.TaskConfigs[taskType] = config
}

// GetTaskConfig returns the configuration for a specific task type
func (p *MaintenancePolicy) GetTaskConfig(taskType TaskType) interface{} {
	if p.TaskConfigs == nil {
		return nil
	}
	return p.TaskConfigs[taskType]
}

// IsTaskEnabled returns whether a task type is enabled (generic helper)
func (p *MaintenancePolicy) IsTaskEnabled(taskType TaskType) bool {
	if !p.GlobalSettings.MaintenanceEnabled {
		return false
	}

	config := p.GetTaskConfig(taskType)
	if config == nil {
		return false
	}

	// Try to get enabled field from config using type assertion
	if configMap, ok := config.(map[string]interface{}); ok {
		if enabled, exists := configMap["enabled"]; exists {
			if enabledBool, ok := enabled.(bool); ok {
				return enabledBool
			}
		}
	}

	// If we can't determine from config, default to global setting
	return p.GlobalSettings.MaintenanceEnabled
}

// GetMaxConcurrent returns the max concurrent setting for a task type
func (p *MaintenancePolicy) GetMaxConcurrent(taskType TaskType) int {
	config := p.GetTaskConfig(taskType)
	if config == nil {
		return p.GlobalSettings.DefaultMaxConcurrent
	}

	// Try to get max_concurrent field from config
	if configMap, ok := config.(map[string]interface{}); ok {
		if maxConcurrent, exists := configMap["max_concurrent"]; exists {
			if maxConcurrentInt, ok := maxConcurrent.(int); ok {
				return maxConcurrentInt
			}
			if maxConcurrentFloat, ok := maxConcurrent.(float64); ok {
				return int(maxConcurrentFloat)
			}
		}
	}

	return p.GlobalSettings.DefaultMaxConcurrent
}

// GetScanInterval returns the scan interval for a task type
func (p *MaintenancePolicy) GetScanInterval(taskType TaskType) time.Duration {
	config := p.GetTaskConfig(taskType)
	if config == nil {
		return p.GlobalSettings.DefaultScanInterval
	}

	// Try to get scan_interval field from config
	if configMap, ok := config.(map[string]interface{}); ok {
		if scanInterval, exists := configMap["scan_interval"]; exists {
			if scanIntervalDuration, ok := scanInterval.(time.Duration); ok {
				return scanIntervalDuration
			}
			if scanIntervalString, ok := scanInterval.(string); ok {
				if duration, err := time.ParseDuration(scanIntervalString); err == nil {
					return duration
				}
			}
		}
	}

	return p.GlobalSettings.DefaultScanInterval
}

// GetAllTaskTypes returns all configured task types
func (p *MaintenancePolicy) GetAllTaskTypes() []TaskType {
	if p.TaskConfigs == nil {
		return []TaskType{}
	}

	taskTypes := make([]TaskType, 0, len(p.TaskConfigs))
	for taskType := range p.TaskConfigs {
		taskTypes = append(taskTypes, taskType)
	}
	return taskTypes
}
