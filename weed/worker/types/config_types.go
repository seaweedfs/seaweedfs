package types

import (
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

// MaintenancePolicy represents policies for different maintenance operations
type MaintenancePolicy struct {
	// Vacuum policies
	VacuumEnabled       bool    `json:"vacuum_enabled"`
	VacuumGarbageRatio  float64 `json:"vacuum_garbage_ratio"`  // Trigger vacuum when garbage > this ratio
	VacuumMinInterval   int     `json:"vacuum_min_interval"`   // Minimum hours between vacuum operations
	VacuumMaxConcurrent int     `json:"vacuum_max_concurrent"` // Max concurrent vacuum operations

	// Erasure Coding policies
	ECEnabled        bool    `json:"ec_enabled"`
	ECVolumeAgeHours int     `json:"ec_volume_age_hours"` // Convert volumes older than this to EC
	ECFullnessRatio  float64 `json:"ec_fullness_ratio"`   // Convert volumes when fuller than this ratio
	ECMaxConcurrent  int     `json:"ec_max_concurrent"`   // Max concurrent EC operations

	// Remote Upload policies
	RemoteUploadEnabled       bool   `json:"remote_upload_enabled"`
	RemoteUploadAgeHours      int    `json:"remote_upload_age_hours"` // Upload volumes older than this
	RemoteUploadPattern       string `json:"remote_upload_pattern"`   // Collection pattern for remote upload
	RemoteUploadMaxConcurrent int    `json:"remote_upload_max_concurrent"`

	// Replication Fix policies
	ReplicationFixEnabled    bool `json:"replication_fix_enabled"`
	ReplicationCheckInterval int  `json:"replication_check_interval"` // Hours between replication checks
	ReplicationMaxConcurrent int  `json:"replication_max_concurrent"`

	// Balance policies
	BalanceEnabled       bool    `json:"balance_enabled"`
	BalanceCheckInterval int     `json:"balance_check_interval"` // Hours between balance checks
	BalanceThreshold     float64 `json:"balance_threshold"`      // Trigger balance when imbalance > this ratio
	BalanceMaxConcurrent int     `json:"balance_max_concurrent"`
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

// DefaultMaintenanceConfig returns default maintenance configuration
func DefaultMaintenanceConfig() *MaintenanceConfig {
	return &MaintenanceConfig{
		Enabled:       true,
		ScanInterval:  30 * time.Minute,
		CleanInterval: 6 * time.Hour,
		TaskRetention: 7 * 24 * time.Hour, // 7 days
		WorkerTimeout: 5 * time.Minute,
		Policy: &MaintenancePolicy{
			VacuumEnabled:       true,
			VacuumGarbageRatio:  0.3,
			VacuumMinInterval:   24, // hours
			VacuumMaxConcurrent: 2,

			ECEnabled:        true,
			ECVolumeAgeHours: 168, // 1 week
			ECFullnessRatio:  0.9,
			ECMaxConcurrent:  1,

			RemoteUploadEnabled:       false,
			RemoteUploadAgeHours:      720, // 30 days
			RemoteUploadPattern:       "",
			RemoteUploadMaxConcurrent: 1,

			ReplicationFixEnabled:    true,
			ReplicationCheckInterval: 6, // hours
			ReplicationMaxConcurrent: 3,

			BalanceEnabled:       true,
			BalanceCheckInterval: 12, // hours
			BalanceThreshold:     0.1,
			BalanceMaxConcurrent: 2,
		},
	}
}

// DefaultWorkerConfig returns default worker configuration
func DefaultWorkerConfig() *WorkerConfig {
	return &WorkerConfig{
		AdminServer:         "localhost:9333",
		MaxConcurrent:       2,
		HeartbeatInterval:   30 * time.Second,
		TaskRequestInterval: 5 * time.Second,
		Capabilities: []TaskType{
			TaskTypeVacuum,
			TaskTypeErasureCoding,
			TaskTypeRemoteUpload,
			TaskTypeFixReplication,
			TaskTypeBalance,
		},
	}
}
