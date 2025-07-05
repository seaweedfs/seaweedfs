package dash

import (
	"sync"
	"time"
)

// MaintenanceTaskType represents different types of maintenance operations
type MaintenanceTaskType string

const (
	TaskTypeVacuum             MaintenanceTaskType = "vacuum"
	TaskTypeErasureCoding      MaintenanceTaskType = "erasure_coding"
	TaskTypeRemoteUpload       MaintenanceTaskType = "remote_upload"
	TaskTypeFixReplication     MaintenanceTaskType = "fix_replication"
	TaskTypeBalance            MaintenanceTaskType = "balance"
	TaskTypeClusterReplication MaintenanceTaskType = "cluster_replication"
)

// MaintenanceTaskPriority represents task execution priority
type MaintenanceTaskPriority int

const (
	PriorityLow MaintenanceTaskPriority = iota
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

// MaintenanceTaskStatus represents the current status of a task
type MaintenanceTaskStatus string

const (
	TaskStatusPending    MaintenanceTaskStatus = "pending"
	TaskStatusAssigned   MaintenanceTaskStatus = "assigned"
	TaskStatusInProgress MaintenanceTaskStatus = "in_progress"
	TaskStatusCompleted  MaintenanceTaskStatus = "completed"
	TaskStatusFailed     MaintenanceTaskStatus = "failed"
	TaskStatusCancelled  MaintenanceTaskStatus = "cancelled"
)

// MaintenanceTask represents a single maintenance operation
type MaintenanceTask struct {
	ID          string                  `json:"id"`
	Type        MaintenanceTaskType     `json:"type"`
	Priority    MaintenanceTaskPriority `json:"priority"`
	Status      MaintenanceTaskStatus   `json:"status"`
	VolumeID    uint32                  `json:"volume_id,omitempty"`
	Server      string                  `json:"server,omitempty"`
	Collection  string                  `json:"collection,omitempty"`
	Parameters  map[string]interface{}  `json:"parameters,omitempty"`
	Reason      string                  `json:"reason"`
	CreatedAt   time.Time               `json:"created_at"`
	ScheduledAt time.Time               `json:"scheduled_at"`
	StartedAt   *time.Time              `json:"started_at,omitempty"`
	CompletedAt *time.Time              `json:"completed_at,omitempty"`
	WorkerID    string                  `json:"worker_id,omitempty"`
	Error       string                  `json:"error,omitempty"`
	Progress    float64                 `json:"progress"` // 0-100
	RetryCount  int                     `json:"retry_count"`
	MaxRetries  int                     `json:"max_retries"`
}

// MaintenancePolicy defines when and how maintenance tasks should be created
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

// MaintenanceWorker represents a worker instance
type MaintenanceWorker struct {
	ID            string                `json:"id"`
	Address       string                `json:"address"`
	LastHeartbeat time.Time             `json:"last_heartbeat"`
	Status        string                `json:"status"` // active, inactive, busy
	CurrentTask   *MaintenanceTask      `json:"current_task,omitempty"`
	Capabilities  []MaintenanceTaskType `json:"capabilities"`
	MaxConcurrent int                   `json:"max_concurrent"`
	CurrentLoad   int                   `json:"current_load"`
}

// MaintenanceQueue manages the task queue and worker coordination
type MaintenanceQueue struct {
	tasks                 map[string]*MaintenanceTask
	workers               map[string]*MaintenanceWorker
	pendingTasks          []*MaintenanceTask
	mutex                 sync.RWMutex
	policy                *MaintenancePolicy
	simplifiedIntegration *SimplifiedMaintenanceIntegration
}

// MaintenanceScanner analyzes the cluster and generates maintenance tasks
type MaintenanceScanner struct {
	adminServer           *AdminServer
	policy                *MaintenancePolicy
	queue                 *MaintenanceQueue
	lastScan              map[MaintenanceTaskType]time.Time
	simplifiedIntegration *SimplifiedMaintenanceIntegration
}

// TaskDetectionResult represents the result of scanning for maintenance needs
type TaskDetectionResult struct {
	TaskType   MaintenanceTaskType     `json:"task_type"`
	VolumeID   uint32                  `json:"volume_id,omitempty"`
	Server     string                  `json:"server,omitempty"`
	Collection string                  `json:"collection,omitempty"`
	Priority   MaintenanceTaskPriority `json:"priority"`
	Reason     string                  `json:"reason"`
	Parameters map[string]interface{}  `json:"parameters,omitempty"`
	ScheduleAt time.Time               `json:"schedule_at"`
}

// VolumeHealthMetrics contains health information about a volume
type VolumeHealthMetrics struct {
	VolumeID         uint32        `json:"volume_id"`
	Server           string        `json:"server"`
	Collection       string        `json:"collection"`
	Size             uint64        `json:"size"`
	DeletedBytes     uint64        `json:"deleted_bytes"`
	GarbageRatio     float64       `json:"garbage_ratio"`
	LastModified     time.Time     `json:"last_modified"`
	Age              time.Duration `json:"age"`
	ReplicaCount     int           `json:"replica_count"`
	ExpectedReplicas int           `json:"expected_replicas"`
	IsReadOnly       bool          `json:"is_read_only"`
	HasRemoteCopy    bool          `json:"has_remote_copy"`
	IsECVolume       bool          `json:"is_ec_volume"`
	FullnessRatio    float64       `json:"fullness_ratio"`
}

// MaintenanceStats provides statistics about maintenance operations
type MaintenanceStats struct {
	TotalTasks      int                           `json:"total_tasks"`
	TasksByStatus   map[MaintenanceTaskStatus]int `json:"tasks_by_status"`
	TasksByType     map[MaintenanceTaskType]int   `json:"tasks_by_type"`
	ActiveWorkers   int                           `json:"active_workers"`
	CompletedToday  int                           `json:"completed_today"`
	FailedToday     int                           `json:"failed_today"`
	AverageTaskTime time.Duration                 `json:"average_task_time"`
	LastScanTime    time.Time                     `json:"last_scan_time"`
	NextScanTime    time.Time                     `json:"next_scan_time"`
}

// MaintenanceConfig holds configuration for the maintenance system
type MaintenanceConfig struct {
	Enabled                bool               `json:"enabled"`
	ScanIntervalSeconds    int                `json:"scan_interval_seconds"`    // How often to scan for maintenance needs (in seconds)
	WorkerTimeoutSeconds   int                `json:"worker_timeout_seconds"`   // Worker heartbeat timeout (in seconds)
	TaskTimeoutSeconds     int                `json:"task_timeout_seconds"`     // Individual task timeout (in seconds)
	RetryDelaySeconds      int                `json:"retry_delay_seconds"`      // Delay between retries (in seconds)
	MaxRetries             int                `json:"max_retries"`              // Default max retries for tasks
	CleanupIntervalSeconds int                `json:"cleanup_interval_seconds"` // How often to clean up old tasks (in seconds)
	TaskRetentionSeconds   int                `json:"task_retention_seconds"`   // How long to keep completed/failed tasks (in seconds)
	Policy                 *MaintenancePolicy `json:"policy"`
}

// Default configuration values
func DefaultMaintenanceConfig() *MaintenanceConfig {
	return &MaintenanceConfig{
		Enabled:                false,       // Disabled by default for safety
		ScanIntervalSeconds:    30 * 60,     // 30 minutes
		WorkerTimeoutSeconds:   5 * 60,      // 5 minutes
		TaskTimeoutSeconds:     2 * 60 * 60, // 2 hours
		RetryDelaySeconds:      15 * 60,     // 15 minutes
		MaxRetries:             3,
		CleanupIntervalSeconds: 24 * 60 * 60,     // 24 hours
		TaskRetentionSeconds:   7 * 24 * 60 * 60, // 7 days
		Policy: &MaintenancePolicy{
			VacuumEnabled:             true,
			VacuumGarbageRatio:        0.3, // 30% garbage
			VacuumMinInterval:         6,   // 6 hours
			VacuumMaxConcurrent:       2,
			ECEnabled:                 false,  // Conservative default
			ECVolumeAgeHours:          24 * 7, // 1 week
			ECFullnessRatio:           0.9,    // 90% full
			ECMaxConcurrent:           1,
			RemoteUploadEnabled:       false,
			RemoteUploadAgeHours:      24 * 30, // 1 month
			RemoteUploadMaxConcurrent: 1,
			ReplicationFixEnabled:     true,
			ReplicationCheckInterval:  4, // 4 hours
			ReplicationMaxConcurrent:  2,
			BalanceEnabled:            false,
			BalanceCheckInterval:      12,  // 12 hours
			BalanceThreshold:          0.2, // 20% imbalance
			BalanceMaxConcurrent:      1,
		},
	}
}

// MaintenanceQueueData represents data for the queue visualization UI
type MaintenanceQueueData struct {
	Tasks       []*MaintenanceTask   `json:"tasks"`
	Workers     []*MaintenanceWorker `json:"workers"`
	Stats       *QueueStats          `json:"stats"`
	LastUpdated time.Time            `json:"last_updated"`
}

// QueueStats provides statistics for the queue UI
type QueueStats struct {
	PendingTasks   int `json:"pending_tasks"`
	RunningTasks   int `json:"running_tasks"`
	CompletedToday int `json:"completed_today"`
	FailedToday    int `json:"failed_today"`
	TotalTasks     int `json:"total_tasks"`
}

// MaintenanceConfigData represents configuration data for the UI
type MaintenanceConfigData struct {
	Config       *MaintenanceConfig `json:"config"`
	IsEnabled    bool               `json:"is_enabled"`
	LastScanTime time.Time          `json:"last_scan_time"`
	NextScanTime time.Time          `json:"next_scan_time"`
	SystemStats  *MaintenanceStats  `json:"system_stats"`
}

// WorkerDetailsData represents detailed worker information
type WorkerDetailsData struct {
	Worker       *MaintenanceWorker `json:"worker"`
	CurrentTasks []*MaintenanceTask `json:"current_tasks"`
	RecentTasks  []*MaintenanceTask `json:"recent_tasks"`
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

// ClusterReplicationTask represents a cluster replication task parameters
type ClusterReplicationTask struct {
	SourcePath      string            `json:"source_path"`
	TargetCluster   string            `json:"target_cluster"`
	TargetPath      string            `json:"target_path"`
	ReplicationMode string            `json:"replication_mode"` // "sync", "async", "backup"
	Priority        int               `json:"priority"`
	Checksum        string            `json:"checksum,omitempty"`
	FileSize        int64             `json:"file_size"`
	CreatedAt       time.Time         `json:"created_at"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}
