package maintenance

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// AdminClient interface defines what the maintenance system needs from the admin server
type AdminClient interface {
	WithMasterClient(fn func(client master_pb.SeaweedClient) error) error
}

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

// TaskPolicy represents configuration for a specific task type
type TaskPolicy struct {
	Enabled        bool                   `json:"enabled"`
	MaxConcurrent  int                    `json:"max_concurrent"`
	RepeatInterval int                    `json:"repeat_interval"` // Hours to wait before repeating
	CheckInterval  int                    `json:"check_interval"`  // Hours between checks
	Configuration  map[string]interface{} `json:"configuration"`   // Task-specific config
}

// MaintenancePolicy defines policies for maintenance operations using a dynamic structure
type MaintenancePolicy struct {
	// Task-specific policies mapped by task type
	TaskPolicies map[MaintenanceTaskType]*TaskPolicy `json:"task_policies"`

	// Global policy settings
	GlobalMaxConcurrent   int `json:"global_max_concurrent"`   // Overall limit across all task types
	DefaultRepeatInterval int `json:"default_repeat_interval"` // Default hours if task doesn't specify
	DefaultCheckInterval  int `json:"default_check_interval"`  // Default hours for periodic checks
}

// GetTaskPolicy returns the policy for a specific task type, creating generic defaults if needed
func (mp *MaintenancePolicy) GetTaskPolicy(taskType MaintenanceTaskType) *TaskPolicy {
	if mp.TaskPolicies == nil {
		mp.TaskPolicies = make(map[MaintenanceTaskType]*TaskPolicy)
	}

	policy, exists := mp.TaskPolicies[taskType]
	if !exists {
		// Create generic default policy using global settings - no hardcoded fallbacks
		policy = &TaskPolicy{
			Enabled:        false,                    // Conservative default - require explicit enabling
			MaxConcurrent:  1,                        // Conservative default concurrency
			RepeatInterval: mp.DefaultRepeatInterval, // Use configured default, 0 if not set
			CheckInterval:  mp.DefaultCheckInterval,  // Use configured default, 0 if not set
			Configuration:  make(map[string]interface{}),
		}
		mp.TaskPolicies[taskType] = policy
	}

	return policy
}

// SetTaskPolicy sets the policy for a specific task type
func (mp *MaintenancePolicy) SetTaskPolicy(taskType MaintenanceTaskType, policy *TaskPolicy) {
	if mp.TaskPolicies == nil {
		mp.TaskPolicies = make(map[MaintenanceTaskType]*TaskPolicy)
	}
	mp.TaskPolicies[taskType] = policy
}

// IsTaskEnabled returns whether a task type is enabled
func (mp *MaintenancePolicy) IsTaskEnabled(taskType MaintenanceTaskType) bool {
	policy := mp.GetTaskPolicy(taskType)
	return policy.Enabled
}

// GetMaxConcurrent returns the max concurrent limit for a task type
func (mp *MaintenancePolicy) GetMaxConcurrent(taskType MaintenanceTaskType) int {
	policy := mp.GetTaskPolicy(taskType)
	return policy.MaxConcurrent
}

// GetRepeatInterval returns the repeat interval for a task type
func (mp *MaintenancePolicy) GetRepeatInterval(taskType MaintenanceTaskType) int {
	policy := mp.GetTaskPolicy(taskType)
	return policy.RepeatInterval
}

// GetTaskConfig returns a configuration value for a task type
func (mp *MaintenancePolicy) GetTaskConfig(taskType MaintenanceTaskType, key string) (interface{}, bool) {
	policy := mp.GetTaskPolicy(taskType)
	value, exists := policy.Configuration[key]
	return value, exists
}

// SetTaskConfig sets a configuration value for a task type
func (mp *MaintenancePolicy) SetTaskConfig(taskType MaintenanceTaskType, key string, value interface{}) {
	policy := mp.GetTaskPolicy(taskType)
	if policy.Configuration == nil {
		policy.Configuration = make(map[string]interface{})
	}
	policy.Configuration[key] = value
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
	tasks        map[string]*MaintenanceTask
	workers      map[string]*MaintenanceWorker
	pendingTasks []*MaintenanceTask
	mutex        sync.RWMutex
	policy       *MaintenancePolicy
	integration  *MaintenanceIntegration
}

// MaintenanceScanner analyzes the cluster and generates maintenance tasks
type MaintenanceScanner struct {
	adminClient AdminClient
	policy      *MaintenancePolicy
	queue       *MaintenanceQueue
	lastScan    map[MaintenanceTaskType]time.Time
	integration *MaintenanceIntegration
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
			GlobalMaxConcurrent:   4,
			DefaultRepeatInterval: 6,
			DefaultCheckInterval:  12,
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
