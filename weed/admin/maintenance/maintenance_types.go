package maintenance

import (
	"html/template"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// AdminClient interface defines what the maintenance system needs from the admin server
type AdminClient interface {
	WithMasterClient(fn func(client master_pb.SeaweedClient) error) error
}

// MaintenanceTaskType represents different types of maintenance operations
type MaintenanceTaskType string

// GetRegisteredMaintenanceTaskTypes returns all registered task types as MaintenanceTaskType values
// sorted alphabetically for consistent menu ordering
func GetRegisteredMaintenanceTaskTypes() []MaintenanceTaskType {
	typesRegistry := tasks.GetGlobalTypesRegistry()
	var taskTypes []MaintenanceTaskType

	for workerTaskType := range typesRegistry.GetAllDetectors() {
		maintenanceTaskType := MaintenanceTaskType(string(workerTaskType))
		taskTypes = append(taskTypes, maintenanceTaskType)
	}

	// Sort task types alphabetically to ensure consistent menu ordering
	sort.Slice(taskTypes, func(i, j int) bool {
		return string(taskTypes[i]) < string(taskTypes[j])
	})

	return taskTypes
}

// GetMaintenanceTaskType returns a specific task type if it's registered, or empty string if not found
func GetMaintenanceTaskType(taskTypeName string) MaintenanceTaskType {
	typesRegistry := tasks.GetGlobalTypesRegistry()

	for workerTaskType := range typesRegistry.GetAllDetectors() {
		if string(workerTaskType) == taskTypeName {
			return MaintenanceTaskType(taskTypeName)
		}
	}

	return MaintenanceTaskType("")
}

// IsMaintenanceTaskTypeRegistered checks if a task type is registered
func IsMaintenanceTaskTypeRegistered(taskType MaintenanceTaskType) bool {
	typesRegistry := tasks.GetGlobalTypesRegistry()

	for workerTaskType := range typesRegistry.GetAllDetectors() {
		if string(workerTaskType) == string(taskType) {
			return true
		}
	}

	return false
}

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
	Config       *MaintenanceConfig     `json:"config"`
	IsEnabled    bool                   `json:"is_enabled"`
	LastScanTime time.Time              `json:"last_scan_time"`
	NextScanTime time.Time              `json:"next_scan_time"`
	SystemStats  *MaintenanceStats      `json:"system_stats"`
	MenuItems    []*MaintenanceMenuItem `json:"menu_items"`
}

// MaintenanceMenuItem represents a menu item for task configuration
type MaintenanceMenuItem struct {
	TaskType    MaintenanceTaskType `json:"task_type"`
	DisplayName string              `json:"display_name"`
	Description string              `json:"description"`
	Icon        string              `json:"icon"`
	IsEnabled   bool                `json:"is_enabled"`
	Path        string              `json:"path"`
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

// TaskConfigData represents data for individual task configuration page
type TaskConfigData struct {
	TaskType       MaintenanceTaskType `json:"task_type"`
	TaskName       string              `json:"task_name"`
	TaskIcon       string              `json:"task_icon"`
	Description    string              `json:"description"`
	ConfigFormHTML template.HTML       `json:"config_form_html"`
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

// BuildMaintenancePolicyFromTasks creates a maintenance policy with configurations
// from all registered tasks using their UI providers
func BuildMaintenancePolicyFromTasks() *MaintenancePolicy {
	policy := &MaintenancePolicy{
		TaskPolicies:          make(map[MaintenanceTaskType]*TaskPolicy),
		GlobalMaxConcurrent:   4,
		DefaultRepeatInterval: 6,
		DefaultCheckInterval:  12,
	}

	// Get all registered task types from the UI registry
	uiRegistry := tasks.GetGlobalUIRegistry()
	typesRegistry := tasks.GetGlobalTypesRegistry()

	for taskType, provider := range uiRegistry.GetAllProviders() {
		// Convert task type to maintenance task type
		maintenanceTaskType := MaintenanceTaskType(string(taskType))

		// Get the default configuration from the UI provider
		defaultConfig := provider.GetCurrentConfig()

		// Create task policy from UI configuration
		taskPolicy := &TaskPolicy{
			Enabled:        true, // Default enabled
			MaxConcurrent:  2,    // Default concurrency
			RepeatInterval: policy.DefaultRepeatInterval,
			CheckInterval:  policy.DefaultCheckInterval,
			Configuration:  make(map[string]interface{}),
		}

		// Extract configuration from UI provider's config
		if configMap, ok := defaultConfig.(map[string]interface{}); ok {
			// Copy all configuration values
			for key, value := range configMap {
				taskPolicy.Configuration[key] = value
			}

			// Extract common fields
			if enabled, exists := configMap["enabled"]; exists {
				if enabledBool, ok := enabled.(bool); ok {
					taskPolicy.Enabled = enabledBool
				}
			}
			if maxConcurrent, exists := configMap["max_concurrent"]; exists {
				if maxConcurrentInt, ok := maxConcurrent.(int); ok {
					taskPolicy.MaxConcurrent = maxConcurrentInt
				} else if maxConcurrentFloat, ok := maxConcurrent.(float64); ok {
					taskPolicy.MaxConcurrent = int(maxConcurrentFloat)
				}
			}
		}

		// Also get defaults from scheduler if available (using types.TaskScheduler explicitly)
		var scheduler types.TaskScheduler = typesRegistry.GetScheduler(taskType)
		if scheduler != nil {
			if taskPolicy.MaxConcurrent <= 0 {
				taskPolicy.MaxConcurrent = scheduler.GetMaxConcurrent()
			}
			// Convert default repeat interval to hours
			if repeatInterval := scheduler.GetDefaultRepeatInterval(); repeatInterval > 0 {
				taskPolicy.RepeatInterval = int(repeatInterval.Hours())
			}
		}

		// Also get defaults from detector if available (using types.TaskDetector explicitly)
		var detector types.TaskDetector = typesRegistry.GetDetector(taskType)
		if detector != nil {
			// Convert scan interval to check interval (hours)
			if scanInterval := detector.ScanInterval(); scanInterval > 0 {
				taskPolicy.CheckInterval = int(scanInterval.Hours())
			}
		}

		policy.TaskPolicies[maintenanceTaskType] = taskPolicy
		glog.V(3).Infof("Built policy for task type %s: enabled=%v, max_concurrent=%d",
			maintenanceTaskType, taskPolicy.Enabled, taskPolicy.MaxConcurrent)
	}

	glog.V(2).Infof("Built maintenance policy with %d task configurations", len(policy.TaskPolicies))
	return policy
}

// SetPolicyFromTasks sets the maintenance policy from registered tasks
func SetPolicyFromTasks(policy *MaintenancePolicy) {
	if policy == nil {
		return
	}

	// Build new policy from tasks
	newPolicy := BuildMaintenancePolicyFromTasks()

	// Copy task policies
	policy.TaskPolicies = newPolicy.TaskPolicies

	glog.V(1).Infof("Updated maintenance policy with %d task configurations from registered tasks", len(policy.TaskPolicies))
}

// GetTaskIcon returns the icon CSS class for a task type from its UI provider
func GetTaskIcon(taskType MaintenanceTaskType) string {
	typesRegistry := tasks.GetGlobalTypesRegistry()
	uiRegistry := tasks.GetGlobalUIRegistry()

	// Convert MaintenanceTaskType to TaskType
	for workerTaskType := range typesRegistry.GetAllDetectors() {
		if string(workerTaskType) == string(taskType) {
			// Get the UI provider for this task type
			provider := uiRegistry.GetProvider(workerTaskType)
			if provider != nil {
				return provider.GetIcon()
			}
			break
		}
	}

	// Default icon if no UI provider found
	return "fas fa-cog text-muted"
}

// GetTaskDisplayName returns the display name for a task type from its UI provider
func GetTaskDisplayName(taskType MaintenanceTaskType) string {
	typesRegistry := tasks.GetGlobalTypesRegistry()
	uiRegistry := tasks.GetGlobalUIRegistry()

	// Convert MaintenanceTaskType to TaskType
	for workerTaskType := range typesRegistry.GetAllDetectors() {
		if string(workerTaskType) == string(taskType) {
			// Get the UI provider for this task type
			provider := uiRegistry.GetProvider(workerTaskType)
			if provider != nil {
				return provider.GetDisplayName()
			}
			break
		}
	}

	// Fallback to the task type string
	return string(taskType)
}

// GetTaskDescription returns the description for a task type from its UI provider
func GetTaskDescription(taskType MaintenanceTaskType) string {
	typesRegistry := tasks.GetGlobalTypesRegistry()
	uiRegistry := tasks.GetGlobalUIRegistry()

	// Convert MaintenanceTaskType to TaskType
	for workerTaskType := range typesRegistry.GetAllDetectors() {
		if string(workerTaskType) == string(taskType) {
			// Get the UI provider for this task type
			provider := uiRegistry.GetProvider(workerTaskType)
			if provider != nil {
				return provider.GetDescription()
			}
			break
		}
	}

	// Fallback to a generic description
	return "Configure detailed settings for " + string(taskType) + " tasks."
}

// BuildMaintenanceMenuItems creates menu items for all registered task types
func BuildMaintenanceMenuItems() []*MaintenanceMenuItem {
	var menuItems []*MaintenanceMenuItem

	// Get all registered task types
	registeredTypes := GetRegisteredMaintenanceTaskTypes()

	for _, taskType := range registeredTypes {
		menuItem := &MaintenanceMenuItem{
			TaskType:    taskType,
			DisplayName: GetTaskDisplayName(taskType),
			Description: GetTaskDescription(taskType),
			Icon:        GetTaskIcon(taskType),
			IsEnabled:   IsMaintenanceTaskTypeRegistered(taskType),
			Path:        "/maintenance/config/" + string(taskType),
		}

		menuItems = append(menuItems, menuItem)
	}

	return menuItems
}
