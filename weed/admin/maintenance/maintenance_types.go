package maintenance

import (
	"html/template"
	"sort"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
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
	TypedParams *worker_pb.TaskParams   `json:"typed_params,omitempty"`
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

	// Enhanced fields for detailed task tracking
	CreatedBy         string                  `json:"created_by,omitempty"`         // Who/what created this task
	CreationContext   string                  `json:"creation_context,omitempty"`   // Additional context about creation
	AssignmentHistory []*TaskAssignmentRecord `json:"assignment_history,omitempty"` // History of worker assignments
	DetailedReason    string                  `json:"detailed_reason,omitempty"`    // More detailed explanation than Reason
	Tags              map[string]string       `json:"tags,omitempty"`               // Additional metadata tags
}

// TaskAssignmentRecord tracks when a task was assigned to a worker
type TaskAssignmentRecord struct {
	WorkerID      string     `json:"worker_id"`
	WorkerAddress string     `json:"worker_address"`
	AssignedAt    time.Time  `json:"assigned_at"`
	UnassignedAt  *time.Time `json:"unassigned_at,omitempty"`
	Reason        string     `json:"reason"` // Why was it assigned/unassigned
}

// TaskExecutionLog represents a log entry from task execution
type TaskExecutionLog struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"` // "info", "warn", "error", "debug"
	Message   string    `json:"message"`
	Source    string    `json:"source"` // Which component logged this
	TaskID    string    `json:"task_id"`
	WorkerID  string    `json:"worker_id"`
	// Optional structured fields carried from worker logs
	Fields map[string]string `json:"fields,omitempty"`
	// Optional progress/status carried from worker logs
	Progress *float64 `json:"progress,omitempty"`
	Status   string   `json:"status,omitempty"`
}

// TaskDetailData represents comprehensive information about a task for the detail view
type TaskDetailData struct {
	Task              *MaintenanceTask        `json:"task"`
	AssignmentHistory []*TaskAssignmentRecord `json:"assignment_history"`
	ExecutionLogs     []*TaskExecutionLog     `json:"execution_logs"`
	RelatedTasks      []*MaintenanceTask      `json:"related_tasks,omitempty"`    // Other tasks on same volume/server
	WorkerInfo        *MaintenanceWorker      `json:"worker_info,omitempty"`      // Current or last assigned worker
	CreationMetrics   *TaskCreationMetrics    `json:"creation_metrics,omitempty"` // Metrics that led to task creation
	LastUpdated       time.Time               `json:"last_updated"`
}

// TaskCreationMetrics holds metrics that led to the task being created
type TaskCreationMetrics struct {
	TriggerMetric  string                 `json:"trigger_metric"` // What metric triggered this task
	MetricValue    float64                `json:"metric_value"`   // Value of the trigger metric
	Threshold      float64                `json:"threshold"`      // Threshold that was exceeded
	VolumeMetrics  *VolumeHealthMetrics   `json:"volume_metrics,omitempty"`
	AdditionalData map[string]interface{} `json:"additional_data,omitempty"`
}

// MaintenanceConfig holds configuration for the maintenance system
// DEPRECATED: Use worker_pb.MaintenanceConfig instead
type MaintenanceConfig = worker_pb.MaintenanceConfig

// MaintenancePolicy defines policies for maintenance operations
// DEPRECATED: Use worker_pb.MaintenancePolicy instead
type MaintenancePolicy = worker_pb.MaintenancePolicy

// TaskPolicy represents configuration for a specific task type
// DEPRECATED: Use worker_pb.TaskPolicy instead
type TaskPolicy = worker_pb.TaskPolicy

// TaskPersistence interface for task state persistence
type TaskPersistence interface {
	SaveTaskState(task *MaintenanceTask) error
	LoadTaskState(taskID string) (*MaintenanceTask, error)
	LoadAllTaskStates() ([]*MaintenanceTask, error)
	DeleteTaskState(taskID string) error
	CleanupCompletedTasks() error
}

// Default configuration values
func DefaultMaintenanceConfig() *MaintenanceConfig {
	return DefaultMaintenanceConfigProto()
}

// Policy helper functions (since we can't add methods to type aliases)

// GetTaskPolicy returns the policy for a specific task type
func GetTaskPolicy(mp *MaintenancePolicy, taskType MaintenanceTaskType) *TaskPolicy {
	if mp.TaskPolicies == nil {
		return nil
	}
	return mp.TaskPolicies[string(taskType)]
}

// SetTaskPolicy sets the policy for a specific task type
func SetTaskPolicy(mp *MaintenancePolicy, taskType MaintenanceTaskType, policy *TaskPolicy) {
	if mp.TaskPolicies == nil {
		mp.TaskPolicies = make(map[string]*TaskPolicy)
	}
	mp.TaskPolicies[string(taskType)] = policy
}

// IsTaskEnabled returns whether a task type is enabled
func IsTaskEnabled(mp *MaintenancePolicy, taskType MaintenanceTaskType) bool {
	policy := GetTaskPolicy(mp, taskType)
	if policy == nil {
		return false
	}
	return policy.Enabled
}

// GetMaxConcurrent returns the max concurrent limit for a task type
func GetMaxConcurrent(mp *MaintenancePolicy, taskType MaintenanceTaskType) int {
	policy := GetTaskPolicy(mp, taskType)
	if policy == nil {
		return 1
	}
	return int(policy.MaxConcurrent)
}

// GetRepeatInterval returns the repeat interval for a task type
func GetRepeatInterval(mp *MaintenancePolicy, taskType MaintenanceTaskType) int {
	policy := GetTaskPolicy(mp, taskType)
	if policy == nil {
		return int(mp.DefaultRepeatIntervalSeconds)
	}
	return int(policy.RepeatIntervalSeconds)
}

// GetVacuumTaskConfig returns the vacuum task configuration
func GetVacuumTaskConfig(mp *MaintenancePolicy, taskType MaintenanceTaskType) *worker_pb.VacuumTaskConfig {
	policy := GetTaskPolicy(mp, taskType)
	if policy == nil {
		return nil
	}
	return policy.GetVacuumConfig()
}

// GetErasureCodingTaskConfig returns the erasure coding task configuration
func GetErasureCodingTaskConfig(mp *MaintenancePolicy, taskType MaintenanceTaskType) *worker_pb.ErasureCodingTaskConfig {
	policy := GetTaskPolicy(mp, taskType)
	if policy == nil {
		return nil
	}
	return policy.GetErasureCodingConfig()
}

// GetBalanceTaskConfig returns the balance task configuration
func GetBalanceTaskConfig(mp *MaintenancePolicy, taskType MaintenanceTaskType) *worker_pb.BalanceTaskConfig {
	policy := GetTaskPolicy(mp, taskType)
	if policy == nil {
		return nil
	}
	return policy.GetBalanceConfig()
}

// GetReplicationTaskConfig returns the replication task configuration
func GetReplicationTaskConfig(mp *MaintenancePolicy, taskType MaintenanceTaskType) *worker_pb.ReplicationTaskConfig {
	policy := GetTaskPolicy(mp, taskType)
	if policy == nil {
		return nil
	}
	return policy.GetReplicationConfig()
}

// Note: GetTaskConfig was removed - use typed getters: GetVacuumTaskConfig, GetErasureCodingTaskConfig, GetBalanceTaskConfig, or GetReplicationTaskConfig

// SetVacuumTaskConfig sets the vacuum task configuration
func SetVacuumTaskConfig(mp *MaintenancePolicy, taskType MaintenanceTaskType, config *worker_pb.VacuumTaskConfig) {
	policy := GetTaskPolicy(mp, taskType)
	if policy != nil {
		policy.TaskConfig = &worker_pb.TaskPolicy_VacuumConfig{
			VacuumConfig: config,
		}
	}
}

// SetErasureCodingTaskConfig sets the erasure coding task configuration
func SetErasureCodingTaskConfig(mp *MaintenancePolicy, taskType MaintenanceTaskType, config *worker_pb.ErasureCodingTaskConfig) {
	policy := GetTaskPolicy(mp, taskType)
	if policy != nil {
		policy.TaskConfig = &worker_pb.TaskPolicy_ErasureCodingConfig{
			ErasureCodingConfig: config,
		}
	}
}

// SetBalanceTaskConfig sets the balance task configuration
func SetBalanceTaskConfig(mp *MaintenancePolicy, taskType MaintenanceTaskType, config *worker_pb.BalanceTaskConfig) {
	policy := GetTaskPolicy(mp, taskType)
	if policy != nil {
		policy.TaskConfig = &worker_pb.TaskPolicy_BalanceConfig{
			BalanceConfig: config,
		}
	}
}

// SetReplicationTaskConfig sets the replication task configuration
func SetReplicationTaskConfig(mp *MaintenancePolicy, taskType MaintenanceTaskType, config *worker_pb.ReplicationTaskConfig) {
	policy := GetTaskPolicy(mp, taskType)
	if policy != nil {
		policy.TaskConfig = &worker_pb.TaskPolicy_ReplicationConfig{
			ReplicationConfig: config,
		}
	}
}

// SetTaskConfig sets a configuration value for a task type (legacy method - use typed setters above)
// Note: SetTaskConfig was removed - use typed setters: SetVacuumTaskConfig, SetErasureCodingTaskConfig, SetBalanceTaskConfig, or SetReplicationTaskConfig

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
	persistence  TaskPersistence // Interface for task persistence
}

// MaintenanceScanner analyzes the cluster and generates maintenance tasks
type MaintenanceScanner struct {
	adminClient      AdminClient
	policy           *MaintenancePolicy
	queue            *MaintenanceQueue
	lastScan         map[MaintenanceTaskType]time.Time
	integration      *MaintenanceIntegration
	lastTopologyInfo *master_pb.TopologyInfo
}

// TaskDetectionResult represents the result of scanning for maintenance needs
type TaskDetectionResult struct {
	TaskType    MaintenanceTaskType     `json:"task_type"`
	VolumeID    uint32                  `json:"volume_id,omitempty"`
	Server      string                  `json:"server,omitempty"`
	Collection  string                  `json:"collection,omitempty"`
	Priority    MaintenanceTaskPriority `json:"priority"`
	Reason      string                  `json:"reason"`
	TypedParams *worker_pb.TaskParams   `json:"typed_params,omitempty"`
	ScheduleAt  time.Time               `json:"schedule_at"`
}

// VolumeHealthMetrics represents the health metrics for a volume
type VolumeHealthMetrics struct {
	VolumeID         uint32        `json:"volume_id"`
	Server           string        `json:"server"`
	DiskType         string        `json:"disk_type"`   // Disk type (e.g., "hdd", "ssd") or disk path (e.g., "/data1")
	DiskId           uint32        `json:"disk_id"`     // ID of the disk in Store.Locations array
	DataCenter       string        `json:"data_center"` // Data center of the server
	Rack             string        `json:"rack"`        // Rack of the server
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
		TaskPolicies:                 make(map[string]*TaskPolicy),
		GlobalMaxConcurrent:          4,
		DefaultRepeatIntervalSeconds: 6 * 3600,  // 6 hours in seconds
		DefaultCheckIntervalSeconds:  12 * 3600, // 12 hours in seconds
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
			Enabled:               true, // Default enabled
			MaxConcurrent:         2,    // Default concurrency
			RepeatIntervalSeconds: policy.DefaultRepeatIntervalSeconds,
			CheckIntervalSeconds:  policy.DefaultCheckIntervalSeconds,
		}

		// Extract configuration using TaskConfig interface - no more map conversions!
		if taskConfig, ok := defaultConfig.(interface{ ToTaskPolicy() *worker_pb.TaskPolicy }); ok {
			// Use protobuf directly for clean, type-safe config extraction
			pbTaskPolicy := taskConfig.ToTaskPolicy()
			taskPolicy.Enabled = pbTaskPolicy.Enabled
			taskPolicy.MaxConcurrent = pbTaskPolicy.MaxConcurrent
			if pbTaskPolicy.RepeatIntervalSeconds > 0 {
				taskPolicy.RepeatIntervalSeconds = pbTaskPolicy.RepeatIntervalSeconds
			}
			if pbTaskPolicy.CheckIntervalSeconds > 0 {
				taskPolicy.CheckIntervalSeconds = pbTaskPolicy.CheckIntervalSeconds
			}
		}

		// Also get defaults from scheduler if available (using types.TaskScheduler explicitly)
		var scheduler types.TaskScheduler = typesRegistry.GetScheduler(taskType)
		if scheduler != nil {
			if taskPolicy.MaxConcurrent <= 0 {
				taskPolicy.MaxConcurrent = int32(scheduler.GetMaxConcurrent())
			}
			// Convert default repeat interval to seconds
			if repeatInterval := scheduler.GetDefaultRepeatInterval(); repeatInterval > 0 {
				taskPolicy.RepeatIntervalSeconds = int32(repeatInterval.Seconds())
			}
		}

		// Also get defaults from detector if available (using types.TaskDetector explicitly)
		var detector types.TaskDetector = typesRegistry.GetDetector(taskType)
		if detector != nil {
			// Convert scan interval to check interval (seconds)
			if scanInterval := detector.ScanInterval(); scanInterval > 0 {
				taskPolicy.CheckIntervalSeconds = int32(scanInterval.Seconds())
			}
		}

		policy.TaskPolicies[string(maintenanceTaskType)] = taskPolicy
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

// Helper functions to extract configuration fields

// Note: Removed getVacuumConfigField, getErasureCodingConfigField, getBalanceConfigField, getReplicationConfigField
// These were orphaned after removing GetTaskConfig - use typed getters instead
