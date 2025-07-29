package maintenance

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// MaintenanceConfigManager handles protobuf-based configuration
type MaintenanceConfigManager struct {
	config *worker_pb.MaintenanceConfig
}

// NewMaintenanceConfigManager creates a new config manager with defaults
func NewMaintenanceConfigManager() *MaintenanceConfigManager {
	return &MaintenanceConfigManager{
		config: DefaultMaintenanceConfigProto(),
	}
}

// DefaultMaintenanceConfigProto returns default configuration as protobuf
func DefaultMaintenanceConfigProto() *worker_pb.MaintenanceConfig {
	return &worker_pb.MaintenanceConfig{
		Enabled:                true,
		ScanIntervalSeconds:    30 * 60,     // 30 minutes
		WorkerTimeoutSeconds:   5 * 60,      // 5 minutes
		TaskTimeoutSeconds:     2 * 60 * 60, // 2 hours
		RetryDelaySeconds:      15 * 60,     // 15 minutes
		MaxRetries:             3,
		CleanupIntervalSeconds: 24 * 60 * 60,     // 24 hours
		TaskRetentionSeconds:   7 * 24 * 60 * 60, // 7 days
		// Policy field will be populated dynamically from separate task configuration files
		Policy: nil,
	}
}

// GetConfig returns the current configuration
func (mcm *MaintenanceConfigManager) GetConfig() *worker_pb.MaintenanceConfig {
	return mcm.config
}

// Type-safe configuration accessors

// GetVacuumConfig returns vacuum-specific configuration for a task type
func (mcm *MaintenanceConfigManager) GetVacuumConfig(taskType string) *worker_pb.VacuumTaskConfig {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		if vacuumConfig := policy.GetVacuumConfig(); vacuumConfig != nil {
			return vacuumConfig
		}
	}
	// Return defaults if not configured
	return &worker_pb.VacuumTaskConfig{
		GarbageThreshold:   0.3,
		MinVolumeAgeHours:  24,
		MinIntervalSeconds: 7 * 24 * 60 * 60, // 7 days
	}
}

// GetErasureCodingConfig returns EC-specific configuration for a task type
func (mcm *MaintenanceConfigManager) GetErasureCodingConfig(taskType string) *worker_pb.ErasureCodingTaskConfig {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		if ecConfig := policy.GetErasureCodingConfig(); ecConfig != nil {
			return ecConfig
		}
	}
	// Return defaults if not configured
	return &worker_pb.ErasureCodingTaskConfig{
		FullnessRatio:    0.95,
		QuietForSeconds:  3600,
		MinVolumeSizeMb:  100,
		CollectionFilter: "",
	}
}

// GetBalanceConfig returns balance-specific configuration for a task type
func (mcm *MaintenanceConfigManager) GetBalanceConfig(taskType string) *worker_pb.BalanceTaskConfig {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		if balanceConfig := policy.GetBalanceConfig(); balanceConfig != nil {
			return balanceConfig
		}
	}
	// Return defaults if not configured
	return &worker_pb.BalanceTaskConfig{
		ImbalanceThreshold: 0.2,
		MinServerCount:     2,
	}
}

// GetReplicationConfig returns replication-specific configuration for a task type
func (mcm *MaintenanceConfigManager) GetReplicationConfig(taskType string) *worker_pb.ReplicationTaskConfig {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		if replicationConfig := policy.GetReplicationConfig(); replicationConfig != nil {
			return replicationConfig
		}
	}
	// Return defaults if not configured
	return &worker_pb.ReplicationTaskConfig{
		TargetReplicaCount: 2,
	}
}

// Typed convenience methods for getting task configurations

// GetVacuumTaskConfigForType returns vacuum configuration for a specific task type
func (mcm *MaintenanceConfigManager) GetVacuumTaskConfigForType(taskType string) *worker_pb.VacuumTaskConfig {
	return GetVacuumTaskConfig(mcm.config.Policy, MaintenanceTaskType(taskType))
}

// GetErasureCodingTaskConfigForType returns erasure coding configuration for a specific task type
func (mcm *MaintenanceConfigManager) GetErasureCodingTaskConfigForType(taskType string) *worker_pb.ErasureCodingTaskConfig {
	return GetErasureCodingTaskConfig(mcm.config.Policy, MaintenanceTaskType(taskType))
}

// GetBalanceTaskConfigForType returns balance configuration for a specific task type
func (mcm *MaintenanceConfigManager) GetBalanceTaskConfigForType(taskType string) *worker_pb.BalanceTaskConfig {
	return GetBalanceTaskConfig(mcm.config.Policy, MaintenanceTaskType(taskType))
}

// GetReplicationTaskConfigForType returns replication configuration for a specific task type
func (mcm *MaintenanceConfigManager) GetReplicationTaskConfigForType(taskType string) *worker_pb.ReplicationTaskConfig {
	return GetReplicationTaskConfig(mcm.config.Policy, MaintenanceTaskType(taskType))
}

// Helper methods

func (mcm *MaintenanceConfigManager) getTaskPolicy(taskType string) *worker_pb.TaskPolicy {
	if mcm.config.Policy != nil && mcm.config.Policy.TaskPolicies != nil {
		return mcm.config.Policy.TaskPolicies[taskType]
	}
	return nil
}

// IsTaskEnabled returns whether a task type is enabled
func (mcm *MaintenanceConfigManager) IsTaskEnabled(taskType string) bool {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		return policy.Enabled
	}
	return false
}

// GetMaxConcurrent returns the max concurrent limit for a task type
func (mcm *MaintenanceConfigManager) GetMaxConcurrent(taskType string) int32 {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		return policy.MaxConcurrent
	}
	return 1 // Default
}

// GetRepeatInterval returns the repeat interval for a task type in seconds
func (mcm *MaintenanceConfigManager) GetRepeatInterval(taskType string) int32 {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		return policy.RepeatIntervalSeconds
	}
	return mcm.config.Policy.DefaultRepeatIntervalSeconds
}

// GetCheckInterval returns the check interval for a task type in seconds
func (mcm *MaintenanceConfigManager) GetCheckInterval(taskType string) int32 {
	if policy := mcm.getTaskPolicy(taskType); policy != nil {
		return policy.CheckIntervalSeconds
	}
	return mcm.config.Policy.DefaultCheckIntervalSeconds
}

// Duration accessor methods

// GetScanInterval returns the scan interval as a time.Duration
func (mcm *MaintenanceConfigManager) GetScanInterval() time.Duration {
	return time.Duration(mcm.config.ScanIntervalSeconds) * time.Second
}

// GetWorkerTimeout returns the worker timeout as a time.Duration
func (mcm *MaintenanceConfigManager) GetWorkerTimeout() time.Duration {
	return time.Duration(mcm.config.WorkerTimeoutSeconds) * time.Second
}

// GetTaskTimeout returns the task timeout as a time.Duration
func (mcm *MaintenanceConfigManager) GetTaskTimeout() time.Duration {
	return time.Duration(mcm.config.TaskTimeoutSeconds) * time.Second
}

// GetRetryDelay returns the retry delay as a time.Duration
func (mcm *MaintenanceConfigManager) GetRetryDelay() time.Duration {
	return time.Duration(mcm.config.RetryDelaySeconds) * time.Second
}

// GetCleanupInterval returns the cleanup interval as a time.Duration
func (mcm *MaintenanceConfigManager) GetCleanupInterval() time.Duration {
	return time.Duration(mcm.config.CleanupIntervalSeconds) * time.Second
}

// GetTaskRetention returns the task retention period as a time.Duration
func (mcm *MaintenanceConfigManager) GetTaskRetention() time.Duration {
	return time.Duration(mcm.config.TaskRetentionSeconds) * time.Second
}

// ValidateMaintenanceConfigWithSchema validates protobuf maintenance configuration using ConfigField rules
func ValidateMaintenanceConfigWithSchema(config *worker_pb.MaintenanceConfig) error {
	if config == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	// Get the schema to access field validation rules
	schema := GetMaintenanceConfigSchema()

	// Validate each field individually using the ConfigField rules
	if err := validateFieldWithSchema(schema, "enabled", config.Enabled); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "scan_interval_seconds", int(config.ScanIntervalSeconds)); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "worker_timeout_seconds", int(config.WorkerTimeoutSeconds)); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "task_timeout_seconds", int(config.TaskTimeoutSeconds)); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "retry_delay_seconds", int(config.RetryDelaySeconds)); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "max_retries", int(config.MaxRetries)); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "cleanup_interval_seconds", int(config.CleanupIntervalSeconds)); err != nil {
		return err
	}

	if err := validateFieldWithSchema(schema, "task_retention_seconds", int(config.TaskRetentionSeconds)); err != nil {
		return err
	}

	// Validate policy fields if present
	if config.Policy != nil {
		// Note: These field names might need to be adjusted based on the actual schema
		if err := validatePolicyField("global_max_concurrent", int(config.Policy.GlobalMaxConcurrent)); err != nil {
			return err
		}

		if err := validatePolicyField("default_repeat_interval_seconds", int(config.Policy.DefaultRepeatIntervalSeconds)); err != nil {
			return err
		}

		if err := validatePolicyField("default_check_interval_seconds", int(config.Policy.DefaultCheckIntervalSeconds)); err != nil {
			return err
		}
	}

	return nil
}

// validateFieldWithSchema validates a single field using its ConfigField definition
func validateFieldWithSchema(schema *MaintenanceConfigSchema, fieldName string, value interface{}) error {
	field := schema.GetFieldByName(fieldName)
	if field == nil {
		// Field not in schema, skip validation
		return nil
	}

	return field.ValidateValue(value)
}

// validatePolicyField validates policy fields (simplified validation for now)
func validatePolicyField(fieldName string, value int) error {
	switch fieldName {
	case "global_max_concurrent":
		if value < 1 || value > 20 {
			return fmt.Errorf("Global Max Concurrent must be between 1 and 20, got %d", value)
		}
	case "default_repeat_interval":
		if value < 1 || value > 168 {
			return fmt.Errorf("Default Repeat Interval must be between 1 and 168 hours, got %d", value)
		}
	case "default_check_interval":
		if value < 1 || value > 168 {
			return fmt.Errorf("Default Check Interval must be between 1 and 168 hours, got %d", value)
		}
	}
	return nil
}
