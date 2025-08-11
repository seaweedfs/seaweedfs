package ec_vacuum

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
)

// Config extends BaseConfig with EC vacuum specific settings
type Config struct {
	base.BaseConfig
	DeletionThreshold   float64 `json:"deletion_threshold"`     // Minimum deletion ratio to trigger vacuum
	MinVolumeAgeSeconds int     `json:"min_volume_age_seconds"` // Minimum age before considering vacuum (in seconds)
	CollectionFilter    string  `json:"collection_filter"`      // Filter by collection
	MinSizeMB           int     `json:"min_size_mb"`            // Minimum original volume size
}

// NewDefaultConfig creates a new default EC vacuum configuration
func NewDefaultConfig() *Config {
	return &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 24 * 60 * 60, // 24 hours
			MaxConcurrent:       1,
		},
		DeletionThreshold:   0.3,          // 30% deletions trigger vacuum
		MinVolumeAgeSeconds: 72 * 60 * 60, // 3 days minimum age (72 hours in seconds)
		CollectionFilter:    "",           // No filter by default
		MinSizeMB:           100,          // 100MB minimum size
	}
}

// GetConfigSpec returns the configuration schema for EC vacuum tasks
func GetConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable EC Vacuum Tasks",
				Description:  "Whether EC vacuum tasks should be automatically created",
				HelpText:     "Toggle this to enable or disable automatic EC vacuum task generation",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 24 * 60 * 60,
				MinValue:     6 * 60 * 60,      // 6 hours minimum
				MaxValue:     7 * 24 * 60 * 60, // 7 days maximum
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for EC volumes needing vacuum",
				HelpText:     "The system will check for EC volumes with deletions at this interval",
				Placeholder:  "24",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "max_concurrent",
				JSONName:     "max_concurrent",
				Type:         config.FieldTypeInt,
				DefaultValue: 1,
				MinValue:     1,
				MaxValue:     3,
				Required:     true,
				DisplayName:  "Max Concurrent Tasks",
				Description:  "Maximum number of EC vacuum tasks that can run simultaneously",
				HelpText:     "Limits the number of EC vacuum operations running at the same time",
				Placeholder:  "1 (default)",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "deletion_threshold",
				JSONName:     "deletion_threshold",
				Type:         config.FieldTypeFloat,
				DefaultValue: 0.3,
				MinValue:     0.1,
				MaxValue:     0.8,
				Required:     true,
				DisplayName:  "Deletion Threshold",
				Description:  "Minimum ratio of deletions to trigger vacuum",
				HelpText:     "EC volumes with this ratio of deleted content will be vacuumed",
				Placeholder:  "0.3 (30%)",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_volume_age_seconds",
				JSONName:     "min_volume_age_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 72 * 60 * 60,      // 72 hours in seconds
				MinValue:     24 * 60 * 60,      // 24 hours in seconds
				MaxValue:     30 * 24 * 60 * 60, // 30 days in seconds
				Required:     true,
				DisplayName:  "Minimum Volume Age",
				Description:  "Minimum age before considering EC volume for vacuum",
				HelpText:     "Only EC volumes older than this will be considered for vacuum",
				Placeholder:  "72",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "collection_filter",
				JSONName:     "collection_filter",
				Type:         config.FieldTypeString,
				DefaultValue: "",
				Required:     false,
				DisplayName:  "Collection Filter",
				Description:  "Only vacuum EC volumes in this collection (empty = all collections)",
				HelpText:     "Leave empty to vacuum EC volumes in all collections",
				Placeholder:  "e.g., 'logs' or leave empty",
				Unit:         config.UnitNone,
				InputType:    "text",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_size_mb",
				JSONName:     "min_size_mb",
				Type:         config.FieldTypeInt,
				DefaultValue: 100,
				MinValue:     10,
				MaxValue:     10000,
				Required:     true,
				DisplayName:  "Minimum Size (MB)",
				Description:  "Minimum original EC volume size to consider for vacuum",
				HelpText:     "Only EC volumes larger than this size will be considered for vacuum",
				Placeholder:  "100",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
		},
	}
}

// ToTaskPolicy converts configuration to a TaskPolicy protobuf message
func (c *Config) ToTaskPolicy() *worker_pb.TaskPolicy {
	return &worker_pb.TaskPolicy{
		Enabled:               c.Enabled,
		MaxConcurrent:         int32(c.MaxConcurrent),
		RepeatIntervalSeconds: int32(c.ScanIntervalSeconds),
		CheckIntervalSeconds:  int32(c.ScanIntervalSeconds),
		TaskConfig: &worker_pb.TaskPolicy_EcVacuumConfig{
			EcVacuumConfig: &worker_pb.EcVacuumTaskConfig{
				DeletionThreshold:   c.DeletionThreshold,
				MinVolumeAgeSeconds: int32(c.MinVolumeAgeSeconds),
				CollectionFilter:    c.CollectionFilter,
				MinSizeMb:           int32(c.MinSizeMB),
			},
		},
	}
}

// FromTaskPolicy loads configuration from a TaskPolicy protobuf message
func (c *Config) FromTaskPolicy(policy *worker_pb.TaskPolicy) error {
	if policy == nil {
		return fmt.Errorf("policy is nil")
	}

	// Set general TaskPolicy fields
	c.Enabled = policy.Enabled
	c.MaxConcurrent = int(policy.MaxConcurrent)
	c.ScanIntervalSeconds = int(policy.RepeatIntervalSeconds)

	// Load EC vacuum-specific fields from TaskConfig field
	if ecVacuumConfig := policy.GetEcVacuumConfig(); ecVacuumConfig != nil {
		c.DeletionThreshold = ecVacuumConfig.DeletionThreshold
		c.MinVolumeAgeSeconds = int(ecVacuumConfig.MinVolumeAgeSeconds)
		c.CollectionFilter = ecVacuumConfig.CollectionFilter
		c.MinSizeMB = int(ecVacuumConfig.MinSizeMb)
	}
	// If no EcVacuumConfig found, keep existing values (defaults)

	return nil
}

// LoadConfigFromPersistence loads configuration from the persistence layer if available
func LoadConfigFromPersistence(configPersistence interface{}) *Config {
	config := NewDefaultConfig()

	// Try to load from persistence if available using generic method
	if persistence, ok := configPersistence.(interface {
		LoadTaskPolicyGeneric(taskType string) (*worker_pb.TaskPolicy, error)
	}); ok {
		if policy, err := persistence.LoadTaskPolicyGeneric("ec_vacuum"); err == nil && policy != nil {
			if err := config.FromTaskPolicy(policy); err == nil {
				glog.V(1).Infof("Loaded EC vacuum configuration from persistence")
				return config
			}
		}
	}

	glog.V(1).Infof("Using default EC vacuum configuration")
	return config
}
