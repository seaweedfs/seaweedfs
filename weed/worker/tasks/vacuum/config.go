package vacuum

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
)

// Config extends BaseConfig with vacuum-specific settings
type Config struct {
	base.BaseConfig
	GarbageThreshold    float64 `json:"garbage_threshold"`
	MinVolumeAgeSeconds int     `json:"min_volume_age_seconds"`
	MinIntervalSeconds  int     `json:"min_interval_seconds"`
}

// NewDefaultConfig creates a new default vacuum configuration
func NewDefaultConfig() *Config {
	return &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 2 * 60 * 60, // 2 hours
			MaxConcurrent:       2,
		},
		GarbageThreshold:    0.3,              // 30%
		MinVolumeAgeSeconds: 24 * 60 * 60,     // 24 hours
		MinIntervalSeconds:  7 * 24 * 60 * 60, // 7 days
	}
}

// ToTaskPolicy converts configuration to a TaskPolicy protobuf message
func (c *Config) ToTaskPolicy() *worker_pb.TaskPolicy {
	return &worker_pb.TaskPolicy{
		Enabled:               c.Enabled,
		MaxConcurrent:         int32(c.MaxConcurrent),
		RepeatIntervalSeconds: int32(c.ScanIntervalSeconds),
		CheckIntervalSeconds:  int32(c.ScanIntervalSeconds),
		TaskConfig: &worker_pb.TaskPolicy_VacuumConfig{
			VacuumConfig: &worker_pb.VacuumTaskConfig{
				GarbageThreshold:   float64(c.GarbageThreshold),
				MinVolumeAgeHours:  int32(c.MinVolumeAgeSeconds / 3600), // Convert seconds to hours
				MinIntervalSeconds: int32(c.MinIntervalSeconds),
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
	c.ScanIntervalSeconds = int(policy.RepeatIntervalSeconds) // Direct seconds-to-seconds mapping

	// Set vacuum-specific fields from the task config
	if vacuumConfig := policy.GetVacuumConfig(); vacuumConfig != nil {
		c.GarbageThreshold = float64(vacuumConfig.GarbageThreshold)
		c.MinVolumeAgeSeconds = int(vacuumConfig.MinVolumeAgeHours * 3600) // Convert hours to seconds
		c.MinIntervalSeconds = int(vacuumConfig.MinIntervalSeconds)
	}

	return nil
}

// LoadConfigFromPersistence loads configuration from the persistence layer if available
func LoadConfigFromPersistence(configPersistence interface{}) *Config {
	config := NewDefaultConfig()

	// Try to load from persistence if available
	if persistence, ok := configPersistence.(interface {
		LoadVacuumTaskPolicy() (*worker_pb.TaskPolicy, error)
	}); ok {
		if policy, err := persistence.LoadVacuumTaskPolicy(); err == nil && policy != nil {
			if err := config.FromTaskPolicy(policy); err == nil {
				glog.V(1).Infof("Loaded vacuum configuration from persistence")
				return config
			}
		}
	}

	glog.V(1).Infof("Using default vacuum configuration")
	return config
}

// GetConfigSpec returns the configuration schema for vacuum tasks
func GetConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable Vacuum Tasks",
				Description:  "Whether vacuum tasks should be automatically created",
				HelpText:     "Toggle this to enable or disable automatic vacuum task generation",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 2 * 60 * 60,
				MinValue:     10 * 60,
				MaxValue:     24 * 60 * 60,
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for volumes needing vacuum",
				HelpText:     "The system will check for volumes that need vacuuming at this interval",
				Placeholder:  "2",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "max_concurrent",
				JSONName:     "max_concurrent",
				Type:         config.FieldTypeInt,
				DefaultValue: 2,
				MinValue:     1,
				MaxValue:     10,
				Required:     true,
				DisplayName:  "Max Concurrent Tasks",
				Description:  "Maximum number of vacuum tasks that can run simultaneously",
				HelpText:     "Limits the number of vacuum operations running at the same time to control system load",
				Placeholder:  "2 (default)",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "garbage_threshold",
				JSONName:     "garbage_threshold",
				Type:         config.FieldTypeFloat,
				DefaultValue: 0.3,
				MinValue:     0.0,
				MaxValue:     1.0,
				Required:     true,
				DisplayName:  "Garbage Percentage Threshold",
				Description:  "Trigger vacuum when garbage ratio exceeds this percentage",
				HelpText:     "Volumes with more deleted content than this threshold will be vacuumed",
				Placeholder:  "0.30 (30%)",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_volume_age_seconds",
				JSONName:     "min_volume_age_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 24 * 60 * 60,
				MinValue:     1 * 60 * 60,
				MaxValue:     7 * 24 * 60 * 60,
				Required:     true,
				DisplayName:  "Minimum Volume Age",
				Description:  "Only vacuum volumes older than this duration",
				HelpText:     "Prevents vacuuming of recently created volumes that may still be actively written to",
				Placeholder:  "24",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_interval_seconds",
				JSONName:     "min_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 7 * 24 * 60 * 60,
				MinValue:     1 * 24 * 60 * 60,
				MaxValue:     30 * 24 * 60 * 60,
				Required:     true,
				DisplayName:  "Minimum Interval",
				Description:  "Minimum time between vacuum operations on the same volume",
				HelpText:     "Prevents excessive vacuuming of the same volume by enforcing a minimum wait time",
				Placeholder:  "7",
				Unit:         config.UnitDays,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
		},
	}
}
