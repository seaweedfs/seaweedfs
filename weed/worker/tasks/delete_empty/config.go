package delete_empty

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
)

// Config holds compaction task settings, including the optional
// "delete empty volumes" sub-operation.
type Config struct {
	base.BaseConfig
	// DeleteEmptyEnabled controls whether the compaction task will delete
	// volumes that contain no needle data (size == superblock header only).
	DeleteEmptyEnabled bool `json:"delete_empty_enabled"`
	// QuietForSeconds is how long a volume must be unmodified before it is
	// considered safe to delete. Only used when DeleteEmptyEnabled is true.
	QuietForSeconds int `json:"quiet_for_seconds"`
}

// NewDefaultConfig creates a new default compaction configuration
func NewDefaultConfig() *Config {
	return &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 6 * 60 * 60, // 6 hours
			MaxConcurrent:       1,
		},
		DeleteEmptyEnabled: true,
		QuietForSeconds:    24 * 60 * 60, // 24 hours
	}
}

// ToTaskPolicy converts configuration to a TaskPolicy protobuf message.
// QuietForSeconds is stored in CheckIntervalSeconds so it survives a
// proto round-trip. DeleteEmptyEnabled is persisted via the full JSON
// config (SaveCompactionConfig / LoadCompactionConfig); see worker.proto
// TODO for the tracked typed-oneof approach.
func (c *Config) ToTaskPolicy() *worker_pb.TaskPolicy {
	return &worker_pb.TaskPolicy{
		Enabled:               c.Enabled,
		MaxConcurrent:         int32(c.MaxConcurrent),
		RepeatIntervalSeconds: int32(c.ScanIntervalSeconds),
		CheckIntervalSeconds:  int32(c.QuietForSeconds),
	}
}

// FromTaskPolicy loads fields from a TaskPolicy protobuf message.
// QuietForSeconds is recovered from CheckIntervalSeconds.
func (c *Config) FromTaskPolicy(policy *worker_pb.TaskPolicy) error {
	if policy == nil {
		return nil
	}
	c.Enabled = policy.Enabled
	c.MaxConcurrent = int(policy.MaxConcurrent)
	c.ScanIntervalSeconds = int(policy.RepeatIntervalSeconds)
	if policy.CheckIntervalSeconds > 0 {
		c.QuietForSeconds = int(policy.CheckIntervalSeconds)
	}
	return nil
}

// LoadConfigFromPersistence loads configuration from the persistence layer if available.
// It first tries a full JSON config (which carries all fields including
// DeleteEmptyEnabled) and falls back to the base TaskPolicy proto.
func LoadConfigFromPersistence(configPersistence interface{}) *Config {
	// Prefer the full JSON config — it carries DeleteEmptyEnabled and
	// QuietForSeconds without requiring proto regeneration.
	if persistence, ok := configPersistence.(interface {
		LoadCompactionConfig() (*Config, error)
	}); ok {
		if cfg, err := persistence.LoadCompactionConfig(); err == nil && cfg != nil {
			glog.V(1).Infof("Loaded compaction configuration from full JSON config")
			return cfg
		}
	}

	// Fallback: load from base TaskPolicy proto (QuietForSeconds via check_interval_seconds)
	cfg := NewDefaultConfig()
	if persistence, ok := configPersistence.(interface {
		LoadCompactionTaskPolicy() (*worker_pb.TaskPolicy, error)
	}); ok {
		if policy, err := persistence.LoadCompactionTaskPolicy(); err == nil && policy != nil {
			if err := cfg.FromTaskPolicy(policy); err == nil {
				glog.V(1).Infof("Loaded compaction configuration from persistence")
				return cfg
			}
		}
	}

	glog.V(1).Infof("Using default compaction configuration")
	return cfg
}

// GetConfigSpec returns the configuration schema for compaction tasks
func GetConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable Compaction",
				Description:  "Whether volume compaction tasks should run automatically",
				HelpText:     "Master switch for all compaction operations",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 6 * 60 * 60,
				MinValue:     60 * 60,
				MaxValue:     7 * 24 * 60 * 60,
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan volumes for compaction candidates",
				HelpText:     "The system will scan volumes at this interval",
				Placeholder:  "6",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "delete_empty_enabled",
				JSONName:     "delete_empty_enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Delete Empty Volumes",
				Description:  "Delete volumes that contain no data (zero-byte buckets)",
				HelpText:     "Removes volumes whose on-disk size equals the superblock header only. Requires Compaction to be enabled.",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "quiet_for_seconds",
				JSONName:     "quiet_for_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 24 * 60 * 60,
				MinValue:     60 * 60,
				MaxValue:     7 * 24 * 60 * 60,
				Required:     true,
				DisplayName:  "Empty Volume Quiet Period",
				Description:  "Only delete empty volumes that have not been modified for this duration",
				HelpText:     "Prevents deletion of freshly allocated volumes that may still receive data",
				Placeholder:  "24",
				Unit:         config.UnitHours,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
		},
	}
}
