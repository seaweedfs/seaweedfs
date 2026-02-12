package table_maintenance

import (
	"encoding/json"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
)

// Config extends BaseConfig with table maintenance specific settings
type Config struct {
	base.BaseConfig
	// ScanIntervalMinutes is how often to scan for maintenance needs
	ScanIntervalMinutes int `json:"scan_interval_minutes"`
	// CompactionFileThreshold is the minimum number of data files before compaction is triggered
	CompactionFileThreshold int `json:"compaction_file_threshold"`
	// SnapshotRetentionDays is how long to keep snapshots before expiration
	SnapshotRetentionDays int `json:"snapshot_retention_days"`
}

// NewDefaultConfig returns the default configuration for table maintenance
func NewDefaultConfig() *Config {
	return &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 24 * 60 * 60, // 24 hours
			MaxConcurrent:       2,
		},
		ScanIntervalMinutes:     30,  // Scan every 30 minutes
		CompactionFileThreshold: 100, // Compact when > 100 small files
		SnapshotRetentionDays:   7,   // Keep snapshots for 7 days
	}
}

// ToTaskPolicy converts configuration to a TaskPolicy protobuf message
func (c *Config) ToTaskPolicy() *worker_pb.TaskPolicy {
	policy := &worker_pb.TaskPolicy{
		Enabled:               c.Enabled,
		MaxConcurrent:         int32(c.MaxConcurrent),
		RepeatIntervalSeconds: int32(c.ScanIntervalSeconds),
		CheckIntervalSeconds:  int32(c.ScanIntervalMinutes * 60),
	}

	// Encode config-specific fields in Description as JSON
	configData := map[string]interface{}{
		"compaction_file_threshold": c.CompactionFileThreshold,
		"snapshot_retention_days":   c.SnapshotRetentionDays,
	}
	if data, err := json.Marshal(configData); err == nil {
		policy.Description = string(data)
	}

	return policy
}

// FromTaskPolicy loads configuration from a TaskPolicy protobuf message
func (c *Config) FromTaskPolicy(policy *worker_pb.TaskPolicy) error {
	if policy == nil {
		return fmt.Errorf("policy is nil")
	}

	c.Enabled = policy.Enabled
	c.MaxConcurrent = int(policy.MaxConcurrent)
	c.ScanIntervalSeconds = int(policy.RepeatIntervalSeconds)
	c.ScanIntervalMinutes = int(policy.CheckIntervalSeconds / 60)

	// Decode config-specific fields from Description if present
	if policy.Description != "" {
		var configData map[string]interface{}
		if err := json.Unmarshal([]byte(policy.Description), &configData); err == nil {
			if val, ok := configData["compaction_file_threshold"]; ok {
				if floatVal, ok := val.(float64); ok {
					c.CompactionFileThreshold = int(floatVal)
				}
			}
			if val, ok := configData["snapshot_retention_days"]; ok {
				if floatVal, ok := val.(float64); ok {
					c.SnapshotRetentionDays = int(floatVal)
				}
			}
		}
	}

	return nil
}

// LoadConfigFromPersistence loads configuration from the persistence layer if available
func LoadConfigFromPersistence(configPersistence interface{}) *Config {
	config := NewDefaultConfig()

	// Try to load from persistence if available
	if persistence, ok := configPersistence.(interface {
		LoadTableMaintenanceTaskPolicy() (*worker_pb.TaskPolicy, error)
	}); ok {
		if policy, err := persistence.LoadTableMaintenanceTaskPolicy(); err != nil {
			glog.Warningf("Failed to load table_maintenance configuration from persistence: %v", err)
		} else if policy != nil {
			if err := config.FromTaskPolicy(policy); err != nil {
				glog.Warningf("Failed to parse table_maintenance configuration from persistence: %v", err)
			} else {
				glog.V(1).Infof("Loaded table_maintenance configuration from persistence")
				return config
			}
		}
	}

	glog.V(1).Infof("Using default table_maintenance configuration")
	return config
}

// GetConfigSpec returns the configuration specification for the UI
func GetConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable Table Maintenance",
				Description:  "Whether table maintenance tasks should be automatically created",
				HelpText:     "Toggle this to enable or disable automatic table maintenance",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "scan_interval_minutes",
				JSONName:     "scan_interval_minutes",
				Type:         config.FieldTypeInt,
				DefaultValue: 30,
				MinValue:     5,
				MaxValue:     1440,
				Required:     true,
				DisplayName:  "Scan Interval (minutes)",
				Description:  "How often to scan for tables needing maintenance",
				HelpText:     "Lower values mean faster detection but higher overhead",
				Placeholder:  "30",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "compaction_file_threshold",
				JSONName:     "compaction_file_threshold",
				Type:         config.FieldTypeInt,
				DefaultValue: 100,
				MinValue:     10,
				MaxValue:     10000,
				Required:     true,
				DisplayName:  "Compaction File Threshold",
				Description:  "Number of small files that triggers compaction",
				HelpText:     "Tables with more small files than this will be scheduled for compaction",
				Placeholder:  "100",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "snapshot_retention_days",
				JSONName:     "snapshot_retention_days",
				Type:         config.FieldTypeInt,
				DefaultValue: 7,
				MinValue:     1,
				MaxValue:     365,
				Required:     true,
				DisplayName:  "Snapshot Retention (days)",
				Description:  "How long to keep snapshots before expiration",
				HelpText:     "Snapshots older than this will be candidates for cleanup",
				Placeholder:  "7",
				Unit:         config.UnitCount,
				InputType:    "number",
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
				DisplayName:  "Max Concurrent Jobs",
				Description:  "Maximum number of concurrent maintenance jobs",
				HelpText:     "Limits the number of maintenance operations running at the same time",
				Placeholder:  "2",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
		},
	}
}
