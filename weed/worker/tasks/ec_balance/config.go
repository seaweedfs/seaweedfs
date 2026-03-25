package ec_balance

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
)

// Config extends BaseConfig with EC balance specific settings
type Config struct {
	base.BaseConfig
	ImbalanceThreshold float64  `json:"imbalance_threshold"`
	MinServerCount     int      `json:"min_server_count"`
	CollectionFilter   string   `json:"collection_filter"`
	DiskType           string   `json:"disk_type"`
	PreferredTags      []string `json:"preferred_tags"`
	DataCenterFilter   string   `json:"-"` // per-detection-run, not persisted
}

// NewDefaultConfig creates a new default EC balance configuration
func NewDefaultConfig() *Config {
	return &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 60 * 60, // 1 hour
			MaxConcurrent:       1,
		},
		ImbalanceThreshold: 0.2, // 20%
		MinServerCount:     3,
		CollectionFilter:   "",
		DiskType:           "",
		PreferredTags:      nil,
	}
}

// GetConfigSpec returns the configuration schema for EC balance tasks
func GetConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable EC Shard Balance Tasks",
				Description:  "Whether EC shard balance tasks should be automatically created",
				HelpText:     "Toggle this to enable or disable automatic EC shard balancing",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 60 * 60,
				MinValue:     10 * 60,
				MaxValue:     24 * 60 * 60,
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for EC shard imbalances",
				HelpText:     "The system will check for EC shard distribution imbalances at this interval",
				Placeholder:  "1",
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
				MaxValue:     5,
				Required:     true,
				DisplayName:  "Max Concurrent Tasks",
				Description:  "Maximum number of EC shard balance tasks that can run simultaneously",
				HelpText:     "Limits the number of EC shard balancing operations running at the same time",
				Placeholder:  "1 (default)",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "imbalance_threshold",
				JSONName:     "imbalance_threshold",
				Type:         config.FieldTypeFloat,
				DefaultValue: 0.2,
				MinValue:     0.05,
				MaxValue:     0.5,
				Required:     true,
				DisplayName:  "Imbalance Threshold",
				Description:  "Minimum shard count imbalance ratio to trigger balancing",
				HelpText:     "EC shard distribution imbalances above this threshold will trigger rebalancing",
				Placeholder:  "0.20 (20%)",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_server_count",
				JSONName:     "min_server_count",
				Type:         config.FieldTypeInt,
				DefaultValue: 3,
				MinValue:     2,
				MaxValue:     100,
				Required:     true,
				DisplayName:  "Minimum Server Count",
				Description:  "Minimum number of servers required for EC shard balancing",
				HelpText:     "EC shard balancing will only occur if there are at least this many servers",
				Placeholder:  "3 (default)",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "collection_filter",
				JSONName:     "collection_filter",
				Type:         config.FieldTypeString,
				DefaultValue: "",
				Required:     false,
				DisplayName:  "Collection Filter",
				Description:  "Only balance EC shards from specific collections",
				HelpText:     "Leave empty to balance all collections, or specify collection name/wildcard",
				Placeholder:  "my_collection",
				InputType:    "text",
				CSSClasses:   "form-control",
			},
			{
				Name:         "disk_type",
				JSONName:     "disk_type",
				Type:         config.FieldTypeString,
				DefaultValue: "",
				Required:     false,
				DisplayName:  "Disk Type",
				Description:  "Only balance EC shards on this disk type",
				HelpText:     "Leave empty for all disk types, or specify hdd or ssd",
				Placeholder:  "hdd",
				InputType:    "text",
				CSSClasses:   "form-control",
			},
			{
				Name:         "preferred_tags",
				JSONName:     "preferred_tags",
				Type:         config.FieldTypeString,
				DefaultValue: "",
				Required:     false,
				DisplayName:  "Preferred Disk Tags",
				Description:  "Comma-separated disk tags to prioritize for shard placement",
				HelpText:     "EC shards will be placed on disks with these tags first, ordered by preference",
				Placeholder:  "fast,ssd",
				InputType:    "text",
				CSSClasses:   "form-control",
			},
		},
	}
}

// ToTaskPolicy converts configuration to a TaskPolicy protobuf message
func (c *Config) ToTaskPolicy() *worker_pb.TaskPolicy {
	preferredTagsCopy := append([]string(nil), c.PreferredTags...)
	return &worker_pb.TaskPolicy{
		Enabled:               c.Enabled,
		MaxConcurrent:         int32(c.MaxConcurrent),
		RepeatIntervalSeconds: int32(c.ScanIntervalSeconds),
		CheckIntervalSeconds:  int32(c.ScanIntervalSeconds),
		TaskConfig: &worker_pb.TaskPolicy_EcBalanceConfig{
			EcBalanceConfig: &worker_pb.EcBalanceTaskConfig{
				ImbalanceThreshold: c.ImbalanceThreshold,
				MinServerCount:     int32(c.MinServerCount),
				CollectionFilter:   c.CollectionFilter,
				DiskType:           c.DiskType,
				PreferredTags:      preferredTagsCopy,
			},
		},
	}
}

// FromTaskPolicy loads configuration from a TaskPolicy protobuf message
func (c *Config) FromTaskPolicy(policy *worker_pb.TaskPolicy) error {
	if policy == nil {
		return fmt.Errorf("policy is nil")
	}

	c.Enabled = policy.Enabled
	c.MaxConcurrent = int(policy.MaxConcurrent)
	c.ScanIntervalSeconds = int(policy.RepeatIntervalSeconds)

	if ecbConfig := policy.GetEcBalanceConfig(); ecbConfig != nil {
		c.ImbalanceThreshold = ecbConfig.ImbalanceThreshold
		c.MinServerCount = int(ecbConfig.MinServerCount)
		c.CollectionFilter = ecbConfig.CollectionFilter
		c.DiskType = ecbConfig.DiskType
		c.PreferredTags = append([]string(nil), ecbConfig.PreferredTags...)
	}

	return nil
}

// LoadConfigFromPersistence loads configuration from the persistence layer if available
func LoadConfigFromPersistence(configPersistence interface{}) *Config {
	cfg := NewDefaultConfig()

	if persistence, ok := configPersistence.(interface {
		LoadEcBalanceTaskPolicy() (*worker_pb.TaskPolicy, error)
	}); ok {
		if policy, err := persistence.LoadEcBalanceTaskPolicy(); err == nil && policy != nil {
			if err := cfg.FromTaskPolicy(policy); err == nil {
				glog.V(1).Infof("Loaded EC balance configuration from persistence")
				return cfg
			}
		}
	}

	glog.V(1).Infof("Using default EC balance configuration")
	return cfg
}
