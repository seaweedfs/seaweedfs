package balance

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
)

// Config extends BaseConfig with balance-specific settings
type Config struct {
	base.BaseConfig
	ImbalanceThreshold float64 `json:"imbalance_threshold"`
	MinServerCount     int     `json:"min_server_count"`
}

// NewDefaultConfig creates a new default balance configuration
func NewDefaultConfig() *Config {
	return &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 30 * 60, // 30 minutes
			MaxConcurrent:       1,
		},
		ImbalanceThreshold: 0.2, // 20%
		MinServerCount:     2,
	}
}

// GetConfigSpec returns the configuration schema for balance tasks
func GetConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable Balance Tasks",
				Description:  "Whether balance tasks should be automatically created",
				HelpText:     "Toggle this to enable or disable automatic balance task generation",
				InputType:    "checkbox",
				CSSClasses:   "form-check-input",
			},
			{
				Name:         "scan_interval_seconds",
				JSONName:     "scan_interval_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 30 * 60,
				MinValue:     5 * 60,
				MaxValue:     2 * 60 * 60,
				Required:     true,
				DisplayName:  "Scan Interval",
				Description:  "How often to scan for volume distribution imbalances",
				HelpText:     "The system will check for volume distribution imbalances at this interval",
				Placeholder:  "30",
				Unit:         config.UnitMinutes,
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
				Description:  "Maximum number of balance tasks that can run simultaneously",
				HelpText:     "Limits the number of balance operations running at the same time",
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
				Description:  "Minimum imbalance ratio to trigger balancing",
				HelpText:     "Volume distribution imbalances above this threshold will trigger balancing",
				Placeholder:  "0.20 (20%)",
				Unit:         config.UnitNone,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "min_server_count",
				JSONName:     "min_server_count",
				Type:         config.FieldTypeInt,
				DefaultValue: 2,
				MinValue:     2,
				MaxValue:     10,
				Required:     true,
				DisplayName:  "Minimum Server Count",
				Description:  "Minimum number of servers required for balancing",
				HelpText:     "Balancing will only occur if there are at least this many servers",
				Placeholder:  "2 (default)",
				Unit:         config.UnitCount,
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
		TaskConfig: &worker_pb.TaskPolicy_BalanceConfig{
			BalanceConfig: &worker_pb.BalanceTaskConfig{
				ImbalanceThreshold: float64(c.ImbalanceThreshold),
				MinServerCount:     int32(c.MinServerCount),
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

	// Set balance-specific fields from the task config
	if balanceConfig := policy.GetBalanceConfig(); balanceConfig != nil {
		c.ImbalanceThreshold = float64(balanceConfig.ImbalanceThreshold)
		c.MinServerCount = int(balanceConfig.MinServerCount)
	}

	return nil
}

// LoadConfigFromPersistence loads configuration from the persistence layer if available
func LoadConfigFromPersistence(configPersistence interface{}) *Config {
	config := NewDefaultConfig()

	// Try to load from persistence if available
	if persistence, ok := configPersistence.(interface {
		LoadBalanceTaskPolicy() (*worker_pb.TaskPolicy, error)
	}); ok {
		if policy, err := persistence.LoadBalanceTaskPolicy(); err == nil && policy != nil {
			if err := config.FromTaskPolicy(policy); err == nil {
				glog.V(1).Infof("Loaded balance configuration from persistence")
				return config
			}
		}
	}

	glog.V(1).Infof("Using default balance configuration")
	return config
}
