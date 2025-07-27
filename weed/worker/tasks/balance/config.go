package balance

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/config"
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
