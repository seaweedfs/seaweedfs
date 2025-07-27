package balance

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
)

// GetConfigSchema returns the schema for balance task configuration
func GetConfigSchema() *tasks.TaskConfigSchema {
	return &tasks.TaskConfigSchema{
		TaskName:    "balance",
		DisplayName: "Volume Balance",
		Description: "Redistributes volumes across volume servers to optimize storage utilization",
		Icon:        "fas fa-balance-scale text-secondary",
		Schema: config.Schema{
			Fields: []*config.Field{
				{
					Name:         "enabled",
					JSONName:     "enabled",
					Type:         config.FieldTypeBool,
					DefaultValue: true,
					Required:     false,
					DisplayName:  "Enable Balance Tasks",
					Description:  "Whether balance tasks should be automatically created",
					InputType:    "checkbox",
					CSSClasses:   "form-check-input",
				},
				{
					Name:         "imbalance_threshold",
					JSONName:     "imbalance_threshold",
					Type:         config.FieldTypeFloat,
					DefaultValue: 0.1, // 10%
					MinValue:     0.01,
					MaxValue:     0.5,
					Required:     true,
					DisplayName:  "Imbalance Threshold",
					Description:  "Trigger balance when storage imbalance exceeds this ratio",
					Placeholder:  "0.10 (10%)",
					Unit:         config.UnitNone,
					InputType:    "number",
					CSSClasses:   "form-control",
				},
				{
					Name:         "scan_interval_seconds",
					JSONName:     "scan_interval_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 6 * 60 * 60,  // 6 hours
					MinValue:     1 * 60 * 60,  // 1 hour
					MaxValue:     24 * 60 * 60, // 24 hours
					Required:     true,
					DisplayName:  "Scan Interval",
					Description:  "How often to scan for imbalanced volumes",
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
					MaxValue:     5,
					Required:     true,
					DisplayName:  "Max Concurrent Tasks",
					Description:  "Maximum number of balance tasks that can run simultaneously",
					Unit:         config.UnitCount,
					InputType:    "number",
					CSSClasses:   "form-control",
				},
				{
					Name:         "min_server_count",
					JSONName:     "min_server_count",
					Type:         config.FieldTypeInt,
					DefaultValue: 3,
					MinValue:     2,
					MaxValue:     20,
					Required:     true,
					DisplayName:  "Minimum Server Count",
					Description:  "Only balance when at least this many servers are available",
					Unit:         config.UnitCount,
					InputType:    "number",
					CSSClasses:   "form-control",
				},
			},
		},
	}
}
