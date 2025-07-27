package tasks

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/config"
)

// TaskConfigSchema defines the schema for task configuration
type TaskConfigSchema struct {
	config.Schema        // Embed common schema functionality
	TaskName      string `json:"task_name"`
	DisplayName   string `json:"display_name"`
	Description   string `json:"description"`
	Icon          string `json:"icon"`
}

// GetTaskConfigSchema returns the schema for the specified task type
func GetTaskConfigSchema(taskType string) *TaskConfigSchema {
	switch taskType {
	case "vacuum":
		return GetVacuumTaskConfigSchema()
	case "balance":
		return GetBalanceTaskConfigSchema()
	case "erasure_coding":
		return GetErasureCodingTaskConfigSchema()
	default:
		return nil
	}
}

// GetVacuumTaskConfigSchema returns the schema for vacuum task configuration
func GetVacuumTaskConfigSchema() *TaskConfigSchema {
	return &TaskConfigSchema{
		TaskName:    "vacuum",
		DisplayName: "Volume Vacuum",
		Description: "Reclaims disk space by removing deleted files from volumes",
		Icon:        "fas fa-broom text-primary",
		Schema: config.Schema{
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
					Name:         "garbage_threshold",
					JSONName:     "garbage_threshold",
					Type:         config.FieldTypeFloat,
					DefaultValue: 0.3, // 30%
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
					Name:         "scan_interval_seconds",
					JSONName:     "scan_interval_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 2 * 60 * 60,  // 2 hours
					MinValue:     10 * 60,      // 10 minutes
					MaxValue:     24 * 60 * 60, // 24 hours
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
					Name:         "min_volume_age_seconds",
					JSONName:     "min_volume_age_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 24 * 60 * 60,     // 24 hours
					MinValue:     1 * 60 * 60,      // 1 hour
					MaxValue:     7 * 24 * 60 * 60, // 7 days
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
					DefaultValue: 7 * 24 * 60 * 60,  // 7 days
					MinValue:     1 * 24 * 60 * 60,  // 1 day
					MaxValue:     30 * 24 * 60 * 60, // 30 days
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
		},
	}
}

// GetBalanceTaskConfigSchema returns the schema for balance task configuration
func GetBalanceTaskConfigSchema() *TaskConfigSchema {
	return &TaskConfigSchema{
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

// GetErasureCodingTaskConfigSchema returns the schema for erasure coding task configuration
func GetErasureCodingTaskConfigSchema() *TaskConfigSchema {
	return &TaskConfigSchema{
		TaskName:    "erasure_coding",
		DisplayName: "Erasure Coding",
		Description: "Converts volumes to erasure coded format for improved data durability",
		Icon:        "fas fa-shield-alt text-info",
		Schema: config.Schema{
			Fields: []*config.Field{
				{
					Name:         "enabled",
					JSONName:     "enabled",
					Type:         config.FieldTypeBool,
					DefaultValue: true,
					Required:     false,
					DisplayName:  "Enable Erasure Coding Tasks",
					Description:  "Whether erasure coding tasks should be automatically created",
					InputType:    "checkbox",
					CSSClasses:   "form-check-input",
				},
				{
					Name:         "quiet_for_seconds",
					JSONName:     "quiet_for_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 7 * 24 * 60 * 60,  // 7 days
					MinValue:     1 * 24 * 60 * 60,  // 1 day
					MaxValue:     30 * 24 * 60 * 60, // 30 days
					Required:     true,
					DisplayName:  "Quiet For Duration",
					Description:  "Only apply erasure coding to volumes that have not been modified for this duration",
					Unit:         config.UnitDays,
					InputType:    "interval",
					CSSClasses:   "form-control",
				},
				{
					Name:         "scan_interval_seconds",
					JSONName:     "scan_interval_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 12 * 60 * 60, // 12 hours
					MinValue:     2 * 60 * 60,  // 2 hours
					MaxValue:     24 * 60 * 60, // 24 hours
					Required:     true,
					DisplayName:  "Scan Interval",
					Description:  "How often to scan for volumes needing erasure coding",
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
					Description:  "Maximum number of erasure coding tasks that can run simultaneously",
					Unit:         config.UnitCount,
					InputType:    "number",
					CSSClasses:   "form-control",
				},
				{
					Name:         "fullness_ratio",
					JSONName:     "fullness_ratio",
					Type:         config.FieldTypeFloat,
					DefaultValue: 0.9, // 90%
					MinValue:     0.5,
					MaxValue:     1.0,
					Required:     true,
					DisplayName:  "Fullness Ratio",
					Description:  "Only apply erasure coding to volumes with fullness ratio above this threshold",
					Placeholder:  "0.90 (90%)",
					Unit:         config.UnitNone,
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
					Description:  "Only apply erasure coding to volumes in these collections (comma-separated, leave empty for all)",
					Placeholder:  "collection1,collection2",
					InputType:    "text",
					CSSClasses:   "form-control",
				},
			},
		},
	}
}
