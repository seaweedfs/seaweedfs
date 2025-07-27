package vacuum

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
)

// GetConfigSchema returns the schema for vacuum task configuration
func GetConfigSchema() *tasks.TaskConfigSchema {
	return &tasks.TaskConfigSchema{
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
