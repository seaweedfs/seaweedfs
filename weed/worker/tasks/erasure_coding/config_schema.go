package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
)

// GetConfigSchema returns the schema for erasure coding task configuration
func GetConfigSchema() *tasks.TaskConfigSchema {
	return &tasks.TaskConfigSchema{
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
