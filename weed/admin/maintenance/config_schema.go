package maintenance

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/config"
)

// Type aliases for backward compatibility
type ConfigFieldType = config.FieldType
type ConfigFieldUnit = config.FieldUnit
type ConfigField = config.Field

// Constant aliases for backward compatibility
const (
	FieldTypeBool     = config.FieldTypeBool
	FieldTypeInt      = config.FieldTypeInt
	FieldTypeDuration = config.FieldTypeDuration
	FieldTypeInterval = config.FieldTypeInterval
	FieldTypeString   = config.FieldTypeString
	FieldTypeFloat    = config.FieldTypeFloat
)

const (
	UnitSeconds = config.UnitSeconds
	UnitMinutes = config.UnitMinutes
	UnitHours   = config.UnitHours
	UnitDays    = config.UnitDays
	UnitCount   = config.UnitCount
	UnitNone    = config.UnitNone
)

// Function aliases for backward compatibility
var (
	SecondsToIntervalValueUnit = config.SecondsToIntervalValueUnit
	IntervalValueUnitToSeconds = config.IntervalValueUnitToSeconds
)

// MaintenanceConfigSchema defines the schema for maintenance configuration
type MaintenanceConfigSchema struct {
	config.Schema // Embed common schema functionality
}

// GetMaintenanceConfigSchema returns the schema for maintenance configuration
func GetMaintenanceConfigSchema() *MaintenanceConfigSchema {
	return &MaintenanceConfigSchema{
		Schema: config.Schema{
			Fields: []*config.Field{
				{
					Name:         "enabled",
					JSONName:     "enabled",
					Type:         config.FieldTypeBool,
					DefaultValue: true,
					Required:     false,
					DisplayName:  "Enable Maintenance System",
					Description:  "When enabled, the system will automatically scan for and execute maintenance tasks",
					HelpText:     "Toggle this to enable or disable the entire maintenance system",
					InputType:    "checkbox",
					CSSClasses:   "form-check-input",
				},
				{
					Name:         "scan_interval_seconds",
					JSONName:     "scan_interval_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 30 * 60,      // 30 minutes in seconds
					MinValue:     1 * 60,       // 1 minute
					MaxValue:     24 * 60 * 60, // 24 hours
					Required:     true,
					DisplayName:  "Scan Interval",
					Description:  "How often to scan for maintenance tasks",
					HelpText:     "The system will check for new maintenance tasks at this interval",
					Placeholder:  "30",
					Unit:         config.UnitMinutes,
					InputType:    "interval",
					CSSClasses:   "form-control",
				},
				{
					Name:         "worker_timeout_seconds",
					JSONName:     "worker_timeout_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 5 * 60,  // 5 minutes
					MinValue:     1 * 60,  // 1 minute
					MaxValue:     60 * 60, // 1 hour
					Required:     true,
					DisplayName:  "Worker Timeout",
					Description:  "How long to wait for worker heartbeat before considering it inactive",
					HelpText:     "Workers that don't send heartbeats within this time are considered offline",
					Placeholder:  "5",
					Unit:         config.UnitMinutes,
					InputType:    "interval",
					CSSClasses:   "form-control",
				},
				{
					Name:         "task_timeout_seconds",
					JSONName:     "task_timeout_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 2 * 60 * 60,  // 2 hours
					MinValue:     10 * 60,      // 10 minutes
					MaxValue:     24 * 60 * 60, // 24 hours
					Required:     true,
					DisplayName:  "Task Timeout",
					Description:  "Maximum time allowed for a task to complete",
					HelpText:     "Tasks that exceed this duration will be marked as failed",
					Placeholder:  "2",
					Unit:         config.UnitHours,
					InputType:    "interval",
					CSSClasses:   "form-control",
				},
				{
					Name:         "retry_delay_seconds",
					JSONName:     "retry_delay_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 15 * 60,      // 15 minutes
					MinValue:     1 * 60,       // 1 minute
					MaxValue:     24 * 60 * 60, // 24 hours
					Required:     true,
					DisplayName:  "Retry Delay",
					Description:  "How long to wait before retrying a failed task",
					HelpText:     "Failed tasks will be retried after this delay",
					Placeholder:  "15",
					Unit:         config.UnitMinutes,
					InputType:    "interval",
					CSSClasses:   "form-control",
				},
				{
					Name:         "max_retries",
					JSONName:     "max_retries",
					Type:         config.FieldTypeInt,
					DefaultValue: 3,
					MinValue:     0,
					MaxValue:     10,
					Required:     true,
					DisplayName:  "Max Retries",
					Description:  "Maximum number of times to retry a failed task",
					HelpText:     "Tasks that fail more than this many times will be marked as permanently failed",
					Placeholder:  "3",
					Unit:         config.UnitCount,
					InputType:    "number",
					CSSClasses:   "form-control",
				},
				{
					Name:         "cleanup_interval_seconds",
					JSONName:     "cleanup_interval_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 24 * 60 * 60,     // 24 hours
					MinValue:     1 * 60 * 60,      // 1 hour
					MaxValue:     7 * 24 * 60 * 60, // 7 days
					Required:     true,
					DisplayName:  "Cleanup Interval",
					Description:  "How often to run maintenance cleanup operations",
					HelpText:     "Removes old task records and temporary files at this interval",
					Placeholder:  "24",
					Unit:         config.UnitHours,
					InputType:    "interval",
					CSSClasses:   "form-control",
				},
				{
					Name:         "task_retention_seconds",
					JSONName:     "task_retention_seconds",
					Type:         config.FieldTypeInterval,
					DefaultValue: 7 * 24 * 60 * 60,  // 7 days
					MinValue:     1 * 24 * 60 * 60,  // 1 day
					MaxValue:     30 * 24 * 60 * 60, // 30 days
					Required:     true,
					DisplayName:  "Task Retention",
					Description:  "How long to keep completed task records",
					HelpText:     "Task history older than this duration will be automatically deleted",
					Placeholder:  "7",
					Unit:         config.UnitDays,
					InputType:    "interval",
					CSSClasses:   "form-control",
				},
				{
					Name:         "global_max_concurrent",
					JSONName:     "global_max_concurrent",
					Type:         config.FieldTypeInt,
					DefaultValue: 10,
					MinValue:     1,
					MaxValue:     100,
					Required:     true,
					DisplayName:  "Global Max Concurrent Tasks",
					Description:  "Maximum number of maintenance tasks that can run simultaneously across all workers",
					HelpText:     "Limits the total number of maintenance operations to control system load",
					Placeholder:  "10",
					Unit:         config.UnitCount,
					InputType:    "number",
					CSSClasses:   "form-control",
				},
			},
		},
	}
}
