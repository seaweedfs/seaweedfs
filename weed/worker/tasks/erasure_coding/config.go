package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/admin/config"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
)

// Config extends BaseConfig with erasure coding specific settings
type Config struct {
	base.BaseConfig
	QuietForSeconds  int     `json:"quiet_for_seconds"`
	FullnessRatio    float64 `json:"fullness_ratio"`
	CollectionFilter string  `json:"collection_filter"`
}

// NewDefaultConfig creates a new default erasure coding configuration
func NewDefaultConfig() *Config {
	return &Config{
		BaseConfig: base.BaseConfig{
			Enabled:             true,
			ScanIntervalSeconds: 60 * 60, // 1 hour
			MaxConcurrent:       1,
		},
		QuietForSeconds:  300, // 5 minutes
		FullnessRatio:    0.8, // 80%
		CollectionFilter: "",
	}
}

// GetConfigSpec returns the configuration schema for erasure coding tasks
func GetConfigSpec() base.ConfigSpec {
	return base.ConfigSpec{
		Fields: []*config.Field{
			{
				Name:         "enabled",
				JSONName:     "enabled",
				Type:         config.FieldTypeBool,
				DefaultValue: true,
				Required:     false,
				DisplayName:  "Enable Erasure Coding Tasks",
				Description:  "Whether erasure coding tasks should be automatically created",
				HelpText:     "Toggle this to enable or disable automatic erasure coding task generation",
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
				Description:  "How often to scan for volumes needing erasure coding",
				HelpText:     "The system will check for volumes that need erasure coding at this interval",
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
				Description:  "Maximum number of erasure coding tasks that can run simultaneously",
				HelpText:     "Limits the number of erasure coding operations running at the same time",
				Placeholder:  "1 (default)",
				Unit:         config.UnitCount,
				InputType:    "number",
				CSSClasses:   "form-control",
			},
			{
				Name:         "quiet_for_seconds",
				JSONName:     "quiet_for_seconds",
				Type:         config.FieldTypeInterval,
				DefaultValue: 300,
				MinValue:     60,
				MaxValue:     3600,
				Required:     true,
				DisplayName:  "Quiet Period",
				Description:  "Minimum time volume must be quiet before erasure coding",
				HelpText:     "Volume must not be modified for this duration before erasure coding",
				Placeholder:  "5",
				Unit:         config.UnitMinutes,
				InputType:    "interval",
				CSSClasses:   "form-control",
			},
			{
				Name:         "fullness_ratio",
				JSONName:     "fullness_ratio",
				Type:         config.FieldTypeFloat,
				DefaultValue: 0.8,
				MinValue:     0.1,
				MaxValue:     1.0,
				Required:     true,
				DisplayName:  "Fullness Ratio",
				Description:  "Minimum fullness ratio to trigger erasure coding",
				HelpText:     "Only volumes with this fullness ratio or higher will be erasure coded",
				Placeholder:  "0.80 (80%)",
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
				Description:  "Only process volumes from specific collections",
				HelpText:     "Leave empty to process all collections, or specify collection name",
				Placeholder:  "my_collection",
				InputType:    "text",
				CSSClasses:   "form-control",
			},
		},
	}
}
