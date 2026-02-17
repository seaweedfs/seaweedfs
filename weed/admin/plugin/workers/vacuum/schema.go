package vacuum

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// GetConfigurationSchema returns the configuration schema for the vacuum job type
func GetConfigurationSchema() *plugin_pb.JobTypeConfigSchema {
	return &plugin_pb.JobTypeConfigSchema{
		JobType:     "vacuum",
		Version:     "v1",
		Description: "Automatically vacuum volumes to reclaim space by removing deleted entries",
		AdminFields: []*plugin_pb.ConfigField{
			{
				Name:         "garbageThreshold",
				Label:        "Garbage Threshold (%)",
				Description:  "Vacuum volumes when garbage ratio exceeds this percentage (0-100)",
				FieldType:    plugin_pb.ConfigField_INT,
				Required:     true,
				DefaultValue: "30",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_VALUE,
						Value:        "0",
						ErrorMessage: "Garbage threshold must be at least 0",
					},
					{
						RuleType:     plugin_pb.ValidationRule_MAX_VALUE,
						Value:        "100",
						ErrorMessage: "Garbage threshold must be at most 100",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					MinValue: 0,
					MaxValue: 100,
				},
			},
			{
				Name:         "minVolumeAge",
				Label:        "Minimum Volume Age",
				Description:  "Skip vacuuming volumes created less than this duration ago (format: 24h, 7d, etc.)",
				FieldType:    plugin_pb.ConfigField_STRING,
				Required:     false,
				DefaultValue: "24h",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_PATTERN,
						Value:        "^(\\d+[smhd])+$",
						ErrorMessage: "Must be a valid duration (e.g., 24h, 7d, 30m)",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					Placeholder: "24h",
				},
			},
			{
				Name:         "minInterval",
				Label:        "Minimum Interval Between Vacuums",
				Description:  "Don't vacuum the same volume more frequently than this interval (format: 7d, 168h, etc.)",
				FieldType:    plugin_pb.ConfigField_STRING,
				Required:     false,
				DefaultValue: "7d",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_PATTERN,
						Value:        "^(\\d+[smhd])+$",
						ErrorMessage: "Must be a valid duration (e.g., 7d, 168h, 1w)",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					Placeholder: "7d",
				},
			},
		},
		WorkerFields: []*plugin_pb.ConfigField{
			{
				Name:         "compactLevel",
				Label:        "Compaction Level",
				Description:  "Compaction level for volume defragmentation (0-5, higher = more aggressive)",
				FieldType:    plugin_pb.ConfigField_INT,
				Required:     false,
				DefaultValue: "2",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_VALUE,
						Value:        "0",
						ErrorMessage: "Compaction level must be at least 0",
					},
					{
						RuleType:     plugin_pb.ValidationRule_MAX_VALUE,
						Value:        "5",
						ErrorMessage: "Compaction level must be at most 5",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					MinValue: 0,
					MaxValue: 5,
				},
			},
			{
				Name:         "enableCleanup",
				Label:        "Enable Space Cleanup",
				Description:  "Whether to clean up and release deleted space after compaction",
				FieldType:    plugin_pb.ConfigField_BOOL,
				Required:     false,
				DefaultValue: "true",
			},
		},
		FieldGroups: []*plugin_pb.ConfigGroup{
			{
				Name:        "Vacuum Settings",
				Label:       "Vacuum Settings",
				Description: "Configuration for volume vacuuming and garbage collection",
				FieldNames: []string{
					"garbageThreshold",
					"minVolumeAge",
					"minInterval",
				},
			},
			{
				Name:        "advanced",
				Label:       "Advanced",
				Description: "Advanced vacuum parameters",
				FieldNames: []string{
					"compactLevel",
					"enableCleanup",
				},
			},
		},
	}
}
