package balance

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// GetConfigurationSchema returns the configuration schema for the balance job type
func GetConfigurationSchema() *plugin_pb.JobTypeConfigSchema {
	return &plugin_pb.JobTypeConfigSchema{
		JobType:     "balance",
		Version:     "v1",
		Description: "Automatically balance volume distribution across servers to optimize storage and performance",
		AdminFields: []*plugin_pb.ConfigField{
			{
				Name:         "imbalanceThreshold",
				Label:        "Imbalance Threshold (%)",
				Description:  "Trigger rebalancing when imbalance exceeds this percentage (1-100)",
				FieldType:    plugin_pb.ConfigField_INT,
				Required:     true,
				DefaultValue: "20",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_VALUE,
						Value:        "1",
						ErrorMessage: "Imbalance threshold must be at least 1",
					},
					{
						RuleType:     plugin_pb.ValidationRule_MAX_VALUE,
						Value:        "100",
						ErrorMessage: "Imbalance threshold must be at most 100",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					MinValue: 1,
					MaxValue: 100,
				},
			},
			{
				Name:         "minServers",
				Label:        "Minimum Servers",
				Description:  "Only rebalance when cluster has at least this many servers (2-10)",
				FieldType:    plugin_pb.ConfigField_INT,
				Required:     false,
				DefaultValue: "2",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_VALUE,
						Value:        "2",
						ErrorMessage: "Minimum servers must be at least 2",
					},
					{
						RuleType:     plugin_pb.ValidationRule_MAX_VALUE,
						Value:        "10",
						ErrorMessage: "Minimum servers must be at most 10",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					MinValue: 2,
					MaxValue: 10,
				},
			},
			{
				Name:         "preferRackDiversity",
				Label:        "Prefer Rack Diversity",
				Description:  "Consider rack locations when balancing to improve fault tolerance",
				FieldType:    plugin_pb.ConfigField_BOOL,
				Required:     false,
				DefaultValue: "false",
			},
		},
		WorkerFields: []*plugin_pb.ConfigField{
			{
				Name:         "parallelMigrations",
				Label:        "Parallel Migrations",
				Description:  "Number of concurrent volume migrations to perform (1-10)",
				FieldType:    plugin_pb.ConfigField_INT,
				Required:     false,
				DefaultValue: "2",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_VALUE,
						Value:        "1",
						ErrorMessage: "Parallel migrations must be at least 1",
					},
					{
						RuleType:     plugin_pb.ValidationRule_MAX_VALUE,
						Value:        "10",
						ErrorMessage: "Parallel migrations must be at most 10",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					MinValue: 1,
					MaxValue: 10,
				},
			},
			{
				Name:         "maxBytesPerSecond",
				Label:        "Max Bytes Per Second",
				Description:  "Limit migration bandwidth (0 = unlimited, in bytes/sec)",
				FieldType:    plugin_pb.ConfigField_INT,
				Required:     false,
				DefaultValue: "0",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_VALUE,
						Value:        "0",
						ErrorMessage: "Max bytes per second must be non-negative",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					MinValue: 0,
				},
			},
		},
		FieldGroups: []*plugin_pb.ConfigGroup{
			{
				Name:        "Balance Settings",
				Label:       "Balance Settings",
				Description: "Configuration for volume rebalancing",
				FieldNames: []string{
					"imbalanceThreshold",
					"minServers",
					"preferRackDiversity",
				},
			},
			{
				Name:        "advanced",
				Label:       "Advanced",
				Description: "Advanced migration parameters",
				FieldNames: []string{
					"parallelMigrations",
					"maxBytesPerSecond",
				},
			},
		},
	}
}
