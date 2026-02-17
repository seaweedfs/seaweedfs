package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// GetConfigurationSchema returns the configuration schema for the erasure coding job type
func GetConfigurationSchema() *plugin_pb.JobTypeConfigSchema {
	return &plugin_pb.JobTypeConfigSchema{
		JobType:     "erasure_coding",
		Version:     "v1",
		Description: "Automatically encode volumes using erasure coding for improved storage efficiency",
		AdminFields: []*plugin_pb.ConfigField{
			{
				Name:         "destination_data_nodes",
				Label:        "Destination Data Nodes",
				Description:  "List of data nodes where EC shards should be distributed",
				FieldType:    plugin_pb.ConfigField_STRING,
				Required:     true,
				DefaultValue: "",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_LENGTH,
						Value:        "1",
						ErrorMessage: "At least one destination node must be specified",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					Placeholder: "node1,node2,node3",
				},
			},
			{
				Name:         "threshold",
				Label:        "Failure Threshold",
				Description:  "Number of data shards that can be lost before data is unrecoverable (1-99)",
				FieldType:    plugin_pb.ConfigField_INT,
				Required:     true,
				DefaultValue: "2",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_VALUE,
						Value:        "1",
						ErrorMessage: "Threshold must be at least 1",
					},
					{
						RuleType:     plugin_pb.ValidationRule_MAX_VALUE,
						Value:        "99",
						ErrorMessage: "Threshold must be at most 99",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					MinValue: 1,
					MaxValue: 99,
				},
			},
			{
				Name:         "delete_source",
				Label:        "Delete Source After Encoding",
				Description:  "Whether to delete the original volume after successful EC encoding",
				FieldType:    plugin_pb.ConfigField_BOOL,
				Required:     false,
				DefaultValue: "true",
			},
		},
		WorkerFields: []*plugin_pb.ConfigField{
			{
				Name:         "ec_m",
				Label:        "Data Shards (M)",
				Description:  "Number of data shards for erasure coding",
				FieldType:    plugin_pb.ConfigField_INT,
				Required:     true,
				DefaultValue: "10",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_VALUE,
						Value:        "1",
						ErrorMessage: "Data shards must be at least 1",
					},
					{
						RuleType:     plugin_pb.ValidationRule_MAX_VALUE,
						Value:        "32",
						ErrorMessage: "Data shards must be at most 32",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					MinValue: 1,
					MaxValue: 32,
				},
			},
			{
				Name:         "ec_n",
				Label:        "Parity Shards (N)",
				Description:  "Number of parity shards for erasure coding",
				FieldType:    plugin_pb.ConfigField_INT,
				Required:     true,
				DefaultValue: "4",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_VALUE,
						Value:        "1",
						ErrorMessage: "Parity shards must be at least 1",
					},
					{
						RuleType:     plugin_pb.ValidationRule_MAX_VALUE,
						Value:        "32",
						ErrorMessage: "Parity shards must be at most 32",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					MinValue: 1,
					MaxValue: 32,
				},
			},
			{
				Name:         "stripe_size",
				Label:        "Stripe Size (bytes)",
				Description:  "Size of data chunks used in erasure coding (must be power of 2)",
				FieldType:    plugin_pb.ConfigField_INT,
				Required:     true,
				DefaultValue: "65536",
				ValidationRules: []*plugin_pb.ValidationRule{
					{
						RuleType:     plugin_pb.ValidationRule_MIN_VALUE,
						Value:        "4096",
						ErrorMessage: "Stripe size must be at least 4096 bytes",
					},
					{
						RuleType:     plugin_pb.ValidationRule_MAX_VALUE,
						Value:        "1048576",
						ErrorMessage: "Stripe size must be at most 1MB",
					},
				},
				OptionsMsg: &plugin_pb.ConfigField_Options{
					MinValue: 4096,
					MaxValue: 1048576,
				},
			},
		},
		FieldGroups: []*plugin_pb.ConfigGroup{
			{
				Name:        "general",
				Label:       "General",
				Description: "Basic configuration for erasure coding",
				FieldNames: []string{
					"destination_data_nodes",
					"threshold",
					"delete_source",
				},
			},
			{
				Name:        "advanced",
				Label:       "Advanced",
				Description: "Advanced erasure coding parameters",
				FieldNames: []string{
					"ec_m",
					"ec_n",
					"stripe_size",
				},
			},
		},
	}
}
