package erasure_coding

import (
	"encoding/json"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// ConfigurationSchema defines the schema for EC plugin configuration
type ConfigurationSchema struct {
	AdminConfig  AdminConfigSchema  `json:"admin_config"`
	WorkerConfig WorkerConfigSchema `json:"worker_config"`
}

// AdminConfigSchema defines admin-side configuration
type AdminConfigSchema struct {
	DetectionInterval    ConfigField `json:"detection_interval"`
	MaxConcurrentJobs    ConfigField `json:"max_concurrent_jobs"`
	JobTimeout           ConfigField `json:"job_timeout"`
	HealthCheckInterval  ConfigField `json:"health_check_interval"`
	RetryPolicy          ConfigField `json:"retry_policy"`
}

// WorkerConfigSchema defines worker-side configuration
type WorkerConfigSchema struct {
	StripeSize         ConfigField `json:"stripe_size"`
	EncodeCopies       ConfigField `json:"encode_copies"`
	RackAwareness      ConfigField `json:"rack_awareness"`
	DataCenterAwareness ConfigField `json:"datacenter_awareness"`
	MinVolumeSize      ConfigField `json:"min_volume_size"`
	MaxVolumeSize      ConfigField `json:"max_volume_size"`
	PreferredDataNodes ConfigField `json:"preferred_data_nodes"`
}

// ConfigField describes a configuration field
type ConfigField struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Type        string        `json:"type"`
	Required    bool          `json:"required"`
	Default     interface{}   `json:"default,omitempty"`
	Min         interface{}   `json:"min,omitempty"`
	Max         interface{}   `json:"max,omitempty"`
	Options     []interface{} `json:"options,omitempty"`
	Unit        string        `json:"unit,omitempty"`
}

// GetConfigurationSchema returns the schema for EC plugin configuration
func GetConfigurationSchema() *plugin_pb.PluginConfig {
	schema := ConfigurationSchema{
		AdminConfig: AdminConfigSchema{
			DetectionInterval: ConfigField{
				Name:        "detection_interval",
				Description: "Time between EC detection scans",
				Type:        "duration",
				Required:    true,
				Default:     "1h",
				Min:         "5m",
				Max:         "24h",
				Unit:        "seconds",
			},
			MaxConcurrentJobs: ConfigField{
				Name:        "max_concurrent_jobs",
				Description: "Maximum concurrent EC encoding jobs",
				Type:        "integer",
				Required:    true,
				Default:     5,
				Min:         1,
				Max:         20,
			},
			JobTimeout: ConfigField{
				Name:        "job_timeout",
				Description: "Timeout for individual EC jobs",
				Type:        "duration",
				Required:    true,
				Default:     "12h",
				Min:         "1h",
				Max:         "48h",
				Unit:        "seconds",
			},
			HealthCheckInterval: ConfigField{
				Name:        "health_check_interval",
				Description: "Health check interval",
				Type:        "duration",
				Required:    true,
				Default:     "30s",
				Min:         "5s",
				Max:         "5m",
				Unit:        "seconds",
			},
			RetryPolicy: ConfigField{
				Name:        "retry_policy",
				Description: "Retry policy for failed jobs",
				Type:        "string",
				Required:    true,
				Default:     "exponential",
				Options:     []interface{}{"linear", "exponential", "none"},
			},
		},
		WorkerConfig: WorkerConfigSchema{
			StripeSize: ConfigField{
				Name:        "stripe_size",
				Description: "Size of each stripe in MB",
				Type:        "integer",
				Required:    true,
				Default:     10,
				Min:         1,
				Max:         100,
				Unit:        "MB",
			},
			EncodeCopies: ConfigField{
				Name:        "encode_copies",
				Description: "Number of copies to keep after encoding",
				Type:        "integer",
				Required:    true,
				Default:     1,
				Min:         1,
				Max:         3,
			},
			RackAwareness: ConfigField{
				Name:        "rack_awareness",
				Description: "Enable rack-aware stripe distribution",
				Type:        "boolean",
				Required:    true,
				Default:     true,
			},
			DataCenterAwareness: ConfigField{
				Name:        "datacenter_awareness",
				Description: "Enable data center aware stripe distribution",
				Type:        "boolean",
				Required:    true,
				Default:     false,
			},
			MinVolumeSize: ConfigField{
				Name:        "min_volume_size",
				Description: "Minimum volume size to consider for EC",
				Type:        "integer",
				Required:    true,
				Default:     1000,
				Min:         100,
				Unit:        "MB",
			},
			MaxVolumeSize: ConfigField{
				Name:        "max_volume_size",
				Description: "Maximum volume size to consider for EC",
				Type:        "integer",
				Required:    true,
				Default:     10000,
				Max:         100000,
				Unit:        "MB",
			},
			PreferredDataNodes: ConfigField{
				Name:        "preferred_data_nodes",
				Description: "Comma-separated list of preferred data nodes",
				Type:        "string",
				Required:    false,
			},
		},
	}

	data, _ := json.MarshalIndent(schema, "", "  ")

	return &plugin_pb.PluginConfig{
		PluginId: "erasure-coding-plugin",
		Properties: map[string]string{
			"schema":                  string(data),
			"detection_interval":      "1h",
			"max_concurrent_jobs":     "5",
			"job_timeout":             "12h",
			"health_check_interval":   "30s",
			"retry_policy":            "exponential",
			"stripe_size":             "10",
			"encode_copies":           "1",
			"rack_awareness":          "true",
			"datacenter_awareness":    "false",
			"min_volume_size":         "1000",
			"max_volume_size":         "10000",
			"preferred_data_nodes":    "",
		},
	}
}

// DefaultAdminConfig returns default admin configuration
func DefaultAdminConfig() map[string]string {
	return map[string]string{
		"detection_interval":     "1h",
		"max_concurrent_jobs":    "5",
		"job_timeout":            "12h",
		"health_check_interval":  "30s",
		"retry_policy":           "exponential",
	}
}

// DefaultWorkerConfig returns default worker configuration
func DefaultWorkerConfig() map[string]string {
	return map[string]string{
		"stripe_size":            "10",
		"encode_copies":          "1",
		"rack_awareness":         "true",
		"datacenter_awareness":   "false",
		"min_volume_size":        "1000",
		"max_volume_size":        "10000",
		"preferred_data_nodes":   "",
	}
}
