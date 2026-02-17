package vacuum

import (
	"encoding/json"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// ConfigurationSchema defines the schema for vacuum plugin configuration
type ConfigurationSchema struct {
	AdminConfig  AdminConfigSchema  `json:"admin_config"`
	WorkerConfig WorkerConfigSchema `json:"worker_config"`
}

// AdminConfigSchema defines admin-side configuration
type AdminConfigSchema struct {
	VacuumInterval         ConfigField `json:"vacuum_interval"`
	MaxConcurrentJobs      ConfigField `json:"max_concurrent_jobs"`
	JobTimeout             ConfigField `json:"job_timeout"`
	HealthCheckInterval    ConfigField `json:"health_check_interval"`
	DeadSpaceThreshold     ConfigField `json:"dead_space_threshold"`
}

// WorkerConfigSchema defines worker-side configuration
type WorkerConfigSchema struct {
	MinVolumeSize      ConfigField `json:"min_volume_size"`
	MaxVolumeSize      ConfigField `json:"max_volume_size"`
	TargetUtilization  ConfigField `json:"target_utilization"`
	BatchSize          ConfigField `json:"batch_size"`
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

// GetConfigurationSchema returns the schema for vacuum plugin configuration
func GetConfigurationSchema() *plugin_pb.PluginConfig {
	schema := ConfigurationSchema{
		AdminConfig: AdminConfigSchema{
			VacuumInterval: ConfigField{
				Name:        "vacuum_interval",
				Description: "Time between vacuum operations",
				Type:        "duration",
				Required:    true,
				Default:     "4h",
				Min:         "1h",
				Max:         "24h",
				Unit:        "seconds",
			},
			MaxConcurrentJobs: ConfigField{
				Name:        "max_concurrent_jobs",
				Description: "Maximum concurrent vacuum jobs",
				Type:        "integer",
				Required:    true,
				Default:     3,
				Min:         1,
				Max:         10,
			},
			JobTimeout: ConfigField{
				Name:        "job_timeout",
				Description: "Timeout for individual vacuum jobs",
				Type:        "duration",
				Required:    true,
				Default:     "8h",
				Min:         "1h",
				Max:         "24h",
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
			DeadSpaceThreshold: ConfigField{
				Name:        "dead_space_threshold",
				Description: "Dead space percentage threshold for vacuum",
				Type:        "integer",
				Required:    true,
				Default:     30,
				Min:         5,
				Max:         95,
				Unit:        "percent",
			},
		},
		WorkerConfig: WorkerConfigSchema{
			MinVolumeSize: ConfigField{
				Name:        "min_volume_size",
				Description: "Minimum volume size to consider for vacuum",
				Type:        "integer",
				Required:    true,
				Default:     500,
				Min:         100,
				Unit:        "MB",
			},
			MaxVolumeSize: ConfigField{
				Name:        "max_volume_size",
				Description: "Maximum volume size to consider for vacuum",
				Type:        "integer",
				Required:    true,
				Default:     5000,
				Max:         50000,
				Unit:        "MB",
			},
			TargetUtilization: ConfigField{
				Name:        "target_utilization",
				Description: "Target volume utilization after vacuum",
				Type:        "integer",
				Required:    true,
				Default:     80,
				Min:         50,
				Max:         95,
				Unit:        "percent",
			},
			BatchSize: ConfigField{
				Name:        "batch_size",
				Description: "Number of volumes to process in a batch",
				Type:        "integer",
				Required:    true,
				Default:     10,
				Min:         1,
				Max:         100,
			},
		},
	}

	data, _ := json.MarshalIndent(schema, "", "  ")

	return &plugin_pb.PluginConfig{
		PluginId: "vacuum-plugin",
		Properties: map[string]string{
			"schema":                 string(data),
			"vacuum_interval":        "4h",
			"max_concurrent_jobs":    "3",
			"job_timeout":            "8h",
			"health_check_interval":  "30s",
			"dead_space_threshold":   "30",
			"min_volume_size":        "500",
			"max_volume_size":        "5000",
			"target_utilization":     "80",
			"batch_size":             "10",
		},
	}
}

// DefaultAdminConfig returns default admin configuration
func DefaultAdminConfig() map[string]string {
	return map[string]string{
		"vacuum_interval":       "4h",
		"max_concurrent_jobs":   "3",
		"job_timeout":           "8h",
		"health_check_interval": "30s",
		"dead_space_threshold":  "30",
	}
}

// DefaultWorkerConfig returns default worker configuration
func DefaultWorkerConfig() map[string]string {
	return map[string]string{
		"min_volume_size":    "500",
		"max_volume_size":    "5000",
		"target_utilization": "80",
		"batch_size":         "10",
	}
}
