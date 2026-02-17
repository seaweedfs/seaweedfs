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
	VacuumInterval      ConfigField `json:"vacuum_interval"`
	MaxConcurrentJobs   ConfigField `json:"max_concurrent_jobs"`
	JobTimeout          ConfigField `json:"job_timeout"`
	HealthCheckInterval ConfigField `json:"health_check_interval"`
	DeadSpaceThreshold  ConfigField `json:"dead_space_threshold"`
}

// WorkerConfigSchema defines worker-side configuration
type WorkerConfigSchema struct {
	MinVolumeSize     ConfigField `json:"min_volume_size"`
	MaxVolumeSize     ConfigField `json:"max_volume_size"`
	TargetUtilization ConfigField `json:"target_utilization"`
	BatchSize         ConfigField `json:"batch_size"`
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
				Description: "Time between vacuum detection scans",
				Type:        "duration",
				Required:    true,
				Default:     "6h",
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
				Max:         "48h",
				Unit:        "seconds",
			},
			HealthCheckInterval: ConfigField{
				Name:        "health_check_interval",
				Description: "Health check interval",
				Type:        "duration",
				Required:    true,
				Default:     "1m",
				Min:         "10s",
				Max:         "10m",
				Unit:        "seconds",
			},
			DeadSpaceThreshold: ConfigField{
				Name:        "dead_space_threshold",
				Description: "Percentage of dead space to trigger vacuum",
				Type:        "integer",
				Required:    true,
				Default:     30,
				Min:         5,
				Max:         90,
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
				Min:         50,
				Unit:        "MB",
			},
			MaxVolumeSize: ConfigField{
				Name:        "max_volume_size",
				Description: "Maximum volume size to consider for vacuum",
				Type:        "integer",
				Required:    true,
				Default:     20000,
				Max:         100000,
				Unit:        "MB",
			},
			TargetUtilization: ConfigField{
				Name:        "target_utilization",
				Description: "Target storage utilization after vacuum",
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
