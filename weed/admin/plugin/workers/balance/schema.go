package balance

import (
	"encoding/json"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// ConfigurationSchema defines the schema for balance plugin configuration
type ConfigurationSchema struct {
	AdminConfig  AdminConfigSchema  `json:"admin_config"`
	WorkerConfig WorkerConfigSchema `json:"worker_config"`
}

// AdminConfigSchema defines admin-side configuration
type AdminConfigSchema struct {
	RebalanceInterval          ConfigField `json:"rebalance_interval"`
	MaxConcurrentJobs          ConfigField `json:"max_concurrent_jobs"`
	JobTimeout                 ConfigField `json:"job_timeout"`
	HealthCheckInterval        ConfigField `json:"health_check_interval"`
	DiskUsageThreshold         ConfigField `json:"disk_usage_threshold"`
	AcceptableImbalancePercent ConfigField `json:"acceptable_imbalance_percent"`
}

// WorkerConfigSchema defines worker-side configuration
type WorkerConfigSchema struct {
	MinVolumeSize              ConfigField `json:"min_volume_size"`
	MaxVolumeSize              ConfigField `json:"max_volume_size"`
	DataNodeCount              ConfigField `json:"data_node_count"`
	ReplicationFactor          ConfigField `json:"replication_factor"`
	PreferBalancedDistribution ConfigField `json:"prefer_balanced_distribution"`
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

// GetConfigurationSchema returns the schema for balance plugin configuration
func GetConfigurationSchema() *plugin_pb.PluginConfig {
	schema := ConfigurationSchema{
		AdminConfig: AdminConfigSchema{
			RebalanceInterval: ConfigField{
				Name:        "rebalance_interval",
				Description: "Time between rebalancing scans",
				Type:        "duration",
				Required:    true,
				Default:     "2h",
				Min:         "10m",
				Max:         "24h",
				Unit:        "seconds",
			},
			MaxConcurrentJobs: ConfigField{
				Name:        "max_concurrent_jobs",
				Description: "Maximum concurrent rebalancing jobs",
				Type:        "integer",
				Required:    true,
				Default:     3,
				Min:         1,
				Max:         10,
			},
			JobTimeout: ConfigField{
				Name:        "job_timeout",
				Description: "Timeout for individual rebalance jobs",
				Type:        "duration",
				Required:    true,
				Default:     "24h",
				Min:         "1h",
				Max:         "72h",
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
			DiskUsageThreshold: ConfigField{
				Name:        "disk_usage_threshold",
				Description: "Trigger rebalancing when disk usage exceeds this percentage",
				Type:        "integer",
				Required:    true,
				Default:     85,
				Min:         50,
				Max:         95,
				Unit:        "percent",
			},
			AcceptableImbalancePercent: ConfigField{
				Name:        "acceptable_imbalance_percent",
				Description: "Acceptable imbalance level before rebalancing",
				Type:        "integer",
				Required:    true,
				Default:     10,
				Min:         1,
				Max:         50,
				Unit:        "percent",
			},
		},
		WorkerConfig: WorkerConfigSchema{
			MinVolumeSize: ConfigField{
				Name:        "min_volume_size",
				Description: "Minimum volume size to consider for rebalancing",
				Type:        "integer",
				Required:    true,
				Default:     500,
				Min:         100,
				Unit:        "MB",
			},
			MaxVolumeSize: ConfigField{
				Name:        "max_volume_size",
				Description: "Maximum volume size to consider for rebalancing",
				Type:        "integer",
				Required:    true,
				Default:     50000,
				Max:         500000,
				Unit:        "MB",
			},
			DataNodeCount: ConfigField{
				Name:        "data_node_count",
				Description: "Expected number of data nodes in cluster",
				Type:        "integer",
				Required:    true,
				Default:     5,
				Min:         2,
				Max:         1000,
			},
			ReplicationFactor: ConfigField{
				Name:        "replication_factor",
				Description: "Default replication factor for volumes",
				Type:        "integer",
				Required:    true,
				Default:     2,
				Min:         1,
				Max:         5,
			},
			PreferBalancedDistribution: ConfigField{
				Name:        "prefer_balanced_distribution",
				Description: "Prefer balanced distribution over other factors",
				Type:        "boolean",
				Required:    true,
				Default:     true,
			},
		},
	}

	data, _ := json.MarshalIndent(schema, "", "  ")

	return &plugin_pb.PluginConfig{
		PluginId: "balance-plugin",
		Properties: map[string]string{
			"schema":                       string(data),
			"rebalance_interval":           "2h",
			"max_concurrent_jobs":          "3",
			"job_timeout":                  "24h",
			"health_check_interval":        "1m",
			"disk_usage_threshold":         "85",
			"acceptable_imbalance_percent": "10",
			"min_volume_size":              "500",
			"max_volume_size":              "50000",
			"data_node_count":              "5",
			"replication_factor":           "2",
			"prefer_balanced_distribution": "true",
		},
	}
}

// DefaultAdminConfig returns default admin configuration
func DefaultAdminConfig() map[string]string {
	return map[string]string{
		"rebalance_interval":           "2h",
		"max_concurrent_jobs":          "3",
		"job_timeout":                  "24h",
		"health_check_interval":        "1m",
		"disk_usage_threshold":         "85",
		"acceptable_imbalance_percent": "10",
	}
}

// DefaultWorkerConfig returns default worker configuration
func DefaultWorkerConfig() map[string]string {
	return map[string]string{
		"min_volume_size":              "500",
		"max_volume_size":              "50000",
		"data_node_count":              "5",
		"replication_factor":           "2",
		"prefer_balanced_distribution": "true",
	}
}
