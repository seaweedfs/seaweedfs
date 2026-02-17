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
RebalanceInterval        ConfigField `json:"rebalance_interval"`
MaxConcurrentJobs        ConfigField `json:"max_concurrent_jobs"`
JobTimeout               ConfigField `json:"job_timeout"`
HealthCheckInterval      ConfigField `json:"health_check_interval"`
DiskUsageThreshold       ConfigField `json:"disk_usage_threshold"`
AcceptableImbalancePercent ConfigField `json:"acceptable_imbalance_percent"`
}

// WorkerConfigSchema defines worker-side configuration
type WorkerConfigSchema struct {
MinVolumeSize               ConfigField `json:"min_volume_size"`
MaxVolumeSize               ConfigField `json:"max_volume_size"`
DataNodeCount               ConfigField `json:"data_node_count"`
ReplicationFactor           ConfigField `json:"replication_factor"`
PreferBalancedDistribution  ConfigField `json:"prefer_balanced_distribution"`
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
Description: "Time between rebalance scans",
Type:        "duration",
Required:    true,
Default:     "2h",
Min:         "30m",
Max:         "12h",
Unit:        "seconds",
},
MaxConcurrentJobs: ConfigField{
Name:        "max_concurrent_jobs",
Description: "Maximum concurrent rebalance jobs",
Type:        "integer",
Required:    true,
Default:     2,
Min:         1,
Max:         5,
},
JobTimeout: ConfigField{
Name:        "job_timeout",
Description: "Timeout for individual rebalance jobs",
Type:        "duration",
Required:    true,
Default:     "6h",
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
DiskUsageThreshold: ConfigField{
Name:        "disk_usage_threshold",
Description: "Disk usage threshold for triggering rebalance",
Type:        "integer",
Required:    true,
Default:     85,
Min:         50,
Max:         95,
Unit:        "percent",
},
AcceptableImbalancePercent: ConfigField{
Name:        "acceptable_imbalance_percent",
Description: "Acceptable imbalance percentage",
Type:        "integer",
Required:    true,
Default:     10,
Min:         1,
Max:         30,
Unit:        "percent",
},
},
WorkerConfig: WorkerConfigSchema{
MinVolumeSize: ConfigField{
Name:        "min_volume_size",
Description: "Minimum volume size to rebalance",
Type:        "integer",
Required:    true,
Default:     500,
Min:         100,
Unit:        "MB",
},
MaxVolumeSize: ConfigField{
Name:        "max_volume_size",
Description: "Maximum volume size to rebalance",
Type:        "integer",
Required:    true,
Default:     10000,
Max:         100000,
Unit:        "MB",
},
DataNodeCount: ConfigField{
Name:        "data_node_count",
Description: "Number of data nodes in cluster",
Type:        "integer",
Required:    true,
Default:     10,
Min:         1,
Max:         1000,
},
ReplicationFactor: ConfigField{
Name:        "replication_factor",
Description: "Replication factor for volumes",
Type:        "integer",
Required:    true,
Default:     2,
Min:         1,
Max:         5,
},
PreferBalancedDistribution: ConfigField{
Name:        "prefer_balanced_distribution",
Description: "Prefer balanced distribution",
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
"schema":                        string(data),
"rebalance_interval":            "2h",
"max_concurrent_jobs":           "2",
"job_timeout":                   "6h",
"health_check_interval":         "30s",
"disk_usage_threshold":          "85",
"acceptable_imbalance_percent":  "10",
"min_volume_size":               "500",
"max_volume_size":               "10000",
"data_node_count":               "10",
"replication_factor":            "2",
"prefer_balanced_distribution":  "true",
},
}
}

// DefaultAdminConfig returns default admin configuration
func DefaultAdminConfig() map[string]string {
return map[string]string{
"rebalance_interval":           "2h",
"max_concurrent_jobs":          "2",
"job_timeout":                  "6h",
"health_check_interval":        "30s",
"disk_usage_threshold":         "85",
"acceptable_imbalance_percent": "10",
}
}

// DefaultWorkerConfig returns default worker configuration
func DefaultWorkerConfig() map[string]string {
return map[string]string{
"min_volume_size":              "500",
"max_volume_size":              "10000",
"data_node_count":              "10",
"replication_factor":           "2",
"prefer_balanced_distribution": "true",
}
}
