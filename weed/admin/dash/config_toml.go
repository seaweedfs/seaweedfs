package dash

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
)

// TomlConfig is the subset of viper used to read admin.toml values.
type TomlConfig interface {
	IsSet(key string) bool
	GetBool(key string) bool
	GetInt(key string) int
	GetFloat64(key string) float64
	GetString(key string) string
	GetStringSlice(key string) []string
}

// ApplyMaintenanceConfigFromToml writes maintenance task settings declared in
// admin.toml through to the persisted task configs, so they survive data
// directory loss and override admin UI edits on restart. Absent keys keep
// their persisted values.
func (cp *ConfigPersistence) ApplyMaintenanceConfigFromToml(v TomlConfig) error {
	vacuumConf := vacuum.LoadConfigFromPersistence(cp)
	vacuumChanged := applyBaseConfigFromToml(v, "maintenance.vacuum.", &vacuumConf.BaseConfig)
	if k := "maintenance.vacuum.garbage_threshold"; v.IsSet(k) {
		vacuumConf.GarbageThreshold = v.GetFloat64(k)
		vacuumChanged = true
	}
	if k := "maintenance.vacuum.min_volume_age_seconds"; v.IsSet(k) {
		vacuumConf.MinVolumeAgeSeconds = v.GetInt(k)
		vacuumChanged = true
	}

	balanceConf := balance.LoadConfigFromPersistence(cp)
	balanceChanged := applyBaseConfigFromToml(v, "maintenance.balance.", &balanceConf.BaseConfig)
	if k := "maintenance.balance.imbalance_threshold"; v.IsSet(k) {
		balanceConf.ImbalanceThreshold = v.GetFloat64(k)
		balanceChanged = true
	}
	if k := "maintenance.balance.min_server_count"; v.IsSet(k) {
		balanceConf.MinServerCount = v.GetInt(k)
		balanceChanged = true
	}

	ecConf := erasure_coding.LoadConfigFromPersistence(cp)
	ecChanged := applyBaseConfigFromToml(v, "maintenance.erasure_coding.", &ecConf.BaseConfig)
	if k := "maintenance.erasure_coding.fullness_ratio"; v.IsSet(k) {
		ecConf.FullnessRatio = v.GetFloat64(k)
		ecChanged = true
	}
	if k := "maintenance.erasure_coding.quiet_for_seconds"; v.IsSet(k) {
		ecConf.QuietForSeconds = v.GetInt(k)
		ecChanged = true
	}
	if k := "maintenance.erasure_coding.min_size_mb"; v.IsSet(k) {
		ecConf.MinSizeMB = v.GetInt(k)
		ecChanged = true
	}
	if k := "maintenance.erasure_coding.collection_filter"; v.IsSet(k) {
		ecConf.CollectionFilter = v.GetString(k)
		ecChanged = true
	}
	if k := "maintenance.erasure_coding.preferred_tags"; v.IsSet(k) {
		// viper does not split comma-separated values from env vars
		var tags []string
		for _, tag := range v.GetStringSlice(k) {
			tags = append(tags, strings.Split(tag, ",")...)
		}
		ecConf.PreferredTags = util.NormalizeTagList(tags)
		ecChanged = true
	}
	if k := "maintenance.erasure_coding.replica_placement"; v.IsSet(k) {
		ecConf.ReplicaPlacement = v.GetString(k)
		ecChanged = true
	}

	if !vacuumChanged && !balanceChanged && !ecChanged {
		return nil
	}
	if !cp.IsConfigured() {
		return fmt.Errorf("admin.toml maintenance settings require -dataDir to persist")
	}

	if vacuumChanged {
		if err := cp.SaveVacuumTaskPolicy(vacuumConf.ToTaskPolicy()); err != nil {
			return fmt.Errorf("save vacuum task config: %w", err)
		}
		glog.V(0).Infof("Applied [maintenance.vacuum] settings from admin.toml")
	}
	if balanceChanged {
		if err := cp.SaveBalanceTaskPolicy(balanceConf.ToTaskPolicy()); err != nil {
			return fmt.Errorf("save balance task config: %w", err)
		}
		glog.V(0).Infof("Applied [maintenance.balance] settings from admin.toml")
	}
	if ecChanged {
		if err := cp.SaveErasureCodingTaskPolicy(ecConf.ToTaskPolicy()); err != nil {
			return fmt.Errorf("save erasure coding task config: %w", err)
		}
		glog.V(0).Infof("Applied [maintenance.erasure_coding] settings from admin.toml")
	}
	return nil
}

func applyBaseConfigFromToml(v TomlConfig, prefix string, c *base.BaseConfig) bool {
	changed := false
	if k := prefix + "enabled"; v.IsSet(k) {
		c.Enabled = v.GetBool(k)
		changed = true
	}
	if k := prefix + "scan_interval_seconds"; v.IsSet(k) {
		c.ScanIntervalSeconds = v.GetInt(k)
		changed = true
	}
	if k := prefix + "max_concurrent"; v.IsSet(k) {
		c.MaxConcurrent = v.GetInt(k)
		changed = true
	}
	return changed
}

// ApplyPluginConfigFromToml applies admin.toml [maintenance.*] settings to the
// plugin job type config store so they are picked up by the admin UI and plugin
// workers. Unlike ApplyMaintenanceConfigFromToml (which writes to the legacy
// task-policy store that the plugin system does not read), this method updates
// the plugin's PersistedJobTypeConfig (WorkerConfigValues and AdminRuntime).
//
// Requires the plugin to already be initialized.
func (s *AdminServer) ApplyPluginConfigFromToml(v TomlConfig) error {
	if s.GetPlugin() == nil {
		return nil
	}

	applied := false
	for _, section := range pluginConfigSections {
		if s.applyPluginConfigSection(v, section) {
			applied = true
		}
	}
	if applied {
		glog.V(0).Infof("Applied maintenance plugin settings from admin.toml")
	}
	return nil
}

type pluginConfigSection struct {
	jobType string
	prefix  string
	// toml key → function to build the ConfigValue for WorkerConfigValues
	workerValueMappings map[string]func(v TomlConfig, key string) *plugin_pb.ConfigValue
}

var pluginConfigSections = []pluginConfigSection{
	{
		jobType: "vacuum",
		prefix:  "maintenance.vacuum",
		workerValueMappings: map[string]func(v TomlConfig, key string) *plugin_pb.ConfigValue{
			"garbage_threshold":      doubleValue,
			"min_volume_age_seconds": int64Value,
		},
	},
	{
		jobType: "balance",
		prefix:  "maintenance.balance",
		workerValueMappings: map[string]func(v TomlConfig, key string) *plugin_pb.ConfigValue{
			"imbalance_threshold": doubleValue,
			"min_server_count":    int64Value,
		},
	},
	{
		jobType: "erasure_coding",
		prefix:  "maintenance.erasure_coding",
		workerValueMappings: map[string]func(v TomlConfig, key string) *plugin_pb.ConfigValue{
			"fullness_ratio":    doubleValue,
			"quiet_for_seconds": int64Value,
			"min_size_mb":       int64Value,
			"collection_filter": stringValue,
			"replica_placement": stringValue,
		},
	},
}

func doubleValue(v TomlConfig, key string) *plugin_pb.ConfigValue {
	return &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: v.GetFloat64(key)}}
}

func int64Value(v TomlConfig, key string) *plugin_pb.ConfigValue {
	return &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(v.GetInt(key))}}
}

func stringValue(v TomlConfig, key string) *plugin_pb.ConfigValue {
	return &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_StringValue{StringValue: v.GetString(key)}}
}

func (s *AdminServer) applyPluginConfigSection(v TomlConfig, section pluginConfigSection) bool {
	allKeys := []string{"enabled", "retry_limit", "retry_backoff_seconds"}
	for k := range section.workerValueMappings {
		allKeys = append(allKeys, k)
	}

	hasAny := false
	for _, k := range allKeys {
		if v.IsSet(section.prefix + "." + k) {
			hasAny = true
			break
		}
	}
	if !hasAny {
		return false
	}

	cfg, err := s.GetPlugin().LoadJobTypeConfig(section.jobType)
	if err != nil {
		glog.Warningf("load %s plugin config: %v", section.jobType, err)
		return false
	}
	if cfg == nil {
		cfg = &plugin_pb.PersistedJobTypeConfig{
			JobType: section.jobType,
		}
	}
	if cfg.WorkerConfigValues == nil {
		cfg.WorkerConfigValues = make(map[string]*plugin_pb.ConfigValue)
	}
	if cfg.AdminRuntime == nil {
		cfg.AdminRuntime = &plugin_pb.AdminRuntimeConfig{}
	}

	// Common runtime fields shared by all maintenance sections
	if k := section.prefix + ".enabled"; v.IsSet(k) {
		cfg.AdminRuntime.Enabled = v.GetBool(k)
	}
	if k := section.prefix + ".retry_limit"; v.IsSet(k) {
		cfg.AdminRuntime.RetryLimit = int32(v.GetInt(k))
	}
	if k := section.prefix + ".retry_backoff_seconds"; v.IsSet(k) {
		cfg.AdminRuntime.RetryBackoffSeconds = int32(v.GetInt(k))
	}

	// Section-specific worker config values
	for tomlKey, mapper := range section.workerValueMappings {
		k := section.prefix + "." + tomlKey
		if v.IsSet(k) {
			cfg.WorkerConfigValues[tomlKey] = mapper(v, k)
		}
	}

	cfg.UpdatedBy = "admin.toml"

	if err := s.GetPlugin().SaveJobTypeConfig(cfg); err != nil {
		glog.Warningf("save %s plugin config: %v", section.jobType, err)
		return false
	}

	glog.V(0).Infof("Applied [%s] plugin settings from admin.toml", section.prefix)
	return true
}
