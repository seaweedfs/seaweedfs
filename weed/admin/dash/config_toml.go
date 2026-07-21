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
	"google.golang.org/protobuf/types/known/timestamppb"
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

// ApplyPluginConfigFromToml overlays admin.toml [maintenance.*] settings onto
// existing plugin job type configs. Configs that do not exist yet get the
// overlay in applyPluginTomlDefaults when the plugin bootstraps them from a
// worker descriptor, so descriptor defaults are never lost.
func (s *AdminServer) ApplyPluginConfigFromToml(v TomlConfig) error {
	plugin := s.GetPlugin()
	if plugin == nil {
		return nil
	}
	for _, section := range pluginConfigSections {
		cfg, err := plugin.LoadJobTypeConfig(section.jobType)
		if err != nil {
			return fmt.Errorf("load %s plugin config: %w", section.jobType, err)
		}
		if cfg == nil {
			continue
		}
		if !section.applyToml(v, cfg) {
			continue
		}
		if err := plugin.SaveJobTypeConfig(cfg); err != nil {
			return fmt.Errorf("save %s plugin config: %w", section.jobType, err)
		}
		glog.V(0).Infof("Applied [%s] settings from admin.toml to plugin config", section.prefix)
	}
	return nil
}

// applyPluginTomlDefaults overlays admin.toml onto a job type config the
// plugin is bootstrapping from descriptor defaults.
func applyPluginTomlDefaults(v TomlConfig, cfg *plugin_pb.PersistedJobTypeConfig) {
	for _, section := range pluginConfigSections {
		if section.jobType == cfg.JobType {
			if section.applyToml(v, cfg) {
				glog.V(0).Infof("Applied [%s] settings from admin.toml to plugin defaults", section.prefix)
			}
			return
		}
	}
}

type pluginConfigSection struct {
	jobType    string
	prefix     string
	workerKeys map[string]func(v TomlConfig, key string) *plugin_pb.ConfigValue
	adminKeys  map[string]func(v TomlConfig, key string) *plugin_pb.ConfigValue
}

var pluginConfigSections = []pluginConfigSection{
	{
		jobType: "vacuum",
		prefix:  "maintenance.vacuum",
		workerKeys: map[string]func(v TomlConfig, key string) *plugin_pb.ConfigValue{
			"garbage_threshold":      doubleValue,
			"min_volume_age_seconds": int64Value,
		},
	},
	{
		jobType: "volume_balance",
		prefix:  "maintenance.balance",
		workerKeys: map[string]func(v TomlConfig, key string) *plugin_pb.ConfigValue{
			"imbalance_threshold": doubleValue,
			"min_server_count":    int64Value,
		},
	},
	{
		jobType: "erasure_coding",
		prefix:  "maintenance.erasure_coding",
		workerKeys: map[string]func(v TomlConfig, key string) *plugin_pb.ConfigValue{
			"fullness_ratio":    doubleValue,
			"quiet_for_seconds": int64Value,
			"min_size_mb":       int64Value,
			"preferred_tags":    stringListValue,
			"replica_placement": stringValue,
		},
		// workers read collection_filter from the admin values, not the worker values
		adminKeys: map[string]func(v TomlConfig, key string) *plugin_pb.ConfigValue{
			"collection_filter": stringValue,
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

func stringListValue(v TomlConfig, key string) *plugin_pb.ConfigValue {
	// viper does not split comma-separated values from env vars
	var items []string
	for _, item := range v.GetStringSlice(key) {
		items = append(items, strings.Split(item, ",")...)
	}
	return &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_StringList{StringList: &plugin_pb.StringList{Values: util.NormalizeTagList(items)}}}
}

func (section pluginConfigSection) applyToml(v TomlConfig, cfg *plugin_pb.PersistedJobTypeConfig) bool {
	changed := false
	if k := section.prefix + ".enabled"; v.IsSet(k) {
		ensureAdminRuntime(cfg).Enabled = v.GetBool(k)
		changed = true
	}
	if k := section.prefix + ".retry_limit"; v.IsSet(k) {
		ensureAdminRuntime(cfg).RetryLimit = int32(v.GetInt(k))
		changed = true
	}
	if k := section.prefix + ".retry_backoff_seconds"; v.IsSet(k) {
		ensureAdminRuntime(cfg).RetryBackoffSeconds = int32(v.GetInt(k))
		changed = true
	}
	for tomlKey, mapper := range section.workerKeys {
		if k := section.prefix + "." + tomlKey; v.IsSet(k) {
			if cfg.WorkerConfigValues == nil {
				cfg.WorkerConfigValues = make(map[string]*plugin_pb.ConfigValue)
			}
			cfg.WorkerConfigValues[tomlKey] = mapper(v, k)
			changed = true
		}
	}
	for tomlKey, mapper := range section.adminKeys {
		if k := section.prefix + "." + tomlKey; v.IsSet(k) {
			if cfg.AdminConfigValues == nil {
				cfg.AdminConfigValues = make(map[string]*plugin_pb.ConfigValue)
			}
			cfg.AdminConfigValues[tomlKey] = mapper(v, k)
			changed = true
		}
	}
	if changed {
		cfg.UpdatedAt = timestamppb.Now()
		cfg.UpdatedBy = "admin.toml"
	}
	return changed
}

func ensureAdminRuntime(cfg *plugin_pb.PersistedJobTypeConfig) *plugin_pb.AdminRuntimeConfig {
	if cfg.AdminRuntime == nil {
		// maintenance job types are enabled by default; a bare runtime must not disable them
		cfg.AdminRuntime = &plugin_pb.AdminRuntimeConfig{Enabled: true}
	}
	return cfg.AdminRuntime
}
