package dash

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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
		ecConf.PreferredTags = v.GetStringSlice(k)
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
