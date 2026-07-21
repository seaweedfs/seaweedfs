package dash

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"github.com/spf13/viper"
)

func tomlConfig(t *testing.T, content string) *viper.Viper {
	t.Helper()
	v := viper.New()
	v.SetConfigType("toml")
	if err := v.ReadConfig(strings.NewReader(content)); err != nil {
		t.Fatalf("read toml: %v", err)
	}
	return v
}

func TestApplyMaintenanceConfigFromToml(t *testing.T) {
	dir := t.TempDir()
	cp := NewConfigPersistence(dir)

	// simulate an earlier UI edit
	saved := vacuum.NewDefaultConfig()
	saved.MaxConcurrent = 5
	saved.GarbageThreshold = 0.5
	if err := cp.SaveVacuumTaskPolicy(saved.ToTaskPolicy()); err != nil {
		t.Fatalf("save vacuum policy: %v", err)
	}

	balanceBefore := balance.LoadConfigFromPersistence(cp)

	v := tomlConfig(t, `
[maintenance.vacuum]
garbage_threshold = 0.03
min_volume_age_seconds = 1800

[maintenance.erasure_coding]
enabled = false
fullness_ratio = 0.8
preferred_tags = "Fast, ssd"
`)
	if err := cp.ApplyMaintenanceConfigFromToml(v); err != nil {
		t.Fatalf("apply: %v", err)
	}

	vacuumConf := vacuum.LoadConfigFromPersistence(cp)
	if vacuumConf.GarbageThreshold != 0.03 {
		t.Errorf("garbage threshold = %v, want 0.03", vacuumConf.GarbageThreshold)
	}
	// sub-hour values round up to a whole hour instead of truncating to 0
	if vacuumConf.MinVolumeAgeSeconds != 3600 {
		t.Errorf("min volume age = %v, want 3600", vacuumConf.MinVolumeAgeSeconds)
	}
	if vacuumConf.MaxConcurrent != 5 {
		t.Errorf("max concurrent = %v, want UI-saved 5 preserved", vacuumConf.MaxConcurrent)
	}

	ecConf := erasure_coding.LoadConfigFromPersistence(cp)
	if ecConf.Enabled {
		t.Errorf("ec enabled = true, want false")
	}
	if ecConf.FullnessRatio != 0.8 {
		t.Errorf("fullness ratio = %v, want 0.8", ecConf.FullnessRatio)
	}
	if !reflect.DeepEqual(ecConf.PreferredTags, []string{"fast", "ssd"}) {
		t.Errorf("preferred tags = %v, want [fast ssd]", ecConf.PreferredTags)
	}
	if ecConf.QuietForSeconds != 3600 {
		t.Errorf("quiet for = %v, want default 3600 preserved", ecConf.QuietForSeconds)
	}

	// balance section absent: nothing written
	if _, err := os.Stat(filepath.Join(dir, ConfigSubdir, BalanceTaskConfigFile)); !os.IsNotExist(err) {
		t.Errorf("balance config written without [maintenance.balance] section")
	}
	if got := balance.LoadConfigFromPersistence(cp); !reflect.DeepEqual(got, balanceBefore) {
		t.Errorf("balance config changed without [maintenance.balance] section")
	}
}

func TestApplyMaintenanceConfigFromTomlNoKeys(t *testing.T) {
	dir := t.TempDir()
	cp := NewConfigPersistence(dir)
	if err := cp.ApplyMaintenanceConfigFromToml(viper.New()); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ConfigSubdir)); !os.IsNotExist(err) {
		t.Errorf("conf dir created without any maintenance keys set")
	}
}

func TestApplyMaintenanceConfigFromTomlRequiresDataDir(t *testing.T) {
	cp := NewConfigPersistence("")
	v := tomlConfig(t, `
[maintenance.vacuum]
garbage_threshold = 0.03
`)
	if err := cp.ApplyMaintenanceConfigFromToml(v); err == nil {
		t.Errorf("expected error when maintenance keys are set without a data dir")
	}
}

// TestApplyMaintenanceConfigFromTomlOnlyUpdatesLegacyStore demonstrates the
// bug: ApplyMaintenanceConfigFromToml writes the admin.toml values to the
// legacy task-policy store (<dataDir>/conf/task_vacuum.json) but does NOT
// propagate them to the plugin config store
// (<dataDir>/plugin/job_types/vacuum/config.json) that the admin UI and plugin
// workers actually read. This is the gap that ApplyPluginConfigFromToml fills.
func TestApplyMaintenanceConfigFromTomlOnlyUpdatesLegacyStore(t *testing.T) {
	dir := t.TempDir()
	cp := NewConfigPersistence(dir)

	v := tomlConfig(t, `
[maintenance.vacuum]
garbage_threshold = 0.1
retry_limit = 10
retry_backoff_seconds = 30
`)
	if err := cp.ApplyMaintenanceConfigFromToml(v); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// Legacy store IS updated (this works correctly)
	legacyFile := filepath.Join(dir, ConfigSubdir, VacuumTaskConfigJSONFile)
	if _, err := os.Stat(legacyFile); os.IsNotExist(err) {
		t.Errorf("legacy vacuum config file missing — admin.toml values should be here")
	}
	vacuumConf := vacuum.LoadConfigFromPersistence(cp)
	if vacuumConf.GarbageThreshold != 0.1 {
		t.Errorf("legacy garbage_threshold = %v, want 0.1 (admin.toml applied to legacy store)", vacuumConf.GarbageThreshold)
	}

	// Plugin config store has the DEFAULT (0.3), not the admin.toml value (0.1).
	// The file exists because the plugin bootstrap creates it with descriptor
	// defaults, but ApplyMaintenanceConfigFromToml never touches it.
	pluginConfigFile := filepath.Join(dir, "plugin", "job_types", "vacuum", "config.json")
	if data, err := os.ReadFile(pluginConfigFile); err == nil {
		if strings.Contains(string(data), `"doubleValue": 0.3`) {
			t.Logf("BUG CONFIRMED: plugin config at %s has garbage_threshold=0.3 (default), not 0.1 from admin.toml",
				pluginConfigFile)
			t.Logf("Legacy store has garbage_threshold=%.1f, but the admin UI and plugin workers read the plugin store and see 0.3",
				vacuumConf.GarbageThreshold)
		} else {
			t.Logf("Plugin config file contents:\n%s", string(data))
		}
	} else {
		// File doesn't exist yet — but in production the plugin bootstrap
		// creates it with defaults when the admin server starts.
		t.Logf("Plugin config file not found at %s (will be created by plugin bootstrap with defaults)", pluginConfigFile)
	}
}
