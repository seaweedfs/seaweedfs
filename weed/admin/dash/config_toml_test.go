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
min_volume_age_seconds = 3600

[maintenance.erasure_coding]
enabled = false
fullness_ratio = 0.8
preferred_tags = ["ssd"]
`)
	if err := cp.ApplyMaintenanceConfigFromToml(v); err != nil {
		t.Fatalf("apply: %v", err)
	}

	vacuumConf := vacuum.LoadConfigFromPersistence(cp)
	if vacuumConf.GarbageThreshold != 0.03 {
		t.Errorf("garbage threshold = %v, want 0.03", vacuumConf.GarbageThreshold)
	}
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
	if !reflect.DeepEqual(ecConf.PreferredTags, []string{"ssd"}) {
		t.Errorf("preferred tags = %v, want [ssd]", ecConf.PreferredTags)
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
