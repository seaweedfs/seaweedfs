package dash

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	adminplugin "github.com/seaweedfs/seaweedfs/weed/admin/plugin"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
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

func newPluginServer(t *testing.T) *AdminServer {
	t.Helper()
	p, err := adminplugin.New(adminplugin.Options{})
	if err != nil {
		t.Fatalf("new plugin: %v", err)
	}
	t.Cleanup(p.Shutdown)
	return &AdminServer{plugin: p}
}

func TestApplyPluginConfigFromTomlUpdatesExistingConfig(t *testing.T) {
	s := newPluginServer(t)
	seed := &plugin_pb.PersistedJobTypeConfig{
		JobType:      "vacuum",
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{Enabled: true, RetryLimit: 1, RetryBackoffSeconds: 10},
		WorkerConfigValues: map[string]*plugin_pb.ConfigValue{
			"garbage_threshold": {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.3}},
		},
	}
	if err := s.GetPlugin().SaveJobTypeConfig(seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	v := tomlConfig(t, `
[maintenance.vacuum]
garbage_threshold = 0.1
retry_limit = 5
`)
	if err := s.ApplyPluginConfigFromToml(v); err != nil {
		t.Fatalf("apply: %v", err)
	}

	cfg, err := s.GetPlugin().LoadJobTypeConfig("vacuum")
	if err != nil || cfg == nil {
		t.Fatalf("load: %v %v", cfg, err)
	}
	if got := cfg.WorkerConfigValues["garbage_threshold"].GetDoubleValue(); got != 0.1 {
		t.Errorf("garbage_threshold = %v, want 0.1", got)
	}
	if cfg.AdminRuntime.RetryLimit != 5 {
		t.Errorf("retry_limit = %d, want 5", cfg.AdminRuntime.RetryLimit)
	}
	if !cfg.AdminRuntime.Enabled {
		t.Errorf("enabled flipped off by unrelated toml keys")
	}
	if cfg.AdminRuntime.RetryBackoffSeconds != 10 {
		t.Errorf("retry_backoff_seconds = %d, want 10 kept", cfg.AdminRuntime.RetryBackoffSeconds)
	}
	if cfg.UpdatedBy != "admin.toml" {
		t.Errorf("updated_by = %q, want admin.toml", cfg.UpdatedBy)
	}
}

func TestApplyPluginConfigFromTomlClampsRetryValues(t *testing.T) {
	s := newPluginServer(t)
	seed := &plugin_pb.PersistedJobTypeConfig{
		JobType:      "vacuum",
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{Enabled: true},
	}
	if err := s.GetPlugin().SaveJobTypeConfig(seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	v := tomlConfig(t, `
[maintenance.vacuum]
retry_limit = 68719476736
retry_backoff_seconds = -5
`)
	if err := s.ApplyPluginConfigFromToml(v); err != nil {
		t.Fatalf("apply: %v", err)
	}

	cfg, err := s.GetPlugin().LoadJobTypeConfig("vacuum")
	if err != nil || cfg == nil {
		t.Fatalf("load: %v %v", cfg, err)
	}
	if cfg.AdminRuntime.RetryLimit != math.MaxInt32 {
		t.Errorf("retry_limit = %d, want clamped to MaxInt32", cfg.AdminRuntime.RetryLimit)
	}
	if cfg.AdminRuntime.RetryBackoffSeconds != 0 {
		t.Errorf("retry_backoff_seconds = %d, want clamped to 0", cfg.AdminRuntime.RetryBackoffSeconds)
	}
}

func TestApplyPluginConfigFromTomlLeavesMissingConfigToBootstrap(t *testing.T) {
	s := newPluginServer(t)
	v := tomlConfig(t, `
[maintenance.vacuum]
garbage_threshold = 0.1
`)
	if err := s.ApplyPluginConfigFromToml(v); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if cfg, _ := s.GetPlugin().LoadJobTypeConfig("vacuum"); cfg != nil {
		t.Errorf("bare config created; descriptor bootstrap would be skipped and disable the job type")
	}
}

func TestApplyPluginTomlDefaultsOverlaysBootstrapConfig(t *testing.T) {
	cfg := &plugin_pb.PersistedJobTypeConfig{
		JobType:      "vacuum",
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{Enabled: true, RetryLimit: 1},
		WorkerConfigValues: map[string]*plugin_pb.ConfigValue{
			"garbage_threshold": {Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.3}},
		},
		UpdatedBy: "plugin",
	}
	v := tomlConfig(t, `
[maintenance.vacuum]
garbage_threshold = 0.1
`)
	applyPluginTomlDefaults(v, cfg)
	if got := cfg.WorkerConfigValues["garbage_threshold"].GetDoubleValue(); got != 0.1 {
		t.Errorf("garbage_threshold = %v, want 0.1", got)
	}
	if !cfg.AdminRuntime.Enabled || cfg.AdminRuntime.RetryLimit != 1 {
		t.Errorf("descriptor defaults lost: %v", cfg.AdminRuntime)
	}
	if cfg.UpdatedBy != "admin.toml" {
		t.Errorf("updated_by = %q, want admin.toml", cfg.UpdatedBy)
	}
}

func TestApplyPluginConfigFromTomlErasureCodingKeyPlacement(t *testing.T) {
	s := newPluginServer(t)
	seed := &plugin_pb.PersistedJobTypeConfig{
		JobType:      "erasure_coding",
		AdminRuntime: &plugin_pb.AdminRuntimeConfig{Enabled: true},
	}
	if err := s.GetPlugin().SaveJobTypeConfig(seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	v := tomlConfig(t, `
[maintenance.erasure_coding]
collection_filter = "important"
replica_placement = "020"
preferred_tags = ["fast,SSD"]
`)
	if err := s.ApplyPluginConfigFromToml(v); err != nil {
		t.Fatalf("apply: %v", err)
	}

	cfg, err := s.GetPlugin().LoadJobTypeConfig("erasure_coding")
	if err != nil || cfg == nil {
		t.Fatalf("load: %v %v", cfg, err)
	}
	// collection_filter is read from the admin values by all workers
	if got := cfg.AdminConfigValues["collection_filter"].GetStringValue(); got != "important" {
		t.Errorf("admin collection_filter = %q, want important", got)
	}
	if _, ok := cfg.WorkerConfigValues["collection_filter"]; ok {
		t.Errorf("collection_filter must not land in worker values")
	}
	if got := cfg.WorkerConfigValues["replica_placement"].GetStringValue(); got != "020" {
		t.Errorf("replica_placement = %q, want 020", got)
	}
	if got := cfg.WorkerConfigValues["preferred_tags"].GetStringList().GetValues(); !reflect.DeepEqual(got, []string{"fast", "ssd"}) {
		t.Errorf("preferred_tags = %v, want [fast ssd]", got)
	}
}
