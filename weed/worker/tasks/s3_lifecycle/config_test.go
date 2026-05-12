package s3_lifecycle

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestParseConfigDefaults(t *testing.T) {
	cfg := ParseConfig(nil, nil)
	if cfg.Workers != shardPipelineGoroutines {
		t.Errorf("Workers default=%d, want %d", cfg.Workers, shardPipelineGoroutines)
	}
	if cfg.MaxRuntime != 60*time.Minute {
		t.Errorf("MaxRuntime default=%v, want 60m", cfg.MaxRuntime)
	}
}

func TestParseConfigOverrideMaxRuntime(t *testing.T) {
	worker := map[string]*plugin_pb.ConfigValue{
		"max_runtime_minutes": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 120}},
	}
	cfg := ParseConfig(nil, worker)
	if cfg.MaxRuntime != 120*time.Minute {
		t.Errorf("MaxRuntime=%v, want 120m", cfg.MaxRuntime)
	}
}

func TestParseConfigNegativeMaxRuntimeClampsToDefault(t *testing.T) {
	worker := map[string]*plugin_pb.ConfigValue{
		"max_runtime_minutes": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: -5}},
	}
	cfg := ParseConfig(nil, worker)
	if cfg.MaxRuntime != 60*time.Minute {
		t.Errorf("negative MaxRuntime should clamp to default, got %v", cfg.MaxRuntime)
	}
}

func TestParseConfigMetaLogRetentionDefaultsToZero(t *testing.T) {
	// Unset key keeps MetaLogRetention at 0, which runShard treats as
	// "no retention info supplied" and falls back to maxTTL.
	cfg := ParseConfig(nil, nil)
	if cfg.MetaLogRetention != 0 {
		t.Errorf("MetaLogRetention default=%v, want 0", cfg.MetaLogRetention)
	}
}

func TestParseConfigMetaLogRetentionDaysConvertsToDuration(t *testing.T) {
	admin := map[string]*plugin_pb.ConfigValue{
		MetaLogRetentionDaysAdminKey: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 7}},
	}
	cfg := ParseConfig(admin, nil)
	if want := 7 * 24 * time.Hour; cfg.MetaLogRetention != want {
		t.Errorf("MetaLogRetention=%v, want %v (7 days)", cfg.MetaLogRetention, want)
	}
}

func TestParseConfigMetaLogRetentionNegativeStaysZero(t *testing.T) {
	// A negative declaration is nonsense; stay at 0 so runShard's
	// fallback applies rather than producing a negative window.
	admin := map[string]*plugin_pb.ConfigValue{
		MetaLogRetentionDaysAdminKey: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: -3}},
	}
	cfg := ParseConfig(admin, nil)
	if cfg.MetaLogRetention != 0 {
		t.Errorf("negative MetaLogRetention should stay 0, got %v", cfg.MetaLogRetention)
	}
}
