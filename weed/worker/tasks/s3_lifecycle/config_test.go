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
}

func TestParseConfigIgnoresWorkerValues(t *testing.T) {
	// Worker-side config form is currently empty. The old
	// max_runtime_minutes knob duplicated the admin scheduler's
	// Execution Timeout and was removed; anything in workerValues is
	// now silently ignored. The single source of truth for the
	// per-Execute wall-clock cap is AdminRuntimeDefaults.ExecutionTimeoutSeconds.
	worker := map[string]*plugin_pb.ConfigValue{
		"max_runtime_minutes": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 120}},
	}
	cfg := ParseConfig(nil, worker)
	if cfg.Workers != shardPipelineGoroutines {
		t.Errorf("Workers default=%d, want %d", cfg.Workers, shardPipelineGoroutines)
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

func TestParseConfigWalkerIntervalDefaultsToZero(t *testing.T) {
	// Unset key keeps WalkerInterval at 0 so dailyrun.runShard fires the
	// walker every pass (the pre-throttle behavior the s3tests fast
	// driver and the in-repo integration tests rely on).
	cfg := ParseConfig(nil, nil)
	if cfg.WalkerInterval != 0 {
		t.Errorf("WalkerInterval default=%v, want 0", cfg.WalkerInterval)
	}
}

func TestParseConfigWalkerIntervalMinutesConvertsToDuration(t *testing.T) {
	admin := map[string]*plugin_pb.ConfigValue{
		WalkerIntervalMinutesAdminKey: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 90}},
	}
	cfg := ParseConfig(admin, nil)
	if want := 90 * time.Minute; cfg.WalkerInterval != want {
		t.Errorf("WalkerInterval=%v, want %v", cfg.WalkerInterval, want)
	}
}

func TestParseConfigWalkerIntervalNegativeStaysZero(t *testing.T) {
	// Negative declarations stay at 0 so the worker keeps "fire every
	// pass" rather than treating the negative as past-due (which would
	// fire every pass anyway — but via a less obvious code path that
	// future readers would have to trace).
	admin := map[string]*plugin_pb.ConfigValue{
		WalkerIntervalMinutesAdminKey: {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: -10}},
	}
	cfg := ParseConfig(admin, nil)
	if cfg.WalkerInterval != 0 {
		t.Errorf("negative WalkerInterval should stay 0, got %v", cfg.WalkerInterval)
	}
}
