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
