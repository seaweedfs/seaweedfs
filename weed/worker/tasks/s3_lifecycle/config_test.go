package s3_lifecycle

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

func TestParseConfigDefaults(t *testing.T) {
	cfg := ParseConfig(nil, nil)
	if cfg.Workers != defaultWorkers {
		t.Errorf("Workers default=%d, want %d", cfg.Workers, defaultWorkers)
	}
	if cfg.DispatchTick != 1*time.Minute {
		t.Errorf("DispatchTick default=%v, want 1m", cfg.DispatchTick)
	}
	if cfg.CheckpointTick != 30*time.Second {
		t.Errorf("CheckpointTick default=%v, want 30s", cfg.CheckpointTick)
	}
	if cfg.RefreshInterval != 5*time.Minute {
		t.Errorf("RefreshInterval default=%v, want 5m", cfg.RefreshInterval)
	}
	if cfg.MaxRuntime != 60*time.Minute {
		t.Errorf("MaxRuntime default=%v, want 60m", cfg.MaxRuntime)
	}
}

func TestParseConfigOverrides(t *testing.T) {
	admin := map[string]*plugin_pb.ConfigValue{
		"workers": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 4}},
	}
	worker := map[string]*plugin_pb.ConfigValue{
		"dispatch_tick_minutes":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2}},
		"checkpoint_tick_seconds":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 15}},
		"refresh_interval_minutes": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 10}},
		"max_runtime_minutes":      {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 120}},
	}
	cfg := ParseConfig(admin, worker)
	if cfg.Workers != 4 {
		t.Errorf("Workers=%d, want 4", cfg.Workers)
	}
	if cfg.DispatchTick != 2*time.Minute {
		t.Errorf("DispatchTick=%v, want 2m", cfg.DispatchTick)
	}
	if cfg.CheckpointTick != 15*time.Second {
		t.Errorf("CheckpointTick=%v, want 15s", cfg.CheckpointTick)
	}
	if cfg.RefreshInterval != 10*time.Minute {
		t.Errorf("RefreshInterval=%v, want 10m", cfg.RefreshInterval)
	}
	if cfg.MaxRuntime != 120*time.Minute {
		t.Errorf("MaxRuntime=%v, want 120m", cfg.MaxRuntime)
	}
}

func TestParseConfigClampsZeroAndNegative(t *testing.T) {
	worker := map[string]*plugin_pb.ConfigValue{
		"dispatch_tick_minutes":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
		"checkpoint_tick_seconds":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
		"refresh_interval_minutes": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
		"max_runtime_minutes":      {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: -5}},
	}
	cfg := ParseConfig(nil, worker)
	if cfg.DispatchTick != 1*time.Minute {
		t.Errorf("zero DispatchTick should clamp to default, got %v", cfg.DispatchTick)
	}
	if cfg.CheckpointTick != 30*time.Second {
		t.Errorf("zero CheckpointTick should clamp to default, got %v", cfg.CheckpointTick)
	}
	if cfg.RefreshInterval != 5*time.Minute {
		t.Errorf("zero RefreshInterval should clamp to default, got %v", cfg.RefreshInterval)
	}
	if cfg.MaxRuntime != 60*time.Minute {
		t.Errorf("negative MaxRuntime should clamp to default, got %v", cfg.MaxRuntime)
	}
}
