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
	if cfg.DispatchTick != 5*time.Second {
		t.Errorf("DispatchTick default=%v, want 5s", cfg.DispatchTick)
	}
	if cfg.CheckpointTick != 30*time.Second {
		t.Errorf("CheckpointTick default=%v, want 30s", cfg.CheckpointTick)
	}
	if cfg.RefreshInterval != 5*time.Minute {
		t.Errorf("RefreshInterval default=%v, want 5m", cfg.RefreshInterval)
	}
	if cfg.EventBudget != 0 {
		t.Errorf("EventBudget default=%d, want 0 (unbounded)", cfg.EventBudget)
	}
}

func TestParseConfigOverrides(t *testing.T) {
	admin := map[string]*plugin_pb.ConfigValue{
		"workers": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 4}},
	}
	worker := map[string]*plugin_pb.ConfigValue{
		"dispatch_tick_ms":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 500}},
		"checkpoint_tick_ms":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 2000}},
		"refresh_interval_ms": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 10000}},
		"event_budget":        {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 100}},
	}
	cfg := ParseConfig(admin, worker)
	if cfg.Workers != 4 {
		t.Errorf("Workers=%d, want 4", cfg.Workers)
	}
	if cfg.DispatchTick != 500*time.Millisecond {
		t.Errorf("DispatchTick=%v, want 500ms", cfg.DispatchTick)
	}
	if cfg.CheckpointTick != 2*time.Second {
		t.Errorf("CheckpointTick=%v, want 2s", cfg.CheckpointTick)
	}
	if cfg.RefreshInterval != 10*time.Second {
		t.Errorf("RefreshInterval=%v, want 10s", cfg.RefreshInterval)
	}
	if cfg.EventBudget != 100 {
		t.Errorf("EventBudget=%d, want 100", cfg.EventBudget)
	}
}

func TestParseConfigClampsZeroAndNegative(t *testing.T) {
	worker := map[string]*plugin_pb.ConfigValue{
		"dispatch_tick_ms":    {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
		"checkpoint_tick_ms":  {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
		"refresh_interval_ms": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
		"event_budget":        {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: -1}},
	}
	cfg := ParseConfig(nil, worker)
	if cfg.DispatchTick != 5*time.Second {
		t.Errorf("zero DispatchTick should clamp to default, got %v", cfg.DispatchTick)
	}
	if cfg.EventBudget != 0 {
		t.Errorf("negative EventBudget should clamp to 0, got %d", cfg.EventBudget)
	}
}
