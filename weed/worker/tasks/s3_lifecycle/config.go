package s3_lifecycle

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const (
	jobType = "s3_lifecycle"

	defaultWorkers           = 1
	defaultDispatchTickMs    = int64(5 * 1000)
	defaultCheckpointTickMs  = int64(30 * 1000)
	defaultRefreshIntervalMs = int64(5 * 60 * 1000)
	defaultEventBudget       = int64(0) // unbounded — handler runs as a daemon
)

// Config is the parsed AdminConfigForm + WorkerConfigForm view.
type Config struct {
	Workers         int
	DispatchTick    time.Duration
	CheckpointTick  time.Duration
	RefreshInterval time.Duration
	EventBudget     int
}

// ParseConfig pulls the lifecycle Handler config from the merged
// admin+worker config values. Missing fields fall back to defaults.
func ParseConfig(adminValues, workerValues map[string]*plugin_pb.ConfigValue) Config {
	cfg := Config{
		Workers:         int(readInt64(adminValues, "workers", defaultWorkers)),
		DispatchTick:    time.Duration(readInt64(workerValues, "dispatch_tick_ms", defaultDispatchTickMs)) * time.Millisecond,
		CheckpointTick:  time.Duration(readInt64(workerValues, "checkpoint_tick_ms", defaultCheckpointTickMs)) * time.Millisecond,
		RefreshInterval: time.Duration(readInt64(workerValues, "refresh_interval_ms", defaultRefreshIntervalMs)) * time.Millisecond,
		EventBudget:     int(readInt64(workerValues, "event_budget", defaultEventBudget)),
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
	}
	if cfg.DispatchTick <= 0 {
		cfg.DispatchTick = time.Duration(defaultDispatchTickMs) * time.Millisecond
	}
	if cfg.CheckpointTick <= 0 {
		cfg.CheckpointTick = time.Duration(defaultCheckpointTickMs) * time.Millisecond
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = time.Duration(defaultRefreshIntervalMs) * time.Millisecond
	}
	if cfg.EventBudget < 0 {
		cfg.EventBudget = 0
	}
	return cfg
}

func readInt64(values map[string]*plugin_pb.ConfigValue, field string, fallback int64) int64 {
	v, ok := values[field]
	if !ok || v == nil {
		return fallback
	}
	switch k := v.Kind.(type) {
	case *plugin_pb.ConfigValue_Int64Value:
		return k.Int64Value
	case *plugin_pb.ConfigValue_DoubleValue:
		return int64(k.DoubleValue)
	case *plugin_pb.ConfigValue_StringValue:
		return fallback // strict: don't try to atoi here, descriptor types are explicit
	}
	return fallback
}

func readString(values map[string]*plugin_pb.ConfigValue, field, fallback string) string {
	v, ok := values[field]
	if !ok || v == nil {
		return fallback
	}
	if k, ok := v.Kind.(*plugin_pb.ConfigValue_StringValue); ok {
		return k.StringValue
	}
	return fallback
}
