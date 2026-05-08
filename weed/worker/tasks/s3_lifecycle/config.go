package s3_lifecycle

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const (
	jobType = "s3_lifecycle"

	defaultWorkers                = 1
	defaultDispatchTickMinutes    = int64(1)
	defaultCheckpointTickSeconds  = int64(30)
	defaultRefreshIntervalMinutes = int64(5)
	defaultMaxRuntimeMinutes      = int64(60)
)

// Config is the parsed AdminConfigForm + WorkerConfigForm view.
type Config struct {
	Workers         int
	DispatchTick    time.Duration
	CheckpointTick  time.Duration
	RefreshInterval time.Duration
	MaxRuntime      time.Duration
}

// ParseConfig pulls the lifecycle Handler config from the merged
// admin+worker config values. Missing fields fall back to defaults.
func ParseConfig(adminValues, workerValues map[string]*plugin_pb.ConfigValue) Config {
	cfg := Config{
		Workers:         int(readInt64(adminValues, "workers", defaultWorkers)),
		DispatchTick:    time.Duration(readInt64(workerValues, "dispatch_tick_minutes", defaultDispatchTickMinutes)) * time.Minute,
		CheckpointTick:  time.Duration(readInt64(workerValues, "checkpoint_tick_seconds", defaultCheckpointTickSeconds)) * time.Second,
		RefreshInterval: time.Duration(readInt64(workerValues, "refresh_interval_minutes", defaultRefreshIntervalMinutes)) * time.Minute,
		MaxRuntime:      time.Duration(readInt64(workerValues, "max_runtime_minutes", defaultMaxRuntimeMinutes)) * time.Minute,
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
	}
	if cfg.DispatchTick <= 0 {
		cfg.DispatchTick = time.Duration(defaultDispatchTickMinutes) * time.Minute
	}
	if cfg.CheckpointTick <= 0 {
		cfg.CheckpointTick = time.Duration(defaultCheckpointTickSeconds) * time.Second
	}
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = time.Duration(defaultRefreshIntervalMinutes) * time.Minute
	}
	if cfg.MaxRuntime <= 0 {
		cfg.MaxRuntime = time.Duration(defaultMaxRuntimeMinutes) * time.Minute
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
		return fallback
	}
	return fallback
}
