package s3_lifecycle

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const (
	jobType = "s3_lifecycle"

	// In-process fan-out across the 16 shards.
	shardPipelineGoroutines = 1

	defaultDispatchTickMinutes      = int64(1)
	defaultCheckpointTickSeconds    = int64(30)
	defaultRefreshIntervalMinutes   = int64(5)
	defaultMaxRuntimeMinutes        = int64(60)
	defaultBootstrapIntervalMinutes = int64(0) // 0 = walk once per process

	AlgorithmDailyReplay = "daily_replay"
	AlgorithmStreaming   = "streaming"

	defaultAlgorithm = AlgorithmDailyReplay
)

type Config struct {
	Workers           int
	DispatchTick      time.Duration
	CheckpointTick    time.Duration
	RefreshInterval   time.Duration
	BootstrapInterval time.Duration
	MaxRuntime        time.Duration
	Algorithm         string
}

func ParseConfig(adminValues, workerValues map[string]*plugin_pb.ConfigValue) Config {
	cfg := Config{
		Workers:           shardPipelineGoroutines,
		DispatchTick:      time.Duration(readInt64(workerValues, "dispatch_tick_minutes", defaultDispatchTickMinutes)) * time.Minute,
		CheckpointTick:    time.Duration(readInt64(workerValues, "checkpoint_tick_seconds", defaultCheckpointTickSeconds)) * time.Second,
		RefreshInterval:   time.Duration(readInt64(workerValues, "refresh_interval_minutes", defaultRefreshIntervalMinutes)) * time.Minute,
		BootstrapInterval: time.Duration(readInt64(workerValues, "bootstrap_interval_minutes", defaultBootstrapIntervalMinutes)) * time.Minute,
		MaxRuntime:        time.Duration(readInt64(workerValues, "max_runtime_minutes", defaultMaxRuntimeMinutes)) * time.Minute,
		Algorithm:         readString(adminValues, "algorithm", defaultAlgorithm),
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
	// Zero means walk-once-per-process; clamp negatives only.
	if cfg.BootstrapInterval < 0 {
		cfg.BootstrapInterval = 0
	}
	if cfg.MaxRuntime <= 0 {
		cfg.MaxRuntime = time.Duration(defaultMaxRuntimeMinutes) * time.Minute
	}
	switch cfg.Algorithm {
	case AlgorithmStreaming, AlgorithmDailyReplay:
	default:
		cfg.Algorithm = defaultAlgorithm
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
