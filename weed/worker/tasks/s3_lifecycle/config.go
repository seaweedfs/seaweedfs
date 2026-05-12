package s3_lifecycle

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const (
	jobType = "s3_lifecycle"

	// In-process fan-out across the 16 shards.
	shardPipelineGoroutines = 1

	defaultMaxRuntimeMinutes = int64(60)
)

type Config struct {
	Workers          int
	MaxRuntime       time.Duration
	MetaLogRetention time.Duration
}

func ParseConfig(adminValues map[string]*plugin_pb.ConfigValue, workerValues map[string]*plugin_pb.ConfigValue) Config {
	cfg := Config{
		Workers:    shardPipelineGoroutines,
		MaxRuntime: time.Duration(readInt64(workerValues, "max_runtime_minutes", defaultMaxRuntimeMinutes)) * time.Minute,
	}
	if cfg.MaxRuntime <= 0 {
		cfg.MaxRuntime = time.Duration(defaultMaxRuntimeMinutes) * time.Minute
	}
	// Operator-declared meta-log retention. Negative or zero values stay
	// zero so runShard falls back to maxTTL (PromotedHash dormant).
	if days := readInt64(adminValues, MetaLogRetentionDaysAdminKey, 0); days > 0 {
		cfg.MetaLogRetention = time.Duration(days) * 24 * time.Hour
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
