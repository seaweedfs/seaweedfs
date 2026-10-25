package s3_lifecycle

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const (
	jobType = "s3_lifecycle"

	// In-process fan-out across the 16 shards.
	shardPipelineGoroutines = 1
)

type Config struct {
	Workers          int
	MetaLogRetention time.Duration
	// WalkerInterval is the minimum time between steady-state walker
	// fires per shard. 0 means "fire on every run", preserving prior
	// behavior; positive values gate the walker via Cursor.LastWalkedNs
	// inside dailyrun.runShard.
	WalkerInterval time.Duration
}

func ParseConfig(adminValues map[string]*plugin_pb.ConfigValue, workerValues map[string]*plugin_pb.ConfigValue) Config {
	cfg := Config{
		Workers: shardPipelineGoroutines,
	}
	_ = workerValues // worker-side config form is currently empty; reserved for future per-worker tuning.
	// Operator-declared meta-log retention. Negative or zero values stay
	// zero so runShard falls back to maxTTL (PromotedHash dormant).
	// Convert days->hours in int64 space before lifting to time.Duration
	// so the unit is unambiguous.
	if days := readInt64(adminValues, MetaLogRetentionDaysAdminKey, 0); days > 0 {
		cfg.MetaLogRetention = time.Duration(days*24) * time.Hour
	}
	// Walker throttle. Negative / zero stay zero so dailyrun.runShard
	// keeps the prior "fire every pass" semantics — important for in-
	// repo integration tests and s3tests's sub-minute driver. Positive
	// values throttle the steady-state walker per shard.
	if mins := readInt64(adminValues, WalkerIntervalMinutesAdminKey, 0); mins > 0 {
		cfg.WalkerInterval = time.Duration(mins) * time.Minute
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
