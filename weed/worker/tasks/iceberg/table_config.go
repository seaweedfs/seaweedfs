package iceberg

import (
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// Iceberg table properties that describe the physical layout maintenance has
// to produce.
const (
	propTargetFileSize       = "write.target-file-size-bytes"
	propDeleteTargetFileSize = "write.delete.target-file-size-bytes"
	propMaxSnapshotAgeMs     = "history.expire.max-snapshot-age-ms"
	propMinSnapshotsToKeep   = "history.expire.min-snapshots-to-keep"
)

// resolveTableConfig layers a table's own properties over the worker config.
// Properties win: a writer targeting 512 MiB and a compactor rewriting to
// 256 MiB would rewrite each other's output forever.
func resolveTableConfig(base Config, props iceberg.Properties) Config {
	cfg := base
	if v, ok := propInt64(props, propTargetFileSize); ok {
		cfg.TargetFileSizeBytes = v
	}
	if v, ok := propInt64(props, propDeleteTargetFileSize); ok {
		cfg.DeleteTargetFileSizeBytes = v
	}
	if v, ok := propInt64(props, propMaxSnapshotAgeMs); ok {
		cfg.SnapshotRetentionMs = v
	}
	if v, ok := propInt64(props, propMinSnapshotsToKeep); ok {
		cfg.MaxSnapshotsToKeep = v
	}
	return applyThresholdDefaults(cfg)
}

// propInt64 reads a positive integer table property. Anything unset,
// unparseable or non-positive leaves the worker config in place so a
// misconfigured table never stops its own maintenance.
func propInt64(props iceberg.Properties, key string) (int64, bool) {
	raw, ok := props[key]
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		glog.V(1).Infof("iceberg maintenance: ignoring table property %s=%q: not an integer", key, raw)
		return 0, false
	}
	if value <= 0 {
		glog.V(1).Infof("iceberg maintenance: ignoring table property %s=%d: not positive", key, value)
		return 0, false
	}
	return value, true
}
