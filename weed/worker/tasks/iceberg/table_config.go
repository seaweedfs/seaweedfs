package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// Iceberg table properties that describe the physical layout maintenance has
// to produce.
const (
	propTargetFileSize       = "write.target-file-size-bytes"
	propDeleteTargetFileSize = "write.delete.target-file-size-bytes"
	propMaxSnapshotAgeMs     = "history.expire.max-snapshot-age-ms"
	propMinSnapshotsToKeep   = "history.expire.min-snapshots-to-keep"
)

// resolveTableConfig layers a table's own intent over the worker config, from
// the maintenance configuration and from the table's Iceberg properties.
//
// Properties win by default: a writer targeting 512 MiB and a compactor
// rewriting to 256 MiB would rewrite each other's output forever, and the
// property is the signal every engine already honours. Operators who want the
// control plane to be authoritative instead can clear
// table_properties_override.
func resolveTableConfig(base Config, props iceberg.Properties, maintenance s3tables.MaintenanceConfiguration) Config {
	cfg := base
	if base.TablePropertiesOverride {
		cfg = applyMaintenanceConfig(cfg, maintenance)
		cfg = applyTableProperties(cfg, props)
	} else {
		cfg = applyTableProperties(cfg, props)
		cfg = applyMaintenanceConfig(cfg, maintenance)
	}
	return applyThresholdDefaults(cfg)
}

func applyTableProperties(cfg Config, props iceberg.Properties) Config {
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
	return cfg
}

func applyMaintenanceConfig(cfg Config, maintenance s3tables.MaintenanceConfiguration) Config {
	if s := compactionSettings(maintenance); s != nil {
		if v := settingValue(s.TargetFileSizeMB); v > 0 {
			cfg.TargetFileSizeBytes = mbToBytes(v)
		}
		switch s.Strategy {
		case s3tables.CompactionStrategyBinpack, s3tables.CompactionStrategySort, s3tables.CompactionStrategyAuto:
			cfg.RewriteStrategy = s.Strategy
		}
	}
	if s := snapshotSettings(maintenance); s != nil {
		if v := settingValue(s.MaxSnapshotAgeHours); v > 0 {
			cfg.SnapshotRetentionMs = hoursToMs(v)
		}
		if v := settingValue(s.MinSnapshotsToKeep); v > 0 {
			cfg.MaxSnapshotsToKeep = v
		}
	}
	// AWS marks a file non-current after unreferencedDays and deletes it a
	// further nonCurrentDays later. remove_orphans deletes in one step, so the
	// cutoff has to be the sum or the recovery window disappears.
	if s := orphanSettings(maintenance); s != nil {
		if days := addDays(settingValue(s.UnreferencedDays), settingValue(s.NonCurrentDays)); days > 0 {
			cfg.OrphanOlderThanHours = daysToHours(days)
		}
	}
	return cfg
}

// settingValue reads an optional maintenance setting, treating unset and
// nonsensical values alike so the worker keeps its own configuration.
func settingValue(v *int64) int64 {
	if v == nil || *v < 0 {
		return 0
	}
	return *v
}

func compactionSettings(maintenance s3tables.MaintenanceConfiguration) *s3tables.IcebergCompactionSettings {
	if v := enabledSettings(maintenance, s3tables.MaintenanceTypeIcebergCompaction); v != nil {
		return v.IcebergCompaction
	}
	return nil
}

func snapshotSettings(maintenance s3tables.MaintenanceConfiguration) *s3tables.IcebergSnapshotManagementSettings {
	if v := enabledSettings(maintenance, s3tables.MaintenanceTypeIcebergSnapshotManagement); v != nil {
		return v.IcebergSnapshotManagement
	}
	return nil
}

func orphanSettings(maintenance s3tables.MaintenanceConfiguration) *s3tables.IcebergUnreferencedFileRemovalSettings {
	if v := enabledSettings(maintenance, s3tables.MaintenanceTypeIcebergUnreferencedFileRemoval); v != nil {
		return v.IcebergUnreferencedFileRemoval
	}
	return nil
}

// enabledSettings returns the settings for a maintenance type only when it is
// switched on; a disabled type drops its operations instead of retuning them.
func enabledSettings(maintenance s3tables.MaintenanceConfiguration, maintenanceType string) *s3tables.MaintenanceSettings {
	value, ok := maintenance[maintenanceType]
	if !ok || value == nil || value.Status == s3tables.MaintenanceStatusDisabled {
		return nil
	}
	return value.Settings
}

var mergeMaintenanceConfiguration = s3tables.MergeMaintenanceConfiguration

// maintenanceJobType maps a worker operation onto the AWS job type that governs
// it. Manifest and delete-file rewrites have no AWS equivalent and ride along
// with compaction, so disabling compaction stops all rewriting.
func maintenanceJobType(op string) string {
	switch op {
	case "compact", "rewrite_position_delete_files", "rewrite_manifests":
		return s3tables.MaintenanceTypeIcebergCompaction
	case "expire_snapshots":
		return s3tables.MaintenanceTypeIcebergSnapshotManagement
	case "remove_orphans":
		return s3tables.MaintenanceTypeIcebergUnreferencedFileRemoval
	}
	return ""
}

// filterDisabledOperations drops the operations the maintenance configuration
// switched off. No table property can re-enable them.
func filterDisabledOperations(ops []string, maintenance s3tables.MaintenanceConfiguration) []string {
	if len(maintenance) == 0 {
		return ops
	}

	filtered := make([]string, 0, len(ops))
	for _, op := range ops {
		value, ok := maintenance[maintenanceJobType(op)]
		if ok && value != nil && value.Status == s3tables.MaintenanceStatusDisabled {
			continue
		}
		filtered = append(filtered, op)
	}
	return filtered
}

// loadMaintenanceConfiguration reads the bucket and table maintenance
// configuration and overlays the table's on the bucket's. Detection gets both
// from entries it already listed; execution has to look them up.
func loadMaintenanceConfiguration(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, tablePath string) (s3tables.MaintenanceConfiguration, error) {
	bucketDir := path.Join(s3tables.TablesPath, bucketName)
	tableDir := path.Join(bucketDir, tablePath)

	bucket, err := lookupMaintenanceConfiguration(ctx, client, bucketDir, bucketName)
	if err != nil {
		return nil, err
	}
	table, err := lookupMaintenanceConfiguration(ctx, client, tableDir, path.Join(bucketName, tablePath))
	if err != nil {
		return nil, err
	}
	return mergeMaintenanceConfiguration(bucket, table), nil
}

func lookupMaintenanceConfiguration(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, resource string) (s3tables.MaintenanceConfiguration, error) {
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: path.Dir(dir),
		Name:      path.Base(dir),
	})
	if err != nil {
		return nil, fmt.Errorf("look up %s: %w", resource, err)
	}
	if resp == nil || resp.Entry == nil {
		return nil, nil
	}
	return parseMaintenanceConfiguration(resp.Entry.Extended, resource)
}

// parseMaintenanceConfiguration reads the maintenance configuration stored on a
// filer entry. An unreadable attribute is an error rather than an empty
// configuration: treating it as empty would run operations the operator
// disabled.
func parseMaintenanceConfiguration(extended map[string][]byte, resource string) (s3tables.MaintenanceConfiguration, error) {
	data, ok := extended[s3tables.ExtendedKeyMaintenance]
	if !ok || len(data) == 0 {
		return nil, nil
	}

	var config s3tables.MaintenanceConfiguration
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parse maintenance configuration on %s: %w", resource, err)
	}
	return config, nil
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
