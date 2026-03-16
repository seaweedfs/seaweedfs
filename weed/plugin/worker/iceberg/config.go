package iceberg

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const (
	jobType = "iceberg_maintenance"

	defaultSnapshotRetentionHours = 168 // 7 days
	defaultMaxSnapshotsToKeep     = 5
	defaultOrphanOlderThanHours   = 72
	defaultMaxCommitRetries       = 5
	defaultTargetFileSizeMB       = 256
	defaultMinInputFiles          = 5
	defaultDeleteTargetFileSizeMB = 64
	defaultDeleteMinInputFiles    = 2
	defaultDeleteMaxGroupSizeMB   = 256
	defaultDeleteMaxOutputFiles   = 8
	defaultRewriteStrategy        = "binpack"
	defaultMinManifestsToRewrite  = 5
	minManifestsToRewrite         = 2
	defaultOperations             = "all"

	// Metric keys returned by maintenance operations.
	MetricFilesMerged          = "files_merged"
	MetricFilesWritten         = "files_written"
	MetricBins                 = "bins"
	MetricSnapshotsExpired     = "snapshots_expired"
	MetricFilesDeleted         = "files_deleted"
	MetricOrphansRemoved       = "orphans_removed"
	MetricManifestsRewritten   = "manifests_rewritten"
	MetricDeleteFilesRewritten = "delete_files_rewritten"
	MetricDeleteFilesWritten   = "delete_files_written"
	MetricDeleteBytesRewritten = "delete_bytes_rewritten"
	MetricDeleteGroupsPlanned  = "delete_groups_planned"
	MetricDeleteGroupsSkipped  = "delete_groups_skipped"
	MetricEntriesTotal         = "entries_total"
	MetricDurationMs           = "duration_ms"
)

// Config holds parsed worker config values.
type Config struct {
	SnapshotRetentionHours      int64
	MaxSnapshotsToKeep          int64
	OrphanOlderThanHours        int64
	MaxCommitRetries            int64
	TargetFileSizeBytes         int64
	MinInputFiles               int64
	DeleteTargetFileSizeBytes   int64
	DeleteMinInputFiles         int64
	DeleteMaxFileGroupSizeBytes int64
	DeleteMaxOutputFiles        int64
	MinManifestsToRewrite       int64
	Operations                  string
	ApplyDeletes                bool
	Where                       string
	RewriteStrategy             string
	SortMaxInputBytes           int64
}

// ParseConfig extracts an iceberg maintenance Config from plugin config values.
// Values are clamped to safe minimums to prevent misconfiguration.
func ParseConfig(values map[string]*plugin_pb.ConfigValue) Config {
	cfg := Config{
		SnapshotRetentionHours:      readInt64Config(values, "snapshot_retention_hours", defaultSnapshotRetentionHours),
		MaxSnapshotsToKeep:          readInt64Config(values, "max_snapshots_to_keep", defaultMaxSnapshotsToKeep),
		OrphanOlderThanHours:        readInt64Config(values, "orphan_older_than_hours", defaultOrphanOlderThanHours),
		MaxCommitRetries:            readInt64Config(values, "max_commit_retries", defaultMaxCommitRetries),
		TargetFileSizeBytes:         readInt64Config(values, "target_file_size_mb", defaultTargetFileSizeMB) * 1024 * 1024,
		MinInputFiles:               readInt64Config(values, "min_input_files", defaultMinInputFiles),
		DeleteTargetFileSizeBytes:   readInt64Config(values, "delete_target_file_size_mb", defaultDeleteTargetFileSizeMB) * 1024 * 1024,
		DeleteMinInputFiles:         readInt64Config(values, "delete_min_input_files", defaultDeleteMinInputFiles),
		DeleteMaxFileGroupSizeBytes: readInt64Config(values, "delete_max_file_group_size_mb", defaultDeleteMaxGroupSizeMB) * 1024 * 1024,
		DeleteMaxOutputFiles:        readInt64Config(values, "delete_max_output_files", defaultDeleteMaxOutputFiles),
		MinManifestsToRewrite:       readInt64Config(values, "min_manifests_to_rewrite", defaultMinManifestsToRewrite),
		Operations:                  readStringConfig(values, "operations", defaultOperations),
		ApplyDeletes:                readBoolConfig(values, "apply_deletes", true),
		Where:                       strings.TrimSpace(readStringConfig(values, "where", "")),
		RewriteStrategy:             strings.TrimSpace(strings.ToLower(readStringConfig(values, "rewrite_strategy", defaultRewriteStrategy))),
		SortMaxInputBytes:           readInt64Config(values, "sort_max_input_mb", 0) * 1024 * 1024,
	}

	// Clamp the fields that are always defaulted by worker config parsing.
	if cfg.SnapshotRetentionHours <= 0 {
		cfg.SnapshotRetentionHours = defaultSnapshotRetentionHours
	}
	if cfg.MaxSnapshotsToKeep <= 0 {
		cfg.MaxSnapshotsToKeep = defaultMaxSnapshotsToKeep
	}
	if cfg.MaxCommitRetries <= 0 {
		cfg.MaxCommitRetries = defaultMaxCommitRetries
	}
	cfg = applyThresholdDefaults(cfg)
	return cfg
}

func applyThresholdDefaults(cfg Config) Config {
	if cfg.OrphanOlderThanHours <= 0 {
		cfg.OrphanOlderThanHours = defaultOrphanOlderThanHours
	}
	if cfg.TargetFileSizeBytes <= 0 {
		cfg.TargetFileSizeBytes = defaultTargetFileSizeMB * 1024 * 1024
	}
	if cfg.MinInputFiles < 2 {
		cfg.MinInputFiles = defaultMinInputFiles
	}
	if cfg.DeleteTargetFileSizeBytes <= 0 {
		cfg.DeleteTargetFileSizeBytes = defaultDeleteTargetFileSizeMB * 1024 * 1024
	}
	if cfg.DeleteMinInputFiles < 2 {
		cfg.DeleteMinInputFiles = defaultDeleteMinInputFiles
	}
	if cfg.DeleteMaxFileGroupSizeBytes <= 0 {
		cfg.DeleteMaxFileGroupSizeBytes = defaultDeleteMaxGroupSizeMB * 1024 * 1024
	}
	if cfg.DeleteMaxOutputFiles <= 0 {
		cfg.DeleteMaxOutputFiles = defaultDeleteMaxOutputFiles
	}
	if cfg.RewriteStrategy == "" {
		cfg.RewriteStrategy = defaultRewriteStrategy
	}
	if cfg.RewriteStrategy != "binpack" && cfg.RewriteStrategy != "sort" {
		cfg.RewriteStrategy = defaultRewriteStrategy
	}
	if cfg.SortMaxInputBytes < 0 {
		cfg.SortMaxInputBytes = 0
	}
	if cfg.MinManifestsToRewrite < minManifestsToRewrite {
		cfg.MinManifestsToRewrite = minManifestsToRewrite
	}
	return cfg
}

// parseOperations returns the ordered list of maintenance operations to execute.
// Order follows Iceberg best practices: compact → rewrite_position_delete_files
// → expire_snapshots → remove_orphans → rewrite_manifests.
// Returns an error if any unknown operation is specified or the result would be empty.
func parseOperations(ops string) ([]string, error) {
	ops = strings.TrimSpace(strings.ToLower(ops))
	if ops == "" || ops == "all" {
		return []string{"compact", "rewrite_position_delete_files", "expire_snapshots", "remove_orphans", "rewrite_manifests"}, nil
	}

	validOps := map[string]struct{}{
		"compact":                       {},
		"rewrite_position_delete_files": {},
		"expire_snapshots":              {},
		"remove_orphans":                {},
		"rewrite_manifests":             {},
	}

	requested := make(map[string]struct{})
	for _, op := range strings.Split(ops, ",") {
		op = strings.TrimSpace(op)
		if op == "" {
			continue
		}
		if _, ok := validOps[op]; !ok {
			return nil, fmt.Errorf("unknown maintenance operation %q (valid: compact, rewrite_position_delete_files, expire_snapshots, remove_orphans, rewrite_manifests)", op)
		}
		requested[op] = struct{}{}
	}

	// Return in canonical order: compact → rewrite_position_delete_files →
	// expire_snapshots → remove_orphans → rewrite_manifests
	canonicalOrder := []string{"compact", "rewrite_position_delete_files", "expire_snapshots", "remove_orphans", "rewrite_manifests"}
	var result []string
	for _, op := range canonicalOrder {
		if _, ok := requested[op]; ok {
			result = append(result, op)
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no valid maintenance operations specified")
	}
	return result, nil
}

func extractMetadataVersion(metadataFileName string) int {
	// Parse "v3.metadata.json" or "v3-{nonce}.metadata.json" → 3
	name := strings.TrimPrefix(metadataFileName, "v")
	name = strings.TrimSuffix(name, ".metadata.json")
	// Strip any nonce suffix (e.g. "3-1709766000" → "3")
	if dashIdx := strings.Index(name, "-"); dashIdx > 0 {
		name = name[:dashIdx]
	}
	version, _ := strconv.Atoi(name)
	return version
}

// readStringConfig reads a string value from plugin config, with fallback.
func readStringConfig(values map[string]*plugin_pb.ConfigValue, field string, fallback string) string {
	if values == nil {
		return fallback
	}
	value := values[field]
	if value == nil {
		return fallback
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_StringValue:
		return kind.StringValue
	case *plugin_pb.ConfigValue_Int64Value:
		return strconv.FormatInt(kind.Int64Value, 10)
	case *plugin_pb.ConfigValue_DoubleValue:
		return strconv.FormatFloat(kind.DoubleValue, 'f', -1, 64)
	case *plugin_pb.ConfigValue_BoolValue:
		return strconv.FormatBool(kind.BoolValue)
	default:
		glog.V(1).Infof("readStringConfig: unexpected config value type %T for field %q, using fallback", value.Kind, field)
	}
	return fallback
}

// readBoolConfig reads a bool value from plugin config, with fallback.
func readBoolConfig(values map[string]*plugin_pb.ConfigValue, field string, fallback bool) bool {
	if values == nil {
		return fallback
	}
	value := values[field]
	if value == nil {
		return fallback
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_BoolValue:
		return kind.BoolValue
	case *plugin_pb.ConfigValue_StringValue:
		s := strings.TrimSpace(strings.ToLower(kind.StringValue))
		if s == "true" || s == "1" || s == "yes" {
			return true
		}
		if s == "false" || s == "0" || s == "no" {
			return false
		}
		glog.V(1).Infof("readBoolConfig: unrecognized string value %q for field %q, using fallback %v", kind.StringValue, field, fallback)
	case *plugin_pb.ConfigValue_Int64Value:
		return kind.Int64Value != 0
	default:
		glog.V(1).Infof("readBoolConfig: unexpected config value type %T for field %q, using fallback", value.Kind, field)
	}
	return fallback
}

// readInt64Config reads an int64 value from plugin config, with fallback.
func readInt64Config(values map[string]*plugin_pb.ConfigValue, field string, fallback int64) int64 {
	if values == nil {
		return fallback
	}
	value := values[field]
	if value == nil {
		return fallback
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_Int64Value:
		return kind.Int64Value
	case *plugin_pb.ConfigValue_DoubleValue:
		return int64(kind.DoubleValue)
	case *plugin_pb.ConfigValue_StringValue:
		parsed, err := strconv.ParseInt(strings.TrimSpace(kind.StringValue), 10, 64)
		if err == nil {
			return parsed
		}
	case *plugin_pb.ConfigValue_BoolValue:
		if kind.BoolValue {
			return 1
		}
		return 0
	default:
		glog.V(1).Infof("readInt64Config: unexpected config value type %T for field %q, using fallback", value.Kind, field)
	}
	return fallback
}
