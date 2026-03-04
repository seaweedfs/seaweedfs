package iceberg

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const (
	jobType = "iceberg_maintenance"

	defaultSnapshotRetentionHours = 168 // 7 days
	defaultMaxSnapshotsToKeep     = 5
	defaultOrphanOlderThanHours   = 72
	defaultMaxCommitRetries       = 5
	defaultTargetFileSizeBytes    = 256 * 1024 * 1024
	defaultMinInputFiles          = 5
	defaultOperations             = "all"
)

// Config holds parsed worker config values.
type Config struct {
	SnapshotRetentionHours int64
	MaxSnapshotsToKeep     int64
	OrphanOlderThanHours   int64
	MaxCommitRetries       int64
	TargetFileSizeBytes    int64
	MinInputFiles          int64
	Operations             string
}

// ParseConfig extracts an iceberg maintenance Config from plugin config values.
func ParseConfig(values map[string]*plugin_pb.ConfigValue) Config {
	return Config{
		SnapshotRetentionHours: readInt64Config(values, "snapshot_retention_hours", defaultSnapshotRetentionHours),
		MaxSnapshotsToKeep:     readInt64Config(values, "max_snapshots_to_keep", defaultMaxSnapshotsToKeep),
		OrphanOlderThanHours:   readInt64Config(values, "orphan_older_than_hours", defaultOrphanOlderThanHours),
		MaxCommitRetries:       readInt64Config(values, "max_commit_retries", defaultMaxCommitRetries),
		TargetFileSizeBytes:    readInt64Config(values, "target_file_size_bytes", defaultTargetFileSizeBytes),
		MinInputFiles:          readInt64Config(values, "min_input_files", defaultMinInputFiles),
		Operations:             readStringConfig(values, "operations", defaultOperations),
	}
}

// parseOperations returns the ordered list of maintenance operations to execute.
// Order follows Iceberg best practices: expire_snapshots → remove_orphans → rewrite_manifests.
// Returns an error if any unknown operation is specified or the result would be empty.
func parseOperations(ops string) ([]string, error) {
	ops = strings.TrimSpace(strings.ToLower(ops))
	if ops == "" || ops == "all" {
		return []string{"compact", "expire_snapshots", "remove_orphans", "rewrite_manifests"}, nil
	}

	validOps := map[string]struct{}{
		"compact":           {},
		"expire_snapshots":  {},
		"remove_orphans":    {},
		"rewrite_manifests": {},
	}

	requested := make(map[string]struct{})
	for _, op := range strings.Split(ops, ",") {
		op = strings.TrimSpace(op)
		if op == "" {
			continue
		}
		if _, ok := validOps[op]; !ok {
			return nil, fmt.Errorf("unknown maintenance operation %q (valid: compact, expire_snapshots, remove_orphans, rewrite_manifests)", op)
		}
		requested[op] = struct{}{}
	}

	// Return in canonical order: compact → expire_snapshots → remove_orphans → rewrite_manifests
	canonicalOrder := []string{"compact", "expire_snapshots", "remove_orphans", "rewrite_manifests"}
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
	// Parse "v3.metadata.json" → 3
	name := strings.TrimPrefix(metadataFileName, "v")
	name = strings.TrimSuffix(name, ".metadata.json")
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
	}
	return fallback
}
