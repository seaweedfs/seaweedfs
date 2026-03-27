package lifecycle

import (
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

const (
	jobType = "s3_lifecycle"

	defaultBatchSize           = 1000
	defaultMaxDeletesPerBucket = 10000
	defaultDryRun              = false
	defaultDeleteMarkerCleanup = true
	defaultAbortMPUDaysDefault = 7

	MetricObjectsExpired     = "objects_expired"
	MetricObjectsScanned     = "objects_scanned"
	MetricBucketsScanned     = "buckets_scanned"
	MetricBucketsWithRules   = "buckets_with_rules"
	MetricDeleteMarkersClean = "delete_markers_cleaned"
	MetricMPUAborted         = "mpu_aborted"
	MetricErrors             = "errors"
	MetricDurationMs         = "duration_ms"
)

// Config holds parsed worker config values for lifecycle management.
type Config struct {
	BatchSize           int64
	MaxDeletesPerBucket int64
	DryRun              bool
	DeleteMarkerCleanup bool
	AbortMPUDays        int64
}

// ParseConfig extracts a lifecycle Config from plugin config values.
func ParseConfig(values map[string]*plugin_pb.ConfigValue) Config {
	cfg := Config{
		BatchSize:           readInt64Config(values, "batch_size", defaultBatchSize),
		MaxDeletesPerBucket: readInt64Config(values, "max_deletes_per_bucket", defaultMaxDeletesPerBucket),
		DryRun:              readBoolConfig(values, "dry_run", defaultDryRun),
		DeleteMarkerCleanup: readBoolConfig(values, "delete_marker_cleanup", defaultDeleteMarkerCleanup),
		AbortMPUDays:        readInt64Config(values, "abort_mpu_days", defaultAbortMPUDaysDefault),
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.MaxDeletesPerBucket <= 0 {
		cfg.MaxDeletesPerBucket = defaultMaxDeletesPerBucket
	}
	if cfg.AbortMPUDays < 0 {
		cfg.AbortMPUDays = defaultAbortMPUDaysDefault
	}

	return cfg
}

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
	default:
		glog.V(1).Infof("readStringConfig: unexpected type %T for field %q", value.Kind, field)
	}
	return fallback
}

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
		glog.V(1).Infof("readBoolConfig: unexpected config value type %T for field %q, using fallback %v", value.Kind, field, fallback)
	}
	return fallback
}

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
	default:
		glog.V(1).Infof("readInt64Config: unexpected config value type %T for field %q, using fallback %d", value.Kind, field, fallback)
	}
	return fallback
}
