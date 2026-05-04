package pluginworker

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	workertypes "github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// ReadStringConfig reads a string-valued plugin config field, returning fallback
// when the value is missing or of an unsupported kind.
func ReadStringConfig(values map[string]*plugin_pb.ConfigValue, field string, fallback string) string {
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

// ReadDoubleConfig reads a double-valued plugin config field, returning
// fallback when the value is missing or unparseable.
func ReadDoubleConfig(values map[string]*plugin_pb.ConfigValue, field string, fallback float64) float64 {
	if values == nil {
		return fallback
	}
	value := values[field]
	if value == nil {
		return fallback
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_DoubleValue:
		return kind.DoubleValue
	case *plugin_pb.ConfigValue_Int64Value:
		return float64(kind.Int64Value)
	case *plugin_pb.ConfigValue_StringValue:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(kind.StringValue), 64)
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

// ReadInt64Config reads an int64-valued plugin config field, returning fallback
// when the value is missing or unparseable.
func ReadInt64Config(values map[string]*plugin_pb.ConfigValue, field string, fallback int64) int64 {
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

// ReadInt32Config reads an int32-valued plugin config field, returning fallback
// when the value is missing or out of int32 range. Used for protobuf int32
// fields whose admin/worker config values arrive as int64.
func ReadInt32Config(values map[string]*plugin_pb.ConfigValue, field string, fallback int32) int32 {
	v := ReadInt64Config(values, field, int64(fallback))
	if v < int64(math.MinInt32) || v > int64(math.MaxInt32) {
		return fallback
	}
	return int32(v)
}

// ReadUint32Config reads a uint32-valued plugin config field, returning
// fallback when the value is missing, negative, or exceeds math.MaxUint32.
// Used for protobuf uint32 fields (volume IDs, shard counts, …).
func ReadUint32Config(values map[string]*plugin_pb.ConfigValue, field string, fallback uint32) uint32 {
	v := ReadInt64Config(values, field, int64(fallback))
	if v < 0 || v > int64(math.MaxUint32) {
		return fallback
	}
	return uint32(v)
}

// ReadIntConfig reads an int-valued plugin config field, returning fallback
// when the value is missing or outside the int32 range. The int32 range is
// used as the platform-portable safe range so that the same value parses
// identically on 32-bit and 64-bit builds.
func ReadIntConfig(values map[string]*plugin_pb.ConfigValue, field string, fallback int) int {
	v := ReadInt64Config(values, field, int64(fallback))
	if v < int64(math.MinInt32) || v > int64(math.MaxInt32) {
		return fallback
	}
	return int(v)
}

// ReadBytesConfig reads a bytes-valued plugin config field, returning nil when
// the value is missing or of a different kind.
func ReadBytesConfig(values map[string]*plugin_pb.ConfigValue, field string) []byte {
	if values == nil {
		return nil
	}
	value := values[field]
	if value == nil {
		return nil
	}
	if kind, ok := value.Kind.(*plugin_pb.ConfigValue_BytesValue); ok {
		return kind.BytesValue
	}
	return nil
}

// ReadStringListConfig reads a list-of-strings plugin config field, returning
// nil when the value is missing. Accepts ConfigValue_StringList,
// ConfigValue_ListValue, or a comma-separated ConfigValue_StringValue.
func ReadStringListConfig(values map[string]*plugin_pb.ConfigValue, field string) []string {
	if values == nil {
		return nil
	}
	value := values[field]
	if value == nil {
		return nil
	}

	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_StringList:
		return normalizeStringList(kind.StringList.GetValues())
	case *plugin_pb.ConfigValue_ListValue:
		out := make([]string, 0, len(kind.ListValue.GetValues()))
		for _, item := range kind.ListValue.GetValues() {
			itemText := readStringFromConfigValue(item)
			if itemText != "" {
				out = append(out, itemText)
			}
		}
		return normalizeStringList(out)
	case *plugin_pb.ConfigValue_StringValue:
		return normalizeStringList(strings.Split(kind.StringValue, ","))
	}

	return nil
}

func readStringFromConfigValue(value *plugin_pb.ConfigValue) string {
	if value == nil {
		return ""
	}
	switch kind := value.Kind.(type) {
	case *plugin_pb.ConfigValue_StringValue:
		return strings.TrimSpace(kind.StringValue)
	case *plugin_pb.ConfigValue_Int64Value:
		return fmt.Sprintf("%d", kind.Int64Value)
	case *plugin_pb.ConfigValue_DoubleValue:
		return fmt.Sprintf("%g", kind.DoubleValue)
	case *plugin_pb.ConfigValue_BoolValue:
		if kind.BoolValue {
			return "true"
		}
		return "false"
	}
	return ""
}

func normalizeStringList(values []string) []string {
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		item := strings.TrimSpace(value)
		if item == "" {
			continue
		}
		if _, found := seen[item]; found {
			continue
		}
		seen[item] = struct{}{}
		normalized = append(normalized, item)
	}
	return normalized
}

// MapTaskPriority converts a worker-task priority into the plugin protocol's
// JobPriority enum.
func MapTaskPriority(priority workertypes.TaskPriority) plugin_pb.JobPriority {
	switch strings.ToLower(string(priority)) {
	case "low":
		return plugin_pb.JobPriority_JOB_PRIORITY_LOW
	case "medium", "normal":
		return plugin_pb.JobPriority_JOB_PRIORITY_NORMAL
	case "high":
		return plugin_pb.JobPriority_JOB_PRIORITY_HIGH
	case "critical":
		return plugin_pb.JobPriority_JOB_PRIORITY_CRITICAL
	default:
		return plugin_pb.JobPriority_JOB_PRIORITY_NORMAL
	}
}
