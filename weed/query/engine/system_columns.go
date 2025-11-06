package engine

import (
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
)

// System column constants used throughout the SQL engine
const (
	SW_COLUMN_NAME_TIMESTAMP = "_ts_ns"  // Message timestamp in nanoseconds (internal)
	SW_COLUMN_NAME_KEY       = "_key"    // Message key
	SW_COLUMN_NAME_SOURCE    = "_source" // Data source (live_log, parquet_archive, etc.)
	SW_COLUMN_NAME_VALUE     = "_value"  // Raw message value (for schema-less topics)
)

// System column display names (what users see)
const (
	SW_DISPLAY_NAME_TIMESTAMP = "_ts" // User-facing timestamp column name
	// Note: _key and _source keep the same names, only _ts_ns changes to _ts
)

// isSystemColumn checks if a column is a system column (_ts_ns, _key, _source)
func (e *SQLEngine) isSystemColumn(columnName string) bool {
	lowerName := strings.ToLower(columnName)
	return lowerName == SW_COLUMN_NAME_TIMESTAMP ||
		lowerName == SW_COLUMN_NAME_KEY ||
		lowerName == SW_COLUMN_NAME_SOURCE
}

// isRegularColumn checks if a column might be a regular data column (placeholder)
func (e *SQLEngine) isRegularColumn(columnName string) bool {
	// For now, assume any non-system column is a regular column
	return !e.isSystemColumn(columnName)
}

// getSystemColumnDisplayName returns the user-facing display name for system columns
func (e *SQLEngine) getSystemColumnDisplayName(columnName string) string {
	lowerName := strings.ToLower(columnName)
	switch lowerName {
	case SW_COLUMN_NAME_TIMESTAMP:
		return SW_DISPLAY_NAME_TIMESTAMP
	case SW_COLUMN_NAME_KEY:
		return SW_COLUMN_NAME_KEY // _key stays the same
	case SW_COLUMN_NAME_SOURCE:
		return SW_COLUMN_NAME_SOURCE // _source stays the same
	default:
		return columnName // Return original name for non-system columns
	}
}

// isSystemColumnDisplayName checks if a column name is a system column display name
func (e *SQLEngine) isSystemColumnDisplayName(columnName string) bool {
	lowerName := strings.ToLower(columnName)
	return lowerName == SW_DISPLAY_NAME_TIMESTAMP ||
		lowerName == SW_COLUMN_NAME_KEY ||
		lowerName == SW_COLUMN_NAME_SOURCE
}

// getSystemColumnInternalName returns the internal name for a system column display name
func (e *SQLEngine) getSystemColumnInternalName(displayName string) string {
	lowerName := strings.ToLower(displayName)
	switch lowerName {
	case SW_DISPLAY_NAME_TIMESTAMP:
		return SW_COLUMN_NAME_TIMESTAMP
	case SW_COLUMN_NAME_KEY:
		return SW_COLUMN_NAME_KEY
	case SW_COLUMN_NAME_SOURCE:
		return SW_COLUMN_NAME_SOURCE
	default:
		return displayName // Return original name for non-system columns
	}
}

// formatTimestampColumn formats a nanosecond timestamp as a proper timestamp value
func (e *SQLEngine) formatTimestampColumn(timestampNs int64) sqltypes.Value {
	// Convert nanoseconds to time.Time
	timestamp := time.Unix(timestampNs/1e9, timestampNs%1e9)

	// Format as timestamp string in MySQL datetime format
	timestampStr := timestamp.UTC().Format("2006-01-02 15:04:05")

	// Return as a timestamp value using the Timestamp type
	return sqltypes.MakeTrusted(sqltypes.Timestamp, []byte(timestampStr))
}

// getSystemColumnGlobalMin computes global min for system columns using file metadata
func (e *SQLEngine) getSystemColumnGlobalMin(columnName string, allFileStats map[string][]*ParquetFileStats) interface{} {
	lowerName := strings.ToLower(columnName)

	switch lowerName {
	case SW_COLUMN_NAME_TIMESTAMP:
		// For timestamps, find the earliest timestamp across all files
		// This should match what's in the Extended[mq.ExtendedAttrTimestampMin] metadata
		var minTimestamp *int64
		for _, fileStats := range allFileStats {
			for _, fileStat := range fileStats {
				// Extract timestamp from filename (format: YYYY-MM-DD-HH-MM-SS.parquet)
				timestamp := e.extractTimestampFromFilename(fileStat.FileName)
				if timestamp != 0 {
					if minTimestamp == nil || timestamp < *minTimestamp {
						minTimestamp = &timestamp
					}
				}
			}
		}
		if minTimestamp != nil {
			return *minTimestamp
		}

	case SW_COLUMN_NAME_KEY:
		// For keys, we'd need to read the actual parquet column stats
		// Fall back to scanning if not available in our current stats
		return nil

	case SW_COLUMN_NAME_SOURCE:
		// Source is always "parquet_archive" for parquet files
		return "parquet_archive"
	}

	return nil
}

// getSystemColumnGlobalMax computes global max for system columns using file metadata
func (e *SQLEngine) getSystemColumnGlobalMax(columnName string, allFileStats map[string][]*ParquetFileStats) interface{} {
	lowerName := strings.ToLower(columnName)

	switch lowerName {
	case SW_COLUMN_NAME_TIMESTAMP:
		// For timestamps, find the latest timestamp across all files
		// This should match what's in the Extended[mq.ExtendedAttrTimestampMax] metadata
		var maxTimestamp *int64
		for _, fileStats := range allFileStats {
			for _, fileStat := range fileStats {
				// Extract timestamp from filename (format: YYYY-MM-DD-HH-MM-SS.parquet)
				timestamp := e.extractTimestampFromFilename(fileStat.FileName)
				if timestamp != 0 {
					if maxTimestamp == nil || timestamp > *maxTimestamp {
						maxTimestamp = &timestamp
					}
				}
			}
		}
		if maxTimestamp != nil {
			return *maxTimestamp
		}

	case SW_COLUMN_NAME_KEY:
		// For keys, we'd need to read the actual parquet column stats
		// Fall back to scanning if not available in our current stats
		return nil

	case SW_COLUMN_NAME_SOURCE:
		// Source is always "parquet_archive" for parquet files
		return "parquet_archive"
	}

	return nil
}
