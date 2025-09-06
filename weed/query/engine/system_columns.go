package engine

import (
	"strings"
)

// System column constants used throughout the SQL engine
const (
	SW_COLUMN_NAME_TIMESTAMP = "_timestamp_ns" // Message timestamp in nanoseconds
	SW_COLUMN_NAME_KEY       = "_key"          // Message key
	SW_COLUMN_NAME_SOURCE    = "_source"       // Data source (live_log, parquet_archive, etc.)
)

// isSystemColumn checks if a column is a system column (_timestamp_ns, _key, _source)
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

// getSystemColumnGlobalMin computes global min for system columns using file metadata
func (e *SQLEngine) getSystemColumnGlobalMin(columnName string, allFileStats map[string][]*ParquetFileStats) interface{} {
	lowerName := strings.ToLower(columnName)

	switch lowerName {
	case SW_COLUMN_NAME_TIMESTAMP:
		// For timestamps, find the earliest timestamp across all files
		// This should match what's in the Extended["min"] metadata
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
		// This should match what's in the Extended["max"] metadata
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
