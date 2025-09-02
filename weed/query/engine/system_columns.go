package engine

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// isSystemColumn checks if a column is a system column (_timestamp_ns, _key, _source)
func (e *SQLEngine) isSystemColumn(columnName string) bool {
	lowerName := strings.ToLower(columnName)
	return lowerName == "_timestamp_ns" || lowerName == "timestamp_ns" ||
		lowerName == "_key" || lowerName == "key" ||
		lowerName == "_source" || lowerName == "source"
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
	case "_timestamp_ns", "timestamp_ns":
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

	case "_key", "key":
		// For keys, we'd need to read the actual parquet column stats
		// Fall back to scanning if not available in our current stats
		return nil

	case "_source", "source":
		// Source is always "parquet_archive" for parquet files
		return "parquet_archive"
	}

	return nil
}

// getSystemColumnGlobalMax computes global max for system columns using file metadata
func (e *SQLEngine) getSystemColumnGlobalMax(columnName string, allFileStats map[string][]*ParquetFileStats) interface{} {
	lowerName := strings.ToLower(columnName)

	switch lowerName {
	case "_timestamp_ns", "timestamp_ns":
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

	case "_key", "key":
		// For keys, we'd need to read the actual parquet column stats
		// Fall back to scanning if not available in our current stats
		return nil

	case "_source", "source":
		// Source is always "parquet_archive" for parquet files
		return "parquet_archive"
	}

	return nil
}

// extractTimestampFromFilename extracts timestamp from parquet filename
func (e *SQLEngine) extractTimestampFromFilename(filename string) int64 {
	// Expected format: YYYY-MM-DD-HH-MM-SS.parquet or similar
	// Try to parse timestamp from filename
	re := regexp.MustCompile(`(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})`)
	matches := re.FindStringSubmatch(filename)
	if len(matches) > 1 {
		timestampStr := matches[1]
		// Convert to time and then to nanoseconds
		t, err := time.Parse("2006-01-02-15-04-05", timestampStr)
		if err == nil {
			return t.UnixNano()
		}
	}

	// Fallback: try to parse as unix timestamp if filename is numeric
	if timestampStr := strings.TrimSuffix(filename, ".parquet"); timestampStr != filename {
		if timestamp, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
			// Assume it's already in nanoseconds
			return timestamp
		}
	}

	return 0
}

// findColumnValue performs case-insensitive lookup of column values
// Now includes support for system columns stored in HybridScanResult
func (e *SQLEngine) findColumnValue(result HybridScanResult, columnName string) *schema_pb.Value {
	lowerName := strings.ToLower(columnName)

	// Check system columns first
	switch lowerName {
	case "_timestamp_ns", "timestamp_ns":
		return &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: result.Timestamp},
		}
	case "_key", "key":
		return &schema_pb.Value{
			Kind: &schema_pb.Value_BytesValue{BytesValue: result.Key},
		}
	case "_source", "source":
		return &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: result.Source},
		}
	}

	// Check regular columns in the record data
	if result.RecordValue != nil {
		recordValue, ok := result.RecordValue.Kind.(*schema_pb.Value_RecordValue)
		if !ok {
			return nil
		}

		if recordValue.RecordValue.Fields != nil {
			// Try exact match first
			if value, exists := recordValue.RecordValue.Fields[columnName]; exists {
				return value
			}

			// Try case-insensitive match
			for fieldName, value := range recordValue.RecordValue.Fields {
				if strings.EqualFold(fieldName, columnName) {
					return value
				}
			}
		}
	}

	return nil
}
