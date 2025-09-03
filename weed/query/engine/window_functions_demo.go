package engine

import (
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// WindowFunctionDemo demonstrates basic window function concepts for timestamp-based analytics
// This provides a foundation for full window function implementation

// ApplyRowNumber applies ROW_NUMBER() OVER (ORDER BY timestamp) to a result set
func (e *SQLEngine) ApplyRowNumber(results []HybridScanResult, orderByColumn string) []HybridScanResult {
	// Sort results by timestamp if ordering by timestamp-related fields
	if orderByColumn == "timestamp" || orderByColumn == "_timestamp_ns" {
		sort.Slice(results, func(i, j int) bool {
			return results[i].Timestamp < results[j].Timestamp
		})
	}

	// Add ROW_NUMBER as a synthetic column
	for i := range results {
		if results[i].Values == nil {
			results[i].Values = make(map[string]*schema_pb.Value)
		}
		results[i].Values["row_number"] = &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: int64(i + 1)},
		}
	}

	return results
}

// ExtractYear extracts the year from a TIMESTAMP logical type
func (e *SQLEngine) ExtractYear(timestampValue *schema_pb.TimestampValue) int {
	if timestampValue == nil {
		return 0
	}
	
	// Convert microseconds to seconds and create time
	t := time.Unix(timestampValue.TimestampMicros/1_000_000, 0)
	return t.Year()
}

// ExtractMonth extracts the month from a TIMESTAMP logical type
func (e *SQLEngine) ExtractMonth(timestampValue *schema_pb.TimestampValue) int {
	if timestampValue == nil {
		return 0
	}
	
	t := time.Unix(timestampValue.TimestampMicros/1_000_000, 0)
	return int(t.Month())
}

// ExtractDay extracts the day from a TIMESTAMP logical type
func (e *SQLEngine) ExtractDay(timestampValue *schema_pb.TimestampValue) int {
	if timestampValue == nil {
		return 0
	}
	
	t := time.Unix(timestampValue.TimestampMicros/1_000_000, 0)
	return t.Day()
}

// FilterByYear demonstrates filtering TIMESTAMP values by year
func (e *SQLEngine) FilterByYear(results []HybridScanResult, targetYear int) []HybridScanResult {
	var filtered []HybridScanResult
	
	for _, result := range results {
		if timestampField := result.Values["timestamp"]; timestampField != nil {
			if timestampVal, ok := timestampField.Kind.(*schema_pb.Value_TimestampValue); ok {
				year := e.ExtractYear(timestampVal.TimestampValue)
				if year == targetYear {
					filtered = append(filtered, result)
				}
			}
		}
	}
	
	return filtered
}

// This demonstrates the foundation for more complex window functions like:
// - LAG(value, offset) OVER (ORDER BY timestamp) - Access previous row value
// - LEAD(value, offset) OVER (ORDER BY timestamp) - Access next row value  
// - RANK() OVER (ORDER BY timestamp) - Ranking with gaps for ties
// - DENSE_RANK() OVER (ORDER BY timestamp) - Ranking without gaps
// - FIRST_VALUE(value) OVER (PARTITION BY category ORDER BY timestamp) - First value in window
// - LAST_VALUE(value) OVER (PARTITION BY category ORDER BY timestamp) - Last value in window
