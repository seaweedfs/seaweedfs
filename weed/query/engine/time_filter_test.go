package engine

import (
	"context"
	"testing"
)

// TestTimeColumnRecognition tests the recognition of time-related columns
func TestTimeColumnRecognition(t *testing.T) {
	engine := NewTestSQLEngine()

	timeColumns := []string{
		"_timestamp_ns",
		"timestamp",
		"created_at",
		"updated_at",
		"event_time",
		"log_time",
		"ts",
	}

	nonTimeColumns := []string{
		"user_id",
		"name",
		"data",
		"count",
		"value",
	}

	// Test time columns are recognized
	for _, col := range timeColumns {
		if !engine.isTimeColumn(col) {
			t.Errorf("Time column '%s' not recognized", col)
		}
	}

	// Test non-time columns are not recognized
	for _, col := range nonTimeColumns {
		if engine.isTimeColumn(col) {
			t.Errorf("Non-time column '%s' incorrectly recognized as time", col)
		}
	}

	// Test case insensitive matching
	if !engine.isTimeColumn("TIMESTAMP") || !engine.isTimeColumn("Timestamp") {
		t.Error("Time column matching should be case-insensitive")
	}

	t.Log("Time column recognition working correctly")
}

// TestTimeFilterIntegration tests the full integration of time filters with SELECT queries
func TestTimeFilterIntegration(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test that time filters are properly extracted and used in SELECT queries
	testQueries := []string{
		"SELECT * FROM user_events WHERE _timestamp_ns > 1672531200000000000",
		"SELECT user_id FROM system_logs WHERE created_at >= '2023-01-01T00:00:00Z'",
		"SELECT * FROM user_events WHERE _timestamp_ns >= 1672531200000000000 AND _timestamp_ns <= 1672617600000000000",
	}

	for _, query := range testQueries {
		t.Run(query, func(t *testing.T) {
			// This should not crash and should execute (even if returning sample data)
			result, err := engine.ExecuteSQL(context.Background(), query)
			if err != nil {
				t.Errorf("Time filter integration failed for query '%s': %v", query, err)
			} else {
				t.Logf("Time filter integration successful for query: %s (returned %d rows)",
					query, len(result.Rows))
			}
		})
	}
}
