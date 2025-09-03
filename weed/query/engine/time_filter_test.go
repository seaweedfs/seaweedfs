package engine

import (
	"context"
	"testing"
)

// TestTimeFilterExtraction tests the extraction of time filters from WHERE clauses
func TestTimeFilterExtraction(t *testing.T) {
	_ = NewTestSQLEngine()

	// Test data: use fixed timestamps for consistent testing

	_ = []struct {
		name            string
		whereClause     string
		expectedStartNs int64
		expectedStopNs  int64
		description     string
	}{
		{
			name:            "Greater Than Filter",
			whereClause:     "_timestamp_ns > 1672531200000000000", // Fixed timestamp
			expectedStartNs: 1672531200000000000,
			expectedStopNs:  0, // No upper bound
			description:     "Should extract start time from > comparison",
		},
		{
			name:            "Less Than Filter",
			whereClause:     "_timestamp_ns < 1672617600000000000", // Fixed timestamp
			expectedStartNs: 0,                                     // No lower bound
			expectedStopNs:  1672617600000000000,
			description:     "Should extract stop time from < comparison",
		},
		{
			name:            "Range Filter (AND)",
			whereClause:     "_timestamp_ns >= 1672531200000000000 AND _timestamp_ns <= 1672617600000000000",
			expectedStartNs: 1672531200000000000,
			expectedStopNs:  1672617600000000000,
			description:     "Should extract both bounds from range query",
		},
		{
			name:            "Equal Filter",
			whereClause:     "_timestamp_ns = 1672531200000000000",
			expectedStartNs: 1672531200000000000,
			expectedStopNs:  1672531200000000000,
			description:     "Should set both bounds for exact match",
		},
		{
			name:            "Non-Time Filter",
			whereClause:     "user_id > 1000",
			expectedStartNs: 0,
			expectedStopNs:  0,
			description:     "Should ignore non-time comparisons",
		},
		{
			name:            "OR Filter (Skip)",
			whereClause:     "_timestamp_ns > 1672531200000000000 OR user_id = 123",
			expectedStartNs: 0,
			expectedStopNs:  0,
			description:     "Should skip time extraction for OR clauses (unsafe)",
		},
	}

	// TODO: Rewrite this test to work with the PostgreSQL parser instead of sqlparser
	// The test has been temporarily disabled while migrating from sqlparser to native PostgreSQL parser
	t.Skip("Test disabled during sqlparser removal - needs rewrite for PostgreSQL parser")
}

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

// TestTimeValueParsing tests parsing of different time value formats
func TestTimeValueParsing(t *testing.T) {
	_ = NewTestSQLEngine()

	// TODO: Rewrite this test to work without sqlparser types
	// The test has been temporarily disabled while migrating from sqlparser to native PostgreSQL parser
	t.Skip("Test disabled during sqlparser removal - needs rewrite for PostgreSQL parser")
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
