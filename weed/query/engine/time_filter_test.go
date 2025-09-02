package engine

import (
	"context"
	"testing"

	"github.com/xwb1989/sqlparser"
)

// TestTimeFilterExtraction tests the extraction of time filters from WHERE clauses
func TestTimeFilterExtraction(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

	// Test data: use fixed timestamps for consistent testing

	testCases := []struct {
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the WHERE clause
			sql := "SELECT * FROM test_table WHERE " + tc.whereClause
			stmt, err := sqlparser.Parse(sql)
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			selectStmt, ok := stmt.(*sqlparser.Select)
			if !ok {
				t.Fatal("Expected SELECT statement")
			}

			if selectStmt.Where == nil {
				t.Fatal("WHERE clause not found")
			}

			// Extract time filters
			startNs, stopNs := engine.extractTimeFilters(selectStmt.Where.Expr)

			// Verify results
			if startNs != tc.expectedStartNs {
				t.Errorf("Start time mismatch. Expected: %d, Got: %d", tc.expectedStartNs, startNs)
			}

			if stopNs != tc.expectedStopNs {
				t.Errorf("Stop time mismatch. Expected: %d, Got: %d", tc.expectedStopNs, stopNs)
			}

			t.Logf("%s: StartNs=%d, StopNs=%d", tc.description, startNs, stopNs)
		})
	}
}

// TestTimeColumnRecognition tests the recognition of time-related columns
func TestTimeColumnRecognition(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

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
	engine := NewSQLEngine("localhost:8888")

	testCases := []struct {
		name        string
		value       string
		sqlType     sqlparser.ValType
		expected    bool // Whether parsing should succeed
		description string
	}{
		{
			name:        "Nanosecond Timestamp",
			value:       "1672531200000000000", // 2023-01-01 00:00:00 UTC in nanoseconds
			sqlType:     sqlparser.IntVal,
			expected:    true,
			description: "Should parse nanosecond timestamp",
		},
		{
			name:        "RFC3339 Date",
			value:       "2023-01-01T00:00:00Z",
			sqlType:     sqlparser.StrVal,
			expected:    true,
			description: "Should parse ISO 8601 date",
		},
		{
			name:        "Date Only",
			value:       "2023-01-01",
			sqlType:     sqlparser.StrVal,
			expected:    true,
			description: "Should parse date-only format",
		},
		{
			name:        "DateTime Format",
			value:       "2023-01-01 00:00:00",
			sqlType:     sqlparser.StrVal,
			expected:    true,
			description: "Should parse datetime format",
		},
		{
			name:        "Invalid Format",
			value:       "not-a-date",
			sqlType:     sqlparser.StrVal,
			expected:    false,
			description: "Should fail on invalid date format",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a SQLVal expression
			sqlVal := &sqlparser.SQLVal{
				Type: tc.sqlType,
				Val:  []byte(tc.value),
			}

			// Extract time value
			timeNs := engine.extractTimeValue(sqlVal)

			if tc.expected {
				if timeNs == 0 {
					t.Errorf("Expected successful parsing for %s, but got 0", tc.value)
				} else {
					t.Logf("%s: Parsed to %d nanoseconds", tc.description, timeNs)
				}
			} else {
				if timeNs != 0 {
					t.Errorf("Expected parsing to fail for %s, but got %d", tc.value, timeNs)
				} else {
					t.Logf("%s: Correctly failed to parse", tc.description)
				}
			}
		})
	}
}

// TestTimeFilterIntegration tests the full integration of time filters with SELECT queries
func TestTimeFilterIntegration(t *testing.T) {
	engine := NewSQLEngine("localhost:8888")

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
