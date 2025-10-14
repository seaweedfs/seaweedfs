package engine

import (
	"testing"
)

// TestFastPathPredicateValidation tests the critical fix for fast-path aggregation
// to ensure non-time predicates are properly detected and fast-path is blocked
func TestFastPathPredicateValidation(t *testing.T) {
	engine := NewTestSQLEngine()

	testCases := []struct {
		name                string
		whereClause         string
		expectedTimeOnly    bool
		expectedStartTimeNs int64
		expectedStopTimeNs  int64
		description         string
	}{
		{
			name:             "No WHERE clause",
			whereClause:      "",
			expectedTimeOnly: true, // No WHERE means time-only is true
			description:      "Queries without WHERE clause should allow fast path",
		},
		{
			name:                "Time-only predicate (greater than)",
			whereClause:         "_ts > 1640995200000000000",
			expectedTimeOnly:    true,
			expectedStartTimeNs: 1640995200000000000,
			expectedStopTimeNs:  0,
			description:         "Pure time predicates should allow fast path",
		},
		{
			name:                "Time-only predicate (less than)",
			whereClause:         "_ts < 1640995200000000000",
			expectedTimeOnly:    true,
			expectedStartTimeNs: 0,
			expectedStopTimeNs:  1640995200000000000,
			description:         "Pure time predicates should allow fast path",
		},
		{
			name:                "Time-only predicate (range with AND)",
			whereClause:         "_ts > 1640995200000000000 AND _ts < 1641081600000000000",
			expectedTimeOnly:    true,
			expectedStartTimeNs: 1640995200000000000,
			expectedStopTimeNs:  1641081600000000000,
			description:         "Time range predicates should allow fast path",
		},
		{
			name:             "Mixed predicate (time + non-time)",
			whereClause:      "_ts > 1640995200000000000 AND user_id = 'user123'",
			expectedTimeOnly: false,
			description:      "CRITICAL: Mixed predicates must block fast path to prevent incorrect results",
		},
		{
			name:             "Non-time predicate only",
			whereClause:      "user_id = 'user123'",
			expectedTimeOnly: false,
			description:      "Non-time predicates must block fast path",
		},
		{
			name:             "Multiple non-time predicates",
			whereClause:      "user_id = 'user123' AND status = 'active'",
			expectedTimeOnly: false,
			description:      "Multiple non-time predicates must block fast path",
		},
		{
			name:             "OR with time predicate (unsafe)",
			whereClause:      "_ts > 1640995200000000000 OR user_id = 'user123'",
			expectedTimeOnly: false,
			description:      "OR expressions are complex and must block fast path",
		},
		{
			name:             "OR with only time predicates (still unsafe)",
			whereClause:      "_ts > 1640995200000000000 OR _ts < 1640908800000000000",
			expectedTimeOnly: false,
			description:      "Even time-only OR expressions must block fast path due to complexity",
		},
		// Note: Parenthesized expressions are not supported by the current parser
		// These test cases are commented out until parser support is added
		{
			name:             "String column comparison",
			whereClause:      "event_type = 'click'",
			expectedTimeOnly: false,
			description:      "String column comparisons must block fast path",
		},
		{
			name:             "Numeric column comparison",
			whereClause:      "id > 1000",
			expectedTimeOnly: false,
			description:      "Numeric column comparisons must block fast path",
		},
		{
			name:                "Internal timestamp column",
			whereClause:         "_ts_ns > 1640995200000000000",
			expectedTimeOnly:    true,
			expectedStartTimeNs: 1640995200000000000,
			description:         "Internal timestamp column should allow fast path",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the WHERE clause if present
			var whereExpr ExprNode
			if tc.whereClause != "" {
				sql := "SELECT COUNT(*) FROM test WHERE " + tc.whereClause
				stmt, err := ParseSQL(sql)
				if err != nil {
					t.Fatalf("Failed to parse SQL: %v", err)
				}
				selectStmt := stmt.(*SelectStatement)
				whereExpr = selectStmt.Where.Expr
			}

			// Test the validation function
			var startTimeNs, stopTimeNs int64
			var onlyTimePredicates bool

			if whereExpr == nil {
				// No WHERE clause case
				onlyTimePredicates = true
			} else {
				startTimeNs, stopTimeNs, onlyTimePredicates = engine.SQLEngine.extractTimeFiltersWithValidation(whereExpr)
			}

			// Verify the results
			if onlyTimePredicates != tc.expectedTimeOnly {
				t.Errorf("Expected onlyTimePredicates=%v, got %v. %s",
					tc.expectedTimeOnly, onlyTimePredicates, tc.description)
			}

			// Check time filters if expected
			if tc.expectedStartTimeNs != 0 && startTimeNs != tc.expectedStartTimeNs {
				t.Errorf("Expected startTimeNs=%d, got %d", tc.expectedStartTimeNs, startTimeNs)
			}
			if tc.expectedStopTimeNs != 0 && stopTimeNs != tc.expectedStopTimeNs {
				t.Errorf("Expected stopTimeNs=%d, got %d", tc.expectedStopTimeNs, stopTimeNs)
			}

			t.Logf("%s: onlyTimePredicates=%v, startTimeNs=%d, stopTimeNs=%d",
				tc.name, onlyTimePredicates, startTimeNs, stopTimeNs)
		})
	}
}

// TestFastPathAggregationSafety tests that fast-path aggregation is only attempted
// when it's safe to do so (no non-time predicates)
func TestFastPathAggregationSafety(t *testing.T) {
	engine := NewTestSQLEngine()

	testCases := []struct {
		name              string
		sql               string
		shouldUseFastPath bool
		description       string
	}{
		{
			name:              "No WHERE - should use fast path",
			sql:               "SELECT COUNT(*) FROM test",
			shouldUseFastPath: true,
			description:       "Queries without WHERE should use fast path",
		},
		{
			name:              "Time-only WHERE - should use fast path",
			sql:               "SELECT COUNT(*) FROM test WHERE _ts > 1640995200000000000",
			shouldUseFastPath: true,
			description:       "Time-only predicates should use fast path",
		},
		{
			name:              "Mixed WHERE - should NOT use fast path",
			sql:               "SELECT COUNT(*) FROM test WHERE _ts > 1640995200000000000 AND user_id = 'user123'",
			shouldUseFastPath: false,
			description:       "CRITICAL: Mixed predicates must NOT use fast path to prevent wrong results",
		},
		{
			name:              "Non-time WHERE - should NOT use fast path",
			sql:               "SELECT COUNT(*) FROM test WHERE user_id = 'user123'",
			shouldUseFastPath: false,
			description:       "Non-time predicates must NOT use fast path",
		},
		{
			name:              "OR expression - should NOT use fast path",
			sql:               "SELECT COUNT(*) FROM test WHERE _ts > 1640995200000000000 OR user_id = 'user123'",
			shouldUseFastPath: false,
			description:       "OR expressions must NOT use fast path due to complexity",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the SQL
			stmt, err := ParseSQL(tc.sql)
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}
			selectStmt := stmt.(*SelectStatement)

			// Test the fast path decision logic
			startTimeNs, stopTimeNs := int64(0), int64(0)
			onlyTimePredicates := true
			if selectStmt.Where != nil {
				startTimeNs, stopTimeNs, onlyTimePredicates = engine.SQLEngine.extractTimeFiltersWithValidation(selectStmt.Where.Expr)
			}

			canAttemptFastPath := selectStmt.Where == nil || onlyTimePredicates

			// Verify the decision
			if canAttemptFastPath != tc.shouldUseFastPath {
				t.Errorf("Expected canAttemptFastPath=%v, got %v. %s",
					tc.shouldUseFastPath, canAttemptFastPath, tc.description)
			}

			t.Logf("%s: canAttemptFastPath=%v (onlyTimePredicates=%v, startTimeNs=%d, stopTimeNs=%d)",
				tc.name, canAttemptFastPath, onlyTimePredicates, startTimeNs, stopTimeNs)
		})
	}
}

// TestTimestampColumnDetection tests that the engine correctly identifies timestamp columns
func TestTimestampColumnDetection(t *testing.T) {
	engine := NewTestSQLEngine()

	testCases := []struct {
		columnName  string
		isTimestamp bool
		description string
	}{
		{
			columnName:  "_ts",
			isTimestamp: true,
			description: "System timestamp display column should be detected",
		},
		{
			columnName:  "_ts_ns",
			isTimestamp: true,
			description: "Internal timestamp column should be detected",
		},
		{
			columnName:  "user_id",
			isTimestamp: false,
			description: "Non-timestamp column should not be detected as timestamp",
		},
		{
			columnName:  "id",
			isTimestamp: false,
			description: "ID column should not be detected as timestamp",
		},
		{
			columnName:  "status",
			isTimestamp: false,
			description: "Status column should not be detected as timestamp",
		},
		{
			columnName:  "event_type",
			isTimestamp: false,
			description: "Event type column should not be detected as timestamp",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.columnName, func(t *testing.T) {
			isTimestamp := engine.SQLEngine.isTimestampColumn(tc.columnName)
			if isTimestamp != tc.isTimestamp {
				t.Errorf("Expected isTimestampColumn(%s)=%v, got %v. %s",
					tc.columnName, tc.isTimestamp, isTimestamp, tc.description)
			}
			t.Logf("Column '%s': isTimestamp=%v", tc.columnName, isTimestamp)
		})
	}
}
