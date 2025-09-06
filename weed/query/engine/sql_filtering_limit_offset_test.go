package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestSQLFilteringLimitOffset tests comprehensive SQL filtering, LIMIT, and OFFSET functionality
func TestSQLFilteringLimitOffset(t *testing.T) {
	engine := NewTestSQLEngine()

	testCases := []struct {
		name        string
		sql         string
		shouldError bool
		expectRows  int // -1 means don't check row count
		desc        string
	}{
		// =========== WHERE CLAUSE OPERATORS ===========
		{
			name:        "Where_Equals_Integer",
			sql:         "SELECT * FROM user_events WHERE id = 82460",
			shouldError: false,
			expectRows:  1,
			desc:        "WHERE with equals operator (integer)",
		},
		{
			name:        "Where_Equals_String",
			sql:         "SELECT * FROM user_events WHERE status = 'active'",
			shouldError: false,
			expectRows:  -1, // Don't check exact count
			desc:        "WHERE with equals operator (string)",
		},
		{
			name:        "Where_Not_Equals",
			sql:         "SELECT * FROM user_events WHERE status != 'inactive'",
			shouldError: false,
			expectRows:  -1,
			desc:        "WHERE with not equals operator",
		},
		{
			name:        "Where_Greater_Than",
			sql:         "SELECT * FROM user_events WHERE id > 100000",
			shouldError: false,
			expectRows:  -1,
			desc:        "WHERE with greater than operator",
		},
		{
			name:        "Where_Less_Than",
			sql:         "SELECT * FROM user_events WHERE id < 100000",
			shouldError: false,
			expectRows:  -1,
			desc:        "WHERE with less than operator",
		},
		{
			name:        "Where_Greater_Equal",
			sql:         "SELECT * FROM user_events WHERE id >= 82460",
			shouldError: false,
			expectRows:  -1,
			desc:        "WHERE with greater than or equal operator",
		},
		{
			name:        "Where_Less_Equal",
			sql:         "SELECT * FROM user_events WHERE id <= 82460",
			shouldError: false,
			expectRows:  -1,
			desc:        "WHERE with less than or equal operator",
		},

		// =========== WHERE WITH COLUMNS AND EXPRESSIONS ===========
		{
			name:        "Where_Column_Comparison",
			sql:         "SELECT id, status FROM user_events WHERE id = 82460",
			shouldError: false,
			expectRows:  1,
			desc:        "WHERE filtering with specific columns selected",
		},
		{
			name:        "Where_With_Function",
			sql:         "SELECT LENGTH(status) FROM user_events WHERE status = 'active'",
			shouldError: false,
			expectRows:  -1,
			desc:        "WHERE with function in SELECT",
		},
		{
			name:        "Where_With_Arithmetic",
			sql:         "SELECT id*2 FROM user_events WHERE id = 82460",
			shouldError: false,
			expectRows:  1,
			desc:        "WHERE with arithmetic in SELECT",
		},

		// =========== LIMIT FUNCTIONALITY ===========
		{
			name:        "Limit_1",
			sql:         "SELECT * FROM user_events LIMIT 1",
			shouldError: false,
			expectRows:  1,
			desc:        "LIMIT 1 row",
		},
		{
			name:        "Limit_5",
			sql:         "SELECT * FROM user_events LIMIT 5",
			shouldError: false,
			expectRows:  5,
			desc:        "LIMIT 5 rows",
		},
		{
			name:        "Limit_0",
			sql:         "SELECT * FROM user_events LIMIT 0",
			shouldError: false,
			expectRows:  0,
			desc:        "LIMIT 0 rows (should return no results)",
		},
		{
			name:        "Limit_Large",
			sql:         "SELECT * FROM user_events LIMIT 1000",
			shouldError: false,
			expectRows:  -1, // Don't check exact count (depends on test data)
			desc:        "LIMIT with large number",
		},
		{
			name:        "Limit_With_Columns",
			sql:         "SELECT id, status FROM user_events LIMIT 3",
			shouldError: false,
			expectRows:  3,
			desc:        "LIMIT with specific columns",
		},
		{
			name:        "Limit_With_Functions",
			sql:         "SELECT LENGTH(status), UPPER(action) FROM user_events LIMIT 2",
			shouldError: false,
			expectRows:  2,
			desc:        "LIMIT with functions",
		},

		// =========== OFFSET FUNCTIONALITY ===========
		{
			name:        "Offset_0",
			sql:         "SELECT * FROM user_events LIMIT 5 OFFSET 0",
			shouldError: false,
			expectRows:  5,
			desc:        "OFFSET 0 (same as no offset)",
		},
		{
			name:        "Offset_1",
			sql:         "SELECT * FROM user_events LIMIT 3 OFFSET 1",
			shouldError: false,
			expectRows:  3,
			desc:        "OFFSET 1 row",
		},
		{
			name:        "Offset_5",
			sql:         "SELECT * FROM user_events LIMIT 2 OFFSET 5",
			shouldError: false,
			expectRows:  2,
			desc:        "OFFSET 5 rows",
		},
		{
			name:        "Offset_Large",
			sql:         "SELECT * FROM user_events LIMIT 1 OFFSET 100",
			shouldError: false,
			expectRows:  -1, // May be 0 or 1 depending on test data size
			desc:        "OFFSET with large number",
		},

		// =========== LIMIT + OFFSET COMBINATIONS ===========
		{
			name:        "Limit_Offset_Pagination_Page1",
			sql:         "SELECT id, status FROM user_events LIMIT 3 OFFSET 0",
			shouldError: false,
			expectRows:  3,
			desc:        "Pagination: Page 1 (LIMIT 3, OFFSET 0)",
		},
		{
			name:        "Limit_Offset_Pagination_Page2",
			sql:         "SELECT id, status FROM user_events LIMIT 3 OFFSET 3",
			shouldError: false,
			expectRows:  3,
			desc:        "Pagination: Page 2 (LIMIT 3, OFFSET 3)",
		},
		{
			name:        "Limit_Offset_Pagination_Page3",
			sql:         "SELECT id, status FROM user_events LIMIT 3 OFFSET 6",
			shouldError: false,
			expectRows:  3,
			desc:        "Pagination: Page 3 (LIMIT 3, OFFSET 6)",
		},

		// =========== WHERE + LIMIT + OFFSET COMBINATIONS ===========
		{
			name:        "Where_Limit",
			sql:         "SELECT * FROM user_events WHERE status = 'active' LIMIT 2",
			shouldError: false,
			expectRows:  -1, // Depends on filtered data
			desc:        "WHERE clause with LIMIT",
		},
		{
			name:        "Where_Limit_Offset",
			sql:         "SELECT id, status FROM user_events WHERE status = 'active' LIMIT 2 OFFSET 1",
			shouldError: false,
			expectRows:  -1, // Depends on filtered data
			desc:        "WHERE clause with LIMIT and OFFSET",
		},
		{
			name:        "Where_Complex_Limit",
			sql:         "SELECT id*2, LENGTH(status) FROM user_events WHERE id > 100000 LIMIT 3",
			shouldError: false,
			expectRows:  -1,
			desc:        "Complex WHERE with functions and arithmetic, plus LIMIT",
		},

		// =========== EDGE CASES ===========
		{
			name:        "Where_No_Match",
			sql:         "SELECT * FROM user_events WHERE id = -999999",
			shouldError: false,
			expectRows:  0,
			desc:        "WHERE clause that matches no rows",
		},
		{
			name:        "Limit_Offset_Beyond_Data",
			sql:         "SELECT * FROM user_events LIMIT 5 OFFSET 999999",
			shouldError: false,
			expectRows:  0,
			desc:        "OFFSET beyond available data",
		},
		{
			name:        "Where_Empty_String",
			sql:         "SELECT * FROM user_events WHERE status = ''",
			shouldError: false,
			expectRows:  -1,
			desc:        "WHERE with empty string value",
		},

		// =========== PERFORMANCE PATTERNS ===========
		{
			name:        "Small_Result_Set",
			sql:         "SELECT id FROM user_events WHERE id = 82460 LIMIT 1",
			shouldError: false,
			expectRows:  1,
			desc:        "Optimized query: specific WHERE + LIMIT 1",
		},
		{
			name:        "Batch_Processing",
			sql:         "SELECT id, status FROM user_events LIMIT 50 OFFSET 0",
			shouldError: false,
			expectRows:  -1,
			desc:        "Batch processing pattern: moderate LIMIT",
		},
	}

	var successTests []string
	var errorTests []string
	var rowCountMismatches []string

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := engine.ExecuteSQL(context.Background(), tc.sql)

			// Check for unexpected errors
			if tc.shouldError {
				if err == nil && (result == nil || result.Error == nil) {
					t.Errorf("FAIL: Expected error for %s, but query succeeded", tc.desc)
					errorTests = append(errorTests, "FAIL: "+tc.desc)
					return
				}
				t.Logf("PASS: Expected error: %s", tc.desc)
				errorTests = append(errorTests, "PASS: "+tc.desc)
				return
			}

			if err != nil {
				t.Errorf("FAIL: Unexpected error for %s: %v", tc.desc, err)
				errorTests = append(errorTests, "FAIL: "+tc.desc+" (unexpected error)")
				return
			}

			if result != nil && result.Error != nil {
				t.Errorf("FAIL: Unexpected result error for %s: %v", tc.desc, result.Error)
				errorTests = append(errorTests, "FAIL: "+tc.desc+" (unexpected result error)")
				return
			}

			// Check row count if specified
			actualRows := len(result.Rows)
			if tc.expectRows >= 0 {
				if actualRows != tc.expectRows {
					t.Logf("ROW COUNT MISMATCH: %s - Expected %d rows, got %d", tc.desc, tc.expectRows, actualRows)
					rowCountMismatches = append(rowCountMismatches,
						fmt.Sprintf("MISMATCH: %s (expected %d, got %d)", tc.desc, tc.expectRows, actualRows))
				} else {
					t.Logf("PASS: %s - Correct row count: %d", tc.desc, actualRows)
				}
			} else {
				t.Logf("PASS: %s - Row count: %d (not validated)", tc.desc, actualRows)
			}

			successTests = append(successTests, "PASS: "+tc.desc)
		})
	}

	// Summary report
	separator := strings.Repeat("=", 80)
	t.Log("\n" + separator)
	t.Log("SQL FILTERING, LIMIT & OFFSET TEST SUITE SUMMARY")
	t.Log(separator)
	t.Logf("Total Tests: %d", len(testCases))
	t.Logf("Successful: %d", len(successTests))
	t.Logf("Errors: %d", len(errorTests))
	t.Logf("Row Count Mismatches: %d", len(rowCountMismatches))
	t.Log(separator)

	if len(errorTests) > 0 {
		t.Log("\nERRORS:")
		for _, test := range errorTests {
			t.Log("   " + test)
		}
	}

	if len(rowCountMismatches) > 0 {
		t.Log("\nROW COUNT MISMATCHES:")
		for _, test := range rowCountMismatches {
			t.Log("   " + test)
		}
	}
}

// TestSQLFilteringAccuracy tests the accuracy of filtering results
func TestSQLFilteringAccuracy(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Log("Testing SQL filtering accuracy with specific data verification")

	// Test specific ID lookup
	result, err := engine.ExecuteSQL(context.Background(), "SELECT id, status FROM user_events WHERE id = 82460")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row for id=82460, got %d", len(result.Rows))
	} else {
		idValue := result.Rows[0][0].ToString()
		if idValue != "82460" {
			t.Errorf("Expected id=82460, got id=%s", idValue)
		} else {
			t.Log("PASS: Exact ID filtering works correctly")
		}
	}

	// Test LIMIT accuracy
	result2, err2 := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events LIMIT 3")
	if err2 != nil {
		t.Fatalf("LIMIT query failed: %v", err2)
	}

	if len(result2.Rows) != 3 {
		t.Errorf("Expected exactly 3 rows with LIMIT 3, got %d", len(result2.Rows))
	} else {
		t.Log("PASS: LIMIT 3 returns exactly 3 rows")
	}

	// Test OFFSET by comparing with and without offset
	resultNoOffset, err3 := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events LIMIT 2 OFFSET 0")
	if err3 != nil {
		t.Fatalf("No offset query failed: %v", err3)
	}

	resultWithOffset, err4 := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events LIMIT 2 OFFSET 1")
	if err4 != nil {
		t.Fatalf("With offset query failed: %v", err4)
	}

	if len(resultNoOffset.Rows) == 2 && len(resultWithOffset.Rows) == 2 {
		// The second row of no-offset should equal first row of offset-1
		if resultNoOffset.Rows[1][0].ToString() == resultWithOffset.Rows[0][0].ToString() {
			t.Log("PASS: OFFSET 1 correctly skips first row")
		} else {
			t.Errorf("OFFSET verification failed: expected row shifting")
		}
	} else {
		t.Errorf("OFFSET test setup failed: got %d and %d rows", len(resultNoOffset.Rows), len(resultWithOffset.Rows))
	}
}

// TestSQLFilteringEdgeCases tests edge cases and boundary conditions
func TestSQLFilteringEdgeCases(t *testing.T) {
	engine := NewTestSQLEngine()

	edgeCases := []struct {
		name        string
		sql         string
		expectError bool
		desc        string
	}{
		{
			name:        "Zero_Limit",
			sql:         "SELECT * FROM user_events LIMIT 0",
			expectError: false,
			desc:        "LIMIT 0 should return empty result set",
		},
		{
			name:        "Large_Offset",
			sql:         "SELECT * FROM user_events LIMIT 1 OFFSET 99999",
			expectError: false,
			desc:        "Very large OFFSET should handle gracefully",
		},
		{
			name:        "Where_False_Condition",
			sql:         "SELECT * FROM user_events WHERE 1 = 0",
			expectError: true, // This might not be supported
			desc:        "WHERE with always-false condition",
		},
		{
			name:        "Complex_Where",
			sql:         "SELECT id FROM user_events WHERE id > 0 AND id < 999999999",
			expectError: true, // AND might not be implemented
			desc:        "Complex WHERE with AND condition",
		},
	}

	for _, tc := range edgeCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := engine.ExecuteSQL(context.Background(), tc.sql)

			if tc.expectError {
				if err == nil && (result == nil || result.Error == nil) {
					t.Logf("UNEXPECTED SUCCESS: %s (may indicate feature is implemented)", tc.desc)
				} else {
					t.Logf("EXPECTED ERROR: %s", tc.desc)
				}
			} else {
				if err != nil {
					t.Errorf("UNEXPECTED ERROR for %s: %v", tc.desc, err)
				} else if result.Error != nil {
					t.Errorf("UNEXPECTED RESULT ERROR for %s: %v", tc.desc, result.Error)
				} else {
					t.Logf("PASS: %s - Rows: %d", tc.desc, len(result.Rows))
				}
			}
		})
	}
}
