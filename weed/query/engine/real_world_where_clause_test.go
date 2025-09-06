package engine

import (
	"context"
	"strconv"
	"testing"
)

// TestRealWorldWhereClauseFailure demonstrates the exact WHERE clause issue from real usage
func TestRealWorldWhereClauseFailure(t *testing.T) {
	engine := NewTestSQLEngine()

	// This test simulates the exact real-world scenario that failed
	testCases := []struct {
		name        string
		sql         string
		filterValue int64
		operator    string
		desc        string
	}{
		{
			name:        "Where_ID_Greater_Than_Large_Number",
			sql:         "SELECT id FROM user_events WHERE id > 10000000",
			filterValue: 10000000,
			operator:    ">",
			desc:        "Real-world case: WHERE id > 10000000 should filter results",
		},
		{
			name:        "Where_ID_Greater_Than_Small_Number",
			sql:         "SELECT id FROM user_events WHERE id > 100000",
			filterValue: 100000,
			operator:    ">",
			desc:        "WHERE id > 100000 should filter results",
		},
		{
			name:        "Where_ID_Less_Than",
			sql:         "SELECT id FROM user_events WHERE id < 100000",
			filterValue: 100000,
			operator:    "<",
			desc:        "WHERE id < 100000 should filter results",
		},
	}

	t.Log("TESTING REAL-WORLD WHERE CLAUSE SCENARIOS")
	t.Log("============================================")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := engine.ExecuteSQL(context.Background(), tc.sql)

			if err != nil {
				t.Errorf("Query failed: %v", err)
				return
			}

			if result.Error != nil {
				t.Errorf("Result error: %v", result.Error)
				return
			}

			// Analyze the actual results
			actualRows := len(result.Rows)
			var matchingRows, nonMatchingRows int

			t.Logf("Query: %s", tc.sql)
			t.Logf("Total rows returned: %d", actualRows)

			if actualRows > 0 {
				t.Logf("Sample IDs returned:")
				sampleSize := 5
				if actualRows < sampleSize {
					sampleSize = actualRows
				}

				for i := 0; i < sampleSize; i++ {
					idStr := result.Rows[i][0].ToString()
					if idValue, err := strconv.ParseInt(idStr, 10, 64); err == nil {
						t.Logf("  Row %d: id = %d", i+1, idValue)

						// Check if this row should have been filtered
						switch tc.operator {
						case ">":
							if idValue > tc.filterValue {
								matchingRows++
							} else {
								nonMatchingRows++
							}
						case "<":
							if idValue < tc.filterValue {
								matchingRows++
							} else {
								nonMatchingRows++
							}
						}
					}
				}

				// Count all rows for accurate assessment
				allMatchingRows, allNonMatchingRows := 0, 0
				for _, row := range result.Rows {
					idStr := row[0].ToString()
					if idValue, err := strconv.ParseInt(idStr, 10, 64); err == nil {
						switch tc.operator {
						case ">":
							if idValue > tc.filterValue {
								allMatchingRows++
							} else {
								allNonMatchingRows++
							}
						case "<":
							if idValue < tc.filterValue {
								allMatchingRows++
							} else {
								allNonMatchingRows++
							}
						}
					}
				}

				t.Logf("Analysis:")
				t.Logf("  Rows matching WHERE condition: %d", allMatchingRows)
				t.Logf("  Rows NOT matching WHERE condition: %d", allNonMatchingRows)

				if allNonMatchingRows > 0 {
					t.Errorf("FAIL: %s - Found %d rows that should have been filtered out", tc.desc, allNonMatchingRows)
					t.Errorf("      This confirms WHERE clause is being ignored")
				} else {
					t.Logf("PASS: %s - All returned rows match the WHERE condition", tc.desc)
				}
			} else {
				t.Logf("No rows returned - this could be correct if no data matches")
			}
		})
	}
}

// TestWhereClauseWithLimitOffset tests the exact failing scenario
func TestWhereClauseWithLimitOffset(t *testing.T) {
	engine := NewTestSQLEngine()

	// The exact query that was failing in real usage
	sql := "SELECT id FROM user_events WHERE id > 10000000 LIMIT 10 OFFSET 5"

	t.Logf("Testing exact failing query: %s", sql)

	result, err := engine.ExecuteSQL(context.Background(), sql)

	if err != nil {
		t.Errorf("Query failed: %v", err)
		return
	}

	if result.Error != nil {
		t.Errorf("Result error: %v", result.Error)
		return
	}

	actualRows := len(result.Rows)
	t.Logf("Returned %d rows (LIMIT 10 worked)", actualRows)

	if actualRows > 10 {
		t.Errorf("LIMIT not working: expected max 10 rows, got %d", actualRows)
	}

	// Check if WHERE clause worked
	nonMatchingRows := 0
	for i, row := range result.Rows {
		idStr := row[0].ToString()
		if idValue, err := strconv.ParseInt(idStr, 10, 64); err == nil {
			t.Logf("Row %d: id = %d", i+1, idValue)
			if idValue <= 10000000 {
				nonMatchingRows++
			}
		}
	}

	if nonMatchingRows > 0 {
		t.Errorf("WHERE clause completely ignored: %d rows have id <= 10000000", nonMatchingRows)
		t.Log("This matches the real-world failure - WHERE is parsed but not executed")
	} else {
		t.Log("WHERE clause working correctly")
	}
}

// TestWhatShouldHaveBeenTested creates the test that should have caught the WHERE issue
func TestWhatShouldHaveBeenTested(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Log("THE TEST THAT SHOULD HAVE CAUGHT THE WHERE CLAUSE ISSUE")
	t.Log("========================================================")

	// Test 1: Simple WHERE that should return subset
	result1, _ := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events")
	allRowCount := len(result1.Rows)

	result2, _ := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events WHERE id > 999999999")
	filteredCount := len(result2.Rows)

	t.Logf("All rows: %d", allRowCount)
	t.Logf("WHERE id > 999999999: %d rows", filteredCount)

	if filteredCount == allRowCount {
		t.Error("CRITICAL ISSUE: WHERE clause completely ignored")
		t.Error("Expected: Fewer rows after WHERE filtering")
		t.Error("Actual: Same number of rows (no filtering occurred)")
		t.Error("This is the bug that our tests should have caught!")
	}

	// Test 2: Impossible WHERE condition
	result3, _ := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events WHERE 1 = 0")
	impossibleCount := len(result3.Rows)

	t.Logf("WHERE 1 = 0 (impossible): %d rows", impossibleCount)

	if impossibleCount > 0 {
		t.Error("CRITICAL ISSUE: Even impossible WHERE conditions ignored")
		t.Error("Expected: 0 rows")
		t.Errorf("Actual: %d rows", impossibleCount)
	}
}
