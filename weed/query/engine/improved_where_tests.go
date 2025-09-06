package engine

import (
	"context"
	"strconv"
	"testing"
)

// TestWhereClauses_ImprovedCoverage provides the test coverage that should have caught the issue
func TestWhereClauses_ImprovedCoverage(t *testing.T) {
	engine := NewTestSQLEngine()

	// THIS IS THE TEST LOGIC THAT WAS MISSING:
	// Compare row counts between queries with and without WHERE clauses

	// Get baseline (all rows)
	allResult, err := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events")
	if err != nil {
		t.Fatalf("Baseline query failed: %v", err)
	}
	allCount := len(allResult.Rows)

	t.Logf("Baseline: %d total rows", allCount)

	// Test impossible condition - should return 0 rows
	impossibleResult, err := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events WHERE 1 = 0")
	if err != nil {
		t.Fatalf("Impossible WHERE query failed: %v", err)
	}
	impossibleCount := len(impossibleResult.Rows)

	// CRITICAL TEST: This should have caught the WHERE clause issue
	if impossibleCount == allCount {
		t.Errorf("❌ WHERE CLAUSE BUG DETECTED:")
		t.Errorf("   Query: WHERE 1 = 0 (impossible condition)")
		t.Errorf("   Expected: 0 rows")
		t.Errorf("   Actual: %d rows (same as no WHERE clause)", impossibleCount)
		t.Errorf("   Conclusion: WHERE clauses are being ignored")
	} else if impossibleCount == 0 {
		t.Logf("✅ Impossible WHERE condition correctly returns 0 rows")
	} else {
		t.Errorf("❓ Unexpected: impossible WHERE returned %d rows", impossibleCount)
	}

	// Test specific ID filtering
	if allCount > 0 {
		// Get first ID from results
		firstId := allResult.Rows[0][0].ToString()
		specificResult, err := engine.ExecuteSQL(context.Background(),
			"SELECT id FROM user_events WHERE id = "+firstId)

		if err != nil {
			t.Fatalf("Specific ID WHERE query failed: %v", err)
		}
		specificCount := len(specificResult.Rows)

		if specificCount == allCount {
			t.Errorf("❌ WHERE CLAUSE BUG DETECTED:")
			t.Errorf("   Query: WHERE id = %s", firstId)
			t.Errorf("   Expected: 1 row (or small number)")
			t.Errorf("   Actual: %d rows (same as no WHERE clause)", specificCount)
		} else if specificCount == 1 {
			t.Logf("✅ Specific ID WHERE clause working correctly")
		}
	}
}

// TestActualVsExpectedBehavior shows what the real behavior should be
func TestActualVsExpectedBehavior(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Log("DEMONSTRATING ACTUAL vs EXPECTED BEHAVIOR")
	t.Log("==========================================")

	testCases := []struct {
		sql      string
		expected string
		actual   string
	}{
		{
			sql:      "SELECT id FROM user_events WHERE id > 10000000",
			expected: "Only IDs > 10,000,000",
			actual:   "All rows returned (filtering ignored)",
		},
		{
			sql:      "SELECT id FROM user_events WHERE 1 = 0",
			expected: "0 rows (impossible condition)",
			actual:   "All rows returned (filtering ignored)",
		},
		{
			sql:      "SELECT id FROM user_events WHERE id = 999999999",
			expected: "0 or 1 rows (specific match)",
			actual:   "All rows returned (filtering ignored)",
		},
	}

	for _, tc := range testCases {
		result, err := engine.ExecuteSQL(context.Background(), tc.sql)
		if err != nil {
			t.Errorf("Query failed: %v", err)
			continue
		}

		actualCount := len(result.Rows)
		t.Logf("Query: %s", tc.sql)
		t.Logf("  Expected: %s", tc.expected)
		t.Logf("  Actual: %d rows returned (%s)", actualCount, tc.actual)

		// For the user's specific case - check if filtering happened
		if tc.sql == "SELECT id FROM user_events WHERE id > 10000000" {
			nonMatchingCount := 0
			for _, row := range result.Rows {
				if idStr := row[0].ToString(); idStr != "" {
					if idVal, err := strconv.ParseInt(idStr, 10, 64); err == nil {
						if idVal <= 10000000 {
							nonMatchingCount++
						}
					}
				}
			}

			if nonMatchingCount > 0 {
				t.Logf("  PROOF OF BUG: %d rows have id <= 10,000,000 (should be filtered out)", nonMatchingCount)
			}
		}
		t.Log()
	}
}
