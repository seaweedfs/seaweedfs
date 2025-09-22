package engine

import (
	"context"
	"strconv"
	"testing"
)

// TestWhereClauseValidation tests WHERE clause functionality with various conditions
func TestWhereClauseValidation(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Log("WHERE CLAUSE VALIDATION TESTS")
	t.Log("==============================")

	// Test 1: Baseline - get all rows to understand the data
	baselineResult, err := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events")
	if err != nil {
		t.Fatalf("Baseline query failed: %v", err)
	}

	t.Logf("Baseline data - Total rows: %d", len(baselineResult.Rows))
	if len(baselineResult.Rows) > 0 {
		t.Logf("Sample IDs: %s, %s, %s",
			baselineResult.Rows[0][0].ToString(),
			baselineResult.Rows[1][0].ToString(),
			baselineResult.Rows[2][0].ToString())
	}

	// Test 2: Specific ID match (should return 1 row)
	firstId := baselineResult.Rows[0][0].ToString()
	specificResult, err := engine.ExecuteSQL(context.Background(),
		"SELECT id FROM user_events WHERE id = "+firstId)
	if err != nil {
		t.Fatalf("Specific ID query failed: %v", err)
	}

	t.Logf("WHERE id = %s: %d rows", firstId, len(specificResult.Rows))
	if len(specificResult.Rows) == 1 {
		t.Logf("Specific ID filtering works correctly")
	} else {
		t.Errorf("Expected 1 row, got %d rows", len(specificResult.Rows))
	}

	// Test 3: Range filtering (find actual data ranges)
	// First, find the min and max IDs in our data
	var minId, maxId int64 = 999999999, 0
	for _, row := range baselineResult.Rows {
		if idVal, err := strconv.ParseInt(row[0].ToString(), 10, 64); err == nil {
			if idVal < minId {
				minId = idVal
			}
			if idVal > maxId {
				maxId = idVal
			}
		}
	}

	t.Logf("Data range: min ID = %d, max ID = %d", minId, maxId)

	// Test with a threshold between min and max
	threshold := (minId + maxId) / 2
	rangeResult, err := engine.ExecuteSQL(context.Background(),
		"SELECT id FROM user_events WHERE id > "+strconv.FormatInt(threshold, 10))
	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}

	t.Logf("WHERE id > %d: %d rows", threshold, len(rangeResult.Rows))

	// Verify all returned IDs are > threshold
	allCorrect := true
	for _, row := range rangeResult.Rows {
		if idVal, err := strconv.ParseInt(row[0].ToString(), 10, 64); err == nil {
			if idVal <= threshold {
				t.Errorf("Found ID %d which should be filtered out (<= %d)", idVal, threshold)
				allCorrect = false
			}
		}
	}

	if allCorrect && len(rangeResult.Rows) > 0 {
		t.Logf("Range filtering works correctly - all returned IDs > %d", threshold)
	} else if len(rangeResult.Rows) == 0 {
		t.Logf("Range filtering works correctly - no IDs > %d in data", threshold)
	}

	// Test 4: String filtering
	statusResult, err := engine.ExecuteSQL(context.Background(),
		"SELECT id, status FROM user_events WHERE status = 'active'")
	if err != nil {
		t.Fatalf("Status query failed: %v", err)
	}

	t.Logf("WHERE status = 'active': %d rows", len(statusResult.Rows))

	// Verify all returned rows have status = 'active'
	statusCorrect := true
	for _, row := range statusResult.Rows {
		if len(row) > 1 && row[1].ToString() != "active" {
			t.Errorf("Found status '%s' which should be filtered out", row[1].ToString())
			statusCorrect = false
		}
	}

	if statusCorrect {
		t.Logf("String filtering works correctly")
	}

	// Test 5: Comparison with actual real-world case
	t.Log("\nTESTING REAL-WORLD CASE:")
	realWorldResult, err := engine.ExecuteSQL(context.Background(),
		"SELECT id FROM user_events WHERE id > 10000000 LIMIT 10 OFFSET 5")
	if err != nil {
		t.Fatalf("Real-world query failed: %v", err)
	}

	t.Logf("Real-world query returned: %d rows", len(realWorldResult.Rows))

	// Check if any IDs are <= 10,000,000 (should be 0)
	violationCount := 0
	for _, row := range realWorldResult.Rows {
		if idVal, err := strconv.ParseInt(row[0].ToString(), 10, 64); err == nil {
			if idVal <= 10000000 {
				violationCount++
			}
		}
	}

	if violationCount == 0 {
		t.Logf("Real-world case FIXED: No violations found")
	} else {
		t.Errorf("Real-world case FAILED: %d violations found", violationCount)
	}
}

// TestWhereClauseComparisonOperators tests all comparison operators
func TestWhereClauseComparisonOperators(t *testing.T) {
	engine := NewTestSQLEngine()

	// Get baseline data
	baselineResult, _ := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events")
	if len(baselineResult.Rows) == 0 {
		t.Skip("No test data available")
		return
	}

	// Use the second ID as our test value
	testId := baselineResult.Rows[1][0].ToString()

	operators := []struct {
		op         string
		desc       string
		expectRows bool
	}{
		{"=", "equals", true},
		{"!=", "not equals", true},
		{">", "greater than", false}, // Depends on data
		{"<", "less than", true},     // Should have some results
		{">=", "greater or equal", true},
		{"<=", "less or equal", true},
	}

	t.Logf("Testing comparison operators with ID = %s", testId)

	for _, op := range operators {
		sql := "SELECT id FROM user_events WHERE id " + op.op + " " + testId
		result, err := engine.ExecuteSQL(context.Background(), sql)

		if err != nil {
			t.Errorf("Operator %s failed: %v", op.op, err)
			continue
		}

		t.Logf("WHERE id %s %s: %d rows (%s)", op.op, testId, len(result.Rows), op.desc)

		// Basic validation - should not return more rows than baseline
		if len(result.Rows) > len(baselineResult.Rows) {
			t.Errorf("Operator %s returned more rows than baseline", op.op)
		}
	}
}
