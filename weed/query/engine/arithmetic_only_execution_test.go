package engine

import (
	"context"
	"testing"
)

// TestSQLEngine_ArithmeticOnlyQueryExecution tests the specific fix for queries
// that contain ONLY arithmetic expressions (no base columns) in the SELECT clause.
// This was the root issue reported where such queries returned empty values.
func TestSQLEngine_ArithmeticOnlyQueryExecution(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test the core functionality: arithmetic-only queries should return data
	tests := []struct {
		name           string
		query          string
		expectedCols   []string
		mustNotBeEmpty bool
	}{
		{
			name:           "Basic arithmetic only query",
			query:          "SELECT id+user_id, id*2 FROM user_events LIMIT 3",
			expectedCols:   []string{"id+user_id", "id*2"},
			mustNotBeEmpty: true,
		},
		{
			name:           "With LIMIT and OFFSET - original user issue",
			query:          "SELECT id+user_id, id*2 FROM user_events LIMIT 2 OFFSET 1",
			expectedCols:   []string{"id+user_id", "id*2"},
			mustNotBeEmpty: true,
		},
		{
			name:           "Multiple arithmetic expressions",
			query:          "SELECT user_id+100, id-1000 FROM user_events LIMIT 1",
			expectedCols:   []string{"user_id+100", "id-1000"},
			mustNotBeEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.ExecuteSQL(context.Background(), tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result.Error != nil {
				t.Fatalf("Query returned error: %v", result.Error)
			}

			// CRITICAL: Verify we got results (the original bug would return empty)
			if tt.mustNotBeEmpty && len(result.Rows) == 0 {
				t.Fatal("CRITICAL BUG: Query returned no rows - arithmetic-only query fix failed!")
			}

			// Verify column count and names
			if len(result.Columns) != len(tt.expectedCols) {
				t.Errorf("Expected %d columns, got %d", len(tt.expectedCols), len(result.Columns))
			}

			// CRITICAL: Verify no empty/null values (the original bug symptom)
			if len(result.Rows) > 0 {
				firstRow := result.Rows[0]
				for i, val := range firstRow {
					if val.IsNull() {
						t.Errorf("CRITICAL BUG: Column %d (%s) returned NULL", i, result.Columns[i])
					}
					if val.ToString() == "" {
						t.Errorf("CRITICAL BUG: Column %d (%s) returned empty string", i, result.Columns[i])
					}
				}
			}

			// Log success
			t.Logf("SUCCESS: %s returned %d rows with calculated values", tt.query, len(result.Rows))
		})
	}
}

// TestSQLEngine_ArithmeticOnlyQueryBugReproduction tests that the original bug
// (returning empty values) would have failed before our fix
func TestSQLEngine_ArithmeticOnlyQueryBugReproduction(t *testing.T) {
	engine := NewTestSQLEngine()

	// This is the EXACT query from the user's bug report
	query := "SELECT id+user_id, id*amount, id*2 FROM user_events LIMIT 10 OFFSET 5"

	result, err := engine.ExecuteSQL(context.Background(), query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.Error != nil {
		t.Fatalf("Query returned error: %v", result.Error)
	}

	// Key assertions that would fail with the original bug:

	// 1. Must return rows (bug would return 0 rows or empty results)
	if len(result.Rows) == 0 {
		t.Fatal("CRITICAL: Query returned no rows - the original bug is NOT fixed!")
	}

	// 2. Must have expected columns
	expectedColumns := []string{"id+user_id", "id*amount", "id*2"}
	if len(result.Columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
	}

	// 3. Must have calculated values, not empty/null
	for i, row := range result.Rows {
		for j, val := range row {
			if val.IsNull() {
				t.Errorf("Row %d, Column %d (%s) is NULL - original bug not fixed!",
					i, j, result.Columns[j])
			}
			if val.ToString() == "" {
				t.Errorf("Row %d, Column %d (%s) is empty - original bug not fixed!",
					i, j, result.Columns[j])
			}
		}
	}

	// 4. Verify specific calculations for the OFFSET 5 data
	if len(result.Rows) > 0 {
		firstRow := result.Rows[0]
		// With OFFSET 5, first returned row should be 6th row: id=417224, user_id=7810
		expectedSum := "425034" // 417224 + 7810
		if firstRow[0].ToString() != expectedSum {
			t.Errorf("OFFSET 5 calculation wrong: expected id+user_id=%s, got %s",
				expectedSum, firstRow[0].ToString())
		}

		expectedDouble := "834448" // 417224 * 2
		if firstRow[2].ToString() != expectedDouble {
			t.Errorf("OFFSET 5 calculation wrong: expected id*2=%s, got %s",
				expectedDouble, firstRow[2].ToString())
		}
	}

	t.Logf("SUCCESS: Arithmetic-only query with OFFSET works correctly!")
	t.Logf("Query: %s", query)
	t.Logf("Returned %d rows with correct calculations", len(result.Rows))
}
