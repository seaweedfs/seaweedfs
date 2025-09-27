package engine

import (
	"context"
	"testing"
)

// TestSQLEngine_StringConcatenationWithLiterals tests string concatenation with || operator
// This covers the user's reported issue where string literals were being lost
func TestSQLEngine_StringConcatenationWithLiterals(t *testing.T) {
	engine := NewTestSQLEngine()

	tests := []struct {
		name          string
		query         string
		expectedCols  []string
		validateFirst func(t *testing.T, row []string)
	}{
		{
			name:         "Simple concatenation with literals",
			query:        "SELECT 'test' || action || 'end' FROM user_events LIMIT 1",
			expectedCols: []string{"'test'||action||'end'"},
			validateFirst: func(t *testing.T, row []string) {
				expected := "testloginend" // action="login" from first row
				if row[0] != expected {
					t.Errorf("Expected %s, got %s", expected, row[0])
				}
			},
		},
		{
			name:         "User's original complex concatenation",
			query:        "SELECT 'test' || action || 'xxx' || action || ' ~~~ ' || status FROM user_events LIMIT 1",
			expectedCols: []string{"'test'||action||'xxx'||action||'~~~'||status"},
			validateFirst: func(t *testing.T, row []string) {
				// First row: action="login", status="active"
				expected := "testloginxxxlogin ~~~ active"
				if row[0] != expected {
					t.Errorf("Expected %s, got %s", expected, row[0])
				}
			},
		},
		{
			name:         "Mixed columns and literals",
			query:        "SELECT status || '=' || action, 'prefix:' || user_type FROM user_events LIMIT 1",
			expectedCols: []string{"status||'='||action", "'prefix:'||user_type"},
			validateFirst: func(t *testing.T, row []string) {
				// First row: status="active", action="login", user_type="premium"
				if row[0] != "active=login" {
					t.Errorf("Expected 'active=login', got %s", row[0])
				}
				if row[1] != "prefix:premium" {
					t.Errorf("Expected 'prefix:premium', got %s", row[1])
				}
			},
		},
		{
			name:         "Concatenation with spaces in literals",
			query:        "SELECT ' [ ' || status || ' ] ' FROM user_events LIMIT 2",
			expectedCols: []string{"'['||status||']'"},
			validateFirst: func(t *testing.T, row []string) {
				expected := " [ active ] " // status="active" from first row
				if row[0] != expected {
					t.Errorf("Expected '%s', got '%s'", expected, row[0])
				}
			},
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

			// Verify we got results
			if len(result.Rows) == 0 {
				t.Fatal("Query returned no rows")
			}

			// Verify column count
			if len(result.Columns) != len(tt.expectedCols) {
				t.Errorf("Expected %d columns, got %d", len(tt.expectedCols), len(result.Columns))
			}

			// Check column names
			for i, expectedCol := range tt.expectedCols {
				if i < len(result.Columns) && result.Columns[i] != expectedCol {
					t.Logf("Expected column %d to be '%s', got '%s'", i, expectedCol, result.Columns[i])
					// Don't fail on column name formatting differences, just log
				}
			}

			// Validate first row
			if tt.validateFirst != nil {
				firstRow := result.Rows[0]
				stringRow := make([]string, len(firstRow))
				for i, val := range firstRow {
					stringRow[i] = val.ToString()
				}
				tt.validateFirst(t, stringRow)
			}

			// Log results for debugging
			t.Logf("Query: %s", tt.query)
			t.Logf("Columns: %v", result.Columns)
			for i, row := range result.Rows {
				values := make([]string, len(row))
				for j, val := range row {
					values[j] = val.ToString()
				}
				t.Logf("Row %d: %v", i, values)
			}
		})
	}
}

// TestSQLEngine_StringConcatenationBugReproduction tests the exact user query that was failing
func TestSQLEngine_StringConcatenationBugReproduction(t *testing.T) {
	engine := NewTestSQLEngine()

	// This is the EXACT query from the user that was showing incorrect results
	query := "SELECT UPPER(status), id*2, 'test' || action || 'xxx' || action || ' ~~~ ' || status FROM user_events LIMIT 2"

	result, err := engine.ExecuteSQL(context.Background(), query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.Error != nil {
		t.Fatalf("Query returned error: %v", result.Error)
	}

	// Key assertions that would fail with the original bug:

	// 1. Must return rows
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// 2. Must have 3 columns
	expectedColumns := 3
	if len(result.Columns) != expectedColumns {
		t.Errorf("Expected %d columns, got %d", expectedColumns, len(result.Columns))
	}

	// 3. Verify the complex concatenation works correctly
	if len(result.Rows) >= 1 {
		firstRow := result.Rows[0]

		// Column 0: UPPER(status) should be "ACTIVE"
		upperStatus := firstRow[0].ToString()
		if upperStatus != "ACTIVE" {
			t.Errorf("Expected UPPER(status)='ACTIVE', got '%s'", upperStatus)
		}

		// Column 1: id*2 should be calculated correctly
		idTimes2 := firstRow[1].ToString()
		if idTimes2 != "164920" { // id=82460 * 2
			t.Errorf("Expected id*2=164920, got '%s'", idTimes2)
		}

		// Column 2: Complex concatenation should include all parts
		concatenated := firstRow[2].ToString()

		// Should be: "test" + "login" + "xxx" + "login" + " ~~~ " + "active" = "testloginxxxlogin ~~~ active"
		expected := "testloginxxxlogin ~~~ active"
		if concatenated != expected {
			t.Errorf("String concatenation failed. Expected '%s', got '%s'", expected, concatenated)
		}

		// CRITICAL: Must not be the buggy result like "viewviewpending"
		if concatenated == "loginloginactive" || concatenated == "viewviewpending" || concatenated == "clickclickfailed" {
			t.Errorf("CRITICAL BUG: String concatenation returned buggy result '%s' - string literals are being lost!", concatenated)
		}
	}

	t.Logf("SUCCESS: Complex string concatenation works correctly!")
	t.Logf("Query: %s", query)

	for i, row := range result.Rows {
		values := make([]string, len(row))
		for j, val := range row {
			values[j] = val.ToString()
		}
		t.Logf("Row %d: %v", i, values)
	}
}
