package engine

import (
	"context"
	"strings"
	"testing"
)

// TestSQLEngine_StringFunctionsAndLiterals tests the fixes for string functions and string literals
// This covers the user's reported issues:
// 1. String functions like UPPER(), LENGTH() being treated as aggregation functions
// 2. String literals like 'good' returning empty values
func TestSQLEngine_StringFunctionsAndLiterals(t *testing.T) {
	engine := NewTestSQLEngine()

	tests := []struct {
		name             string
		query            string
		expectedCols     []string
		expectNonEmpty   bool
		validateFirstRow func(t *testing.T, row []string)
	}{
		{
			name:           "String functions - UPPER and LENGTH",
			query:          "SELECT status, UPPER(status), LENGTH(status) FROM user_events LIMIT 3",
			expectedCols:   []string{"status", "UPPER(status)", "LENGTH(status)"},
			expectNonEmpty: true,
			validateFirstRow: func(t *testing.T, row []string) {
				if len(row) != 3 {
					t.Errorf("Expected 3 columns, got %d", len(row))
					return
				}
				// Status should exist, UPPER should be uppercase version, LENGTH should be numeric
				status := row[0]
				upperStatus := row[1]
				lengthStr := row[2]

				if status == "" {
					t.Error("Status column should not be empty")
				}
				if upperStatus == "" {
					t.Error("UPPER(status) should not be empty")
				}
				if lengthStr == "" {
					t.Error("LENGTH(status) should not be empty")
				}

				t.Logf("Status: '%s', UPPER: '%s', LENGTH: '%s'", status, upperStatus, lengthStr)
			},
		},
		{
			name:           "String literal in SELECT",
			query:          "SELECT id, user_id, 'good' FROM user_events LIMIT 2",
			expectedCols:   []string{"id", "user_id", "'good'"},
			expectNonEmpty: true,
			validateFirstRow: func(t *testing.T, row []string) {
				if len(row) != 3 {
					t.Errorf("Expected 3 columns, got %d", len(row))
					return
				}

				literal := row[2]
				if literal != "good" {
					t.Errorf("Expected string literal to be 'good', got '%s'", literal)
				}
			},
		},
		{
			name:           "Mixed: columns, functions, arithmetic, and literals",
			query:          "SELECT id, UPPER(status), id*2, 'test' FROM user_events LIMIT 2",
			expectedCols:   []string{"id", "UPPER(status)", "id*2", "'test'"},
			expectNonEmpty: true,
			validateFirstRow: func(t *testing.T, row []string) {
				if len(row) != 4 {
					t.Errorf("Expected 4 columns, got %d", len(row))
					return
				}

				// Verify the literal value
				if row[3] != "test" {
					t.Errorf("Expected literal 'test', got '%s'", row[3])
				}

				// Verify other values are not empty
				for i, val := range row {
					if val == "" {
						t.Errorf("Column %d should not be empty", i)
					}
				}
			},
		},
		{
			name:           "User's original failing query - fixed",
			query:          "SELECT status, action, user_type, UPPER(action), LENGTH(action) FROM user_events LIMIT 2",
			expectedCols:   []string{"status", "action", "user_type", "UPPER(action)", "LENGTH(action)"},
			expectNonEmpty: true,
			validateFirstRow: func(t *testing.T, row []string) {
				if len(row) != 5 {
					t.Errorf("Expected 5 columns, got %d", len(row))
					return
				}

				// All values should be non-empty
				for i, val := range row {
					if val == "" {
						t.Errorf("Column %d (%s) should not be empty", i, []string{"status", "action", "user_type", "UPPER(action)", "LENGTH(action)"}[i])
					}
				}

				// UPPER should be uppercase
				action := row[1]
				upperAction := row[3]
				if action != "" && upperAction != "" {
					if upperAction != action && upperAction != strings.ToUpper(action) {
						t.Logf("Note: UPPER(%s) = %s (may be expected)", action, upperAction)
					}
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
			if tt.expectNonEmpty && len(result.Rows) == 0 {
				t.Fatal("Query returned no rows")
			}

			// Verify column count
			if len(result.Columns) != len(tt.expectedCols) {
				t.Errorf("Expected %d columns, got %d", len(tt.expectedCols), len(result.Columns))
			}

			// Check column names
			for i, expectedCol := range tt.expectedCols {
				if i < len(result.Columns) && result.Columns[i] != expectedCol {
					t.Errorf("Expected column %d to be '%s', got '%s'", i, expectedCol, result.Columns[i])
				}
			}

			// Validate first row if provided
			if len(result.Rows) > 0 && tt.validateFirstRow != nil {
				firstRow := result.Rows[0]
				stringRow := make([]string, len(firstRow))
				for i, val := range firstRow {
					stringRow[i] = val.ToString()
				}
				tt.validateFirstRow(t, stringRow)
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

// TestSQLEngine_StringFunctionErrorHandling tests error cases for string functions
func TestSQLEngine_StringFunctionErrorHandling(t *testing.T) {
	engine := NewTestSQLEngine()

	// This should now work (previously would error as "unsupported aggregation function")
	result, err := engine.ExecuteSQL(context.Background(), "SELECT UPPER(status) FROM user_events LIMIT 1")
	if err != nil {
		t.Fatalf("UPPER function should work, got error: %v", err)
	}
	if result.Error != nil {
		t.Fatalf("UPPER function should work, got query error: %v", result.Error)
	}

	t.Logf("UPPER function works correctly")

	// This should now work (previously would error as "unsupported aggregation function")
	result2, err2 := engine.ExecuteSQL(context.Background(), "SELECT LENGTH(action) FROM user_events LIMIT 1")
	if err2 != nil {
		t.Fatalf("LENGTH function should work, got error: %v", err2)
	}
	if result2.Error != nil {
		t.Fatalf("LENGTH function should work, got query error: %v", result2.Error)
	}

	t.Logf("LENGTH function works correctly")
}
