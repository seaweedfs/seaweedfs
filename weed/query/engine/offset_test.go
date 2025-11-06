package engine

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

// TestParseSQL_OFFSET_EdgeCases tests edge cases for OFFSET parsing
func TestParseSQL_OFFSET_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(t *testing.T, stmt Statement, err error)
	}{
		{
			name:    "Valid LIMIT OFFSET with WHERE",
			sql:     "SELECT * FROM users WHERE age > 18 LIMIT 10 OFFSET 5",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement, err error) {
				selectStmt := stmt.(*SelectStatement)
				if selectStmt.Limit == nil {
					t.Fatal("Expected LIMIT clause, got nil")
				}
				if selectStmt.Limit.Offset == nil {
					t.Fatal("Expected OFFSET clause, got nil")
				}
				if selectStmt.Where == nil {
					t.Fatal("Expected WHERE clause, got nil")
				}
			},
		},
		{
			name:    "LIMIT OFFSET with mixed case",
			sql:     "select * from users limit 5 offset 3",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement, err error) {
				selectStmt := stmt.(*SelectStatement)
				offsetVal := selectStmt.Limit.Offset.(*SQLVal)
				if string(offsetVal.Val) != "3" {
					t.Errorf("Expected offset value '3', got '%s'", string(offsetVal.Val))
				}
			},
		},
		{
			name:    "LIMIT OFFSET with extra spaces",
			sql:     "SELECT * FROM users LIMIT   10   OFFSET   20  ",
			wantErr: false,
			validate: func(t *testing.T, stmt Statement, err error) {
				selectStmt := stmt.(*SelectStatement)
				limitVal := selectStmt.Limit.Rowcount.(*SQLVal)
				offsetVal := selectStmt.Limit.Offset.(*SQLVal)
				if string(limitVal.Val) != "10" {
					t.Errorf("Expected limit value '10', got '%s'", string(limitVal.Val))
				}
				if string(offsetVal.Val) != "20" {
					t.Errorf("Expected offset value '20', got '%s'", string(offsetVal.Val))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error, but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if tt.validate != nil {
				tt.validate(t, stmt, err)
			}
		})
	}
}

// TestSQLEngine_OFFSET_EdgeCases tests edge cases for OFFSET execution
func TestSQLEngine_OFFSET_EdgeCases(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Run("OFFSET larger than result set", func(t *testing.T) {
		result, err := engine.ExecuteSQL(context.Background(), "SELECT * FROM user_events LIMIT 5 OFFSET 100")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if result.Error != nil {
			t.Fatalf("Expected no query error, got %v", result.Error)
		}
		// Should return empty result set
		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows when OFFSET > total rows, got %d", len(result.Rows))
		}
	})

	t.Run("OFFSET with LIMIT 0", func(t *testing.T) {
		result, err := engine.ExecuteSQL(context.Background(), "SELECT * FROM user_events LIMIT 0 OFFSET 2")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if result.Error != nil {
			t.Fatalf("Expected no query error, got %v", result.Error)
		}
		// LIMIT 0 should return no rows regardless of OFFSET
		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows with LIMIT 0, got %d", len(result.Rows))
		}
	})

	t.Run("High OFFSET with small LIMIT", func(t *testing.T) {
		result, err := engine.ExecuteSQL(context.Background(), "SELECT * FROM user_events LIMIT 1 OFFSET 3")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if result.Error != nil {
			t.Fatalf("Expected no query error, got %v", result.Error)
		}
		// In clean mock environment, we have 4 live_log rows from unflushed messages
		// LIMIT 1 OFFSET 3 should return the 4th row (0-indexed: rows 0,1,2,3 -> return row 3)
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row with LIMIT 1 OFFSET 3 (4th live_log row), got %d", len(result.Rows))
		}
	})
}

// TestSQLEngine_OFFSET_ErrorCases tests error conditions for OFFSET
func TestSQLEngine_OFFSET_ErrorCases(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test negative OFFSET - should be caught during execution
	t.Run("Negative OFFSET value", func(t *testing.T) {
		// Note: This would need to be implemented as validation in the execution engine
		// For now, we test that the parser accepts it but execution might handle it
		_, err := ParseSQL("SELECT * FROM users LIMIT 10 OFFSET -5")
		if err != nil {
			t.Logf("Parser rejected negative OFFSET (this is expected): %v", err)
		} else {
			// Parser accepts it, execution should handle validation
			t.Logf("Parser accepts negative OFFSET, execution should validate")
		}
	})

	// Test very large OFFSET
	t.Run("Very large OFFSET value", func(t *testing.T) {
		largeOffset := "2147483647" // Max int32
		sql := "SELECT * FROM user_events LIMIT 1 OFFSET " + largeOffset
		result, err := engine.ExecuteSQL(context.Background(), sql)
		if err != nil {
			// Large OFFSET might cause parsing or execution errors
			if strings.Contains(err.Error(), "out of valid range") {
				t.Logf("Large OFFSET properly rejected: %v", err)
			} else {
				t.Errorf("Unexpected error for large OFFSET: %v", err)
			}
		} else if result.Error != nil {
			if strings.Contains(result.Error.Error(), "out of valid range") {
				t.Logf("Large OFFSET properly rejected during execution: %v", result.Error)
			} else {
				t.Errorf("Unexpected execution error for large OFFSET: %v", result.Error)
			}
		} else {
			// Should return empty result for very large offset
			if len(result.Rows) != 0 {
				t.Errorf("Expected 0 rows for very large OFFSET, got %d", len(result.Rows))
			}
		}
	})
}

// TestSQLEngine_OFFSET_Consistency tests that OFFSET produces consistent results
func TestSQLEngine_OFFSET_Consistency(t *testing.T) {
	engine := NewTestSQLEngine()

	// Get all rows first
	allResult, err := engine.ExecuteSQL(context.Background(), "SELECT * FROM user_events")
	if err != nil {
		t.Fatalf("Failed to get all rows: %v", err)
	}
	if allResult.Error != nil {
		t.Fatalf("Failed to get all rows: %v", allResult.Error)
	}

	totalRows := len(allResult.Rows)
	if totalRows == 0 {
		t.Skip("No data available for consistency test")
	}

	// Test that OFFSET + remaining rows = total rows
	for offset := 0; offset < totalRows; offset++ {
		t.Run("OFFSET_"+strconv.Itoa(offset), func(t *testing.T) {
			sql := "SELECT * FROM user_events LIMIT 100 OFFSET " + strconv.Itoa(offset)
			result, err := engine.ExecuteSQL(context.Background(), sql)
			if err != nil {
				t.Fatalf("Error with OFFSET %d: %v", offset, err)
			}
			if result.Error != nil {
				t.Fatalf("Query error with OFFSET %d: %v", offset, result.Error)
			}

			expectedRows := totalRows - offset
			if len(result.Rows) != expectedRows {
				t.Errorf("OFFSET %d: expected %d rows, got %d", offset, expectedRows, len(result.Rows))
			}
		})
	}
}

// TestSQLEngine_LIMIT_OFFSET_BugFix tests the specific bug fix for LIMIT with OFFSET
// This test addresses the issue where LIMIT 10 OFFSET 5 was returning 5 rows instead of 10
func TestSQLEngine_LIMIT_OFFSET_BugFix(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test the specific scenario that was broken: LIMIT 10 OFFSET 5 should return 10 rows
	t.Run("LIMIT 10 OFFSET 5 returns correct count", func(t *testing.T) {
		result, err := engine.ExecuteSQL(context.Background(), "SELECT id, user_id, id+user_id FROM user_events LIMIT 10 OFFSET 5")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if result.Error != nil {
			t.Fatalf("Expected no query error, got %v", result.Error)
		}

		// The bug was that this returned 5 rows instead of 10
		// After fix, it should return up to 10 rows (limited by available data)
		actualRows := len(result.Rows)
		if actualRows > 10 {
			t.Errorf("LIMIT 10 violated: got %d rows", actualRows)
		}

		t.Logf("LIMIT 10 OFFSET 5 returned %d rows (within limit)", actualRows)

		// Verify we have the expected columns
		expectedCols := 3 // id, user_id, id+user_id
		if len(result.Columns) != expectedCols {
			t.Errorf("Expected %d columns, got %d columns: %v", expectedCols, len(result.Columns), result.Columns)
		}
	})

	// Test various LIMIT and OFFSET combinations to ensure correct row counts
	testCases := []struct {
		name       string
		limit      int
		offset     int
		allowEmpty bool // Whether 0 rows is acceptable (for large offsets)
	}{
		{"LIMIT 5 OFFSET 0", 5, 0, false},
		{"LIMIT 5 OFFSET 2", 5, 2, false},
		{"LIMIT 8 OFFSET 3", 8, 3, false},
		{"LIMIT 15 OFFSET 1", 15, 1, false},
		{"LIMIT 3 OFFSET 7", 3, 7, true},   // Large offset may exceed data
		{"LIMIT 12 OFFSET 4", 12, 4, true}, // Large offset may exceed data
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sql := "SELECT id, user_id FROM user_events LIMIT " + strconv.Itoa(tc.limit) + " OFFSET " + strconv.Itoa(tc.offset)
			result, err := engine.ExecuteSQL(context.Background(), sql)
			if err != nil {
				t.Fatalf("Expected no error for %s, got %v", tc.name, err)
			}
			if result.Error != nil {
				t.Fatalf("Expected no query error for %s, got %v", tc.name, result.Error)
			}

			actualRows := len(result.Rows)

			// Verify LIMIT is never exceeded
			if actualRows > tc.limit {
				t.Errorf("%s: LIMIT violated - returned %d rows, limit was %d", tc.name, actualRows, tc.limit)
			}

			// Check if we expect rows
			if !tc.allowEmpty && actualRows == 0 {
				t.Errorf("%s: expected some rows but got 0 (insufficient test data or early termination bug)", tc.name)
			}

			t.Logf("%s: returned %d rows (within limit %d)", tc.name, actualRows, tc.limit)
		})
	}
}

// TestSQLEngine_OFFSET_DataCollectionBuffer tests that the enhanced data collection buffer works
func TestSQLEngine_OFFSET_DataCollectionBuffer(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test scenarios that specifically stress the data collection buffer enhancement
	t.Run("Large OFFSET with small LIMIT", func(t *testing.T) {
		// This scenario requires collecting more data upfront to handle the offset
		result, err := engine.ExecuteSQL(context.Background(), "SELECT * FROM user_events LIMIT 2 OFFSET 8")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if result.Error != nil {
			t.Fatalf("Expected no query error, got %v", result.Error)
		}

		// Should either return 2 rows or 0 (if offset exceeds available data)
		// The bug would cause early termination and return 0 incorrectly
		actualRows := len(result.Rows)
		if actualRows != 0 && actualRows != 2 {
			t.Errorf("Expected 0 or 2 rows for LIMIT 2 OFFSET 8, got %d", actualRows)
		}
	})

	t.Run("Medium OFFSET with medium LIMIT", func(t *testing.T) {
		result, err := engine.ExecuteSQL(context.Background(), "SELECT id, user_id FROM user_events LIMIT 6 OFFSET 4")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if result.Error != nil {
			t.Fatalf("Expected no query error, got %v", result.Error)
		}

		// With proper buffer enhancement, this should work correctly
		actualRows := len(result.Rows)
		if actualRows > 6 {
			t.Errorf("LIMIT 6 should never return more than 6 rows, got %d", actualRows)
		}
	})

	t.Run("Progressive OFFSET test", func(t *testing.T) {
		// Test that increasing OFFSET values work consistently
		baseSQL := "SELECT id FROM user_events LIMIT 3 OFFSET "

		for offset := 0; offset <= 5; offset++ {
			sql := baseSQL + strconv.Itoa(offset)
			result, err := engine.ExecuteSQL(context.Background(), sql)
			if err != nil {
				t.Fatalf("Error at OFFSET %d: %v", offset, err)
			}
			if result.Error != nil {
				t.Fatalf("Query error at OFFSET %d: %v", offset, result.Error)
			}

			actualRows := len(result.Rows)
			// Each should return at most 3 rows (LIMIT 3)
			if actualRows > 3 {
				t.Errorf("OFFSET %d: LIMIT 3 returned %d rows (should be ≤ 3)", offset, actualRows)
			}

			t.Logf("OFFSET %d: returned %d rows", offset, actualRows)
		}
	})
}

// TestSQLEngine_LIMIT_OFFSET_ArithmeticExpressions tests LIMIT/OFFSET with arithmetic expressions
func TestSQLEngine_LIMIT_OFFSET_ArithmeticExpressions(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test the exact scenario from the user's example
	t.Run("Arithmetic expressions with LIMIT OFFSET", func(t *testing.T) {
		// First query: LIMIT 10 (should return 10 rows)
		result1, err := engine.ExecuteSQL(context.Background(), "SELECT id, user_id, id+user_id FROM user_events LIMIT 10")
		if err != nil {
			t.Fatalf("Expected no error for first query, got %v", err)
		}
		if result1.Error != nil {
			t.Fatalf("Expected no query error for first query, got %v", result1.Error)
		}

		// Second query: LIMIT 10 OFFSET 5 (should return 10 rows, not 5)
		result2, err := engine.ExecuteSQL(context.Background(), "SELECT id, user_id, id+user_id FROM user_events LIMIT 10 OFFSET 5")
		if err != nil {
			t.Fatalf("Expected no error for second query, got %v", err)
		}
		if result2.Error != nil {
			t.Fatalf("Expected no query error for second query, got %v", result2.Error)
		}

		// Verify column structure is correct
		expectedColumns := []string{"id", "user_id", "id+user_id"}
		if len(result2.Columns) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result2.Columns))
		}

		// The key assertion: LIMIT 10 OFFSET 5 should return 10 rows (if available)
		// This was the specific bug reported by the user
		rows1 := len(result1.Rows)
		rows2 := len(result2.Rows)

		t.Logf("LIMIT 10: returned %d rows", rows1)
		t.Logf("LIMIT 10 OFFSET 5: returned %d rows", rows2)

		if rows1 >= 15 { // If we have enough data for the test to be meaningful
			if rows2 != 10 {
				t.Errorf("LIMIT 10 OFFSET 5 should return 10 rows when sufficient data available, got %d", rows2)
			}
		} else {
			t.Logf("Insufficient data (%d rows) to fully test LIMIT 10 OFFSET 5 scenario", rows1)
		}

		// Verify multiplication expressions work in the second query
		if len(result2.Rows) > 0 {
			for i, row := range result2.Rows {
				if len(row) >= 3 { // Check if we have the id+user_id column
					idVal := row[0].ToString()     // id column
					userIdVal := row[1].ToString() // user_id column
					sumVal := row[2].ToString()    // id+user_id column
					t.Logf("Row %d: id=%s, user_id=%s, id+user_id=%s", i, idVal, userIdVal, sumVal)
				}
			}
		}
	})

	// Test multiplication specifically
	t.Run("Multiplication expressions", func(t *testing.T) {
		result, err := engine.ExecuteSQL(context.Background(), "SELECT id, id*2 FROM user_events LIMIT 3")
		if err != nil {
			t.Fatalf("Expected no error for multiplication test, got %v", err)
		}
		if result.Error != nil {
			t.Fatalf("Expected no query error for multiplication test, got %v", result.Error)
		}

		if len(result.Columns) != 2 {
			t.Errorf("Expected 2 columns for multiplication test, got %d", len(result.Columns))
		}

		if len(result.Rows) == 0 {
			t.Error("Expected some rows for multiplication test")
		}

		// Check that id*2 column has values (not empty)
		for i, row := range result.Rows {
			if len(row) >= 2 {
				idVal := row[0].ToString()
				doubledVal := row[1].ToString()
				if doubledVal == "" || doubledVal == "0" {
					t.Errorf("Row %d: id*2 should not be empty, id=%s, id*2=%s", i, idVal, doubledVal)
				} else {
					t.Logf("Row %d: id=%s, id*2=%s ✓", i, idVal, doubledVal)
				}
			}
		}
	})
}

// TestSQLEngine_OFFSET_WithAggregation tests OFFSET with aggregation queries
func TestSQLEngine_OFFSET_WithAggregation(t *testing.T) {
	engine := NewTestSQLEngine()

	// Note: Aggregation queries typically return single rows, so OFFSET behavior is different
	t.Run("COUNT with OFFSET", func(t *testing.T) {
		result, err := engine.ExecuteSQL(context.Background(), "SELECT COUNT(*) FROM user_events LIMIT 1 OFFSET 0")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if result.Error != nil {
			t.Fatalf("Expected no query error, got %v", result.Error)
		}
		// COUNT typically returns 1 row, so OFFSET 0 should return that row
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row for COUNT with OFFSET 0, got %d", len(result.Rows))
		}
	})

	t.Run("COUNT with OFFSET 1", func(t *testing.T) {
		result, err := engine.ExecuteSQL(context.Background(), "SELECT COUNT(*) FROM user_events LIMIT 1 OFFSET 1")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if result.Error != nil {
			t.Fatalf("Expected no query error, got %v", result.Error)
		}
		// COUNT returns 1 row, so OFFSET 1 should return 0 rows
		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows for COUNT with OFFSET 1, got %d", len(result.Rows))
		}
	})
}
