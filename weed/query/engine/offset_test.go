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
		// In clean mock environment, we only have 2 live_log rows from unflushed messages
		// OFFSET 3 exceeds available data, should return 0 rows
		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows with LIMIT 1 OFFSET 3 (exceeds available data), got %d", len(result.Rows))
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
