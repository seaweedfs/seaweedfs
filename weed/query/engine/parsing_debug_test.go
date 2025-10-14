package engine

import (
	"fmt"
	"testing"
)

// TestBasicParsing tests basic SQL parsing
func TestBasicParsing(t *testing.T) {
	testCases := []string{
		"SELECT * FROM user_events",
		"SELECT id FROM user_events",
		"SELECT id FROM user_events WHERE id = 123",
		"SELECT id FROM user_events WHERE id > 123",
		"SELECT id FROM user_events WHERE status = 'active'",
	}

	for i, sql := range testCases {
		t.Run(fmt.Sprintf("Query_%d", i+1), func(t *testing.T) {
			t.Logf("Testing SQL: %s", sql)

			stmt, err := ParseSQL(sql)
			if err != nil {
				t.Errorf("Parse error: %v", err)
				return
			}

			t.Logf("Parsed statement type: %T", stmt)

			if selectStmt, ok := stmt.(*SelectStatement); ok {
				t.Logf("SelectStatement details:")
				t.Logf("  SelectExprs count: %d", len(selectStmt.SelectExprs))
				t.Logf("  From count: %d", len(selectStmt.From))
				t.Logf("  WHERE clause exists: %v", selectStmt.Where != nil)

				if selectStmt.Where != nil {
					t.Logf("  WHERE expression type: %T", selectStmt.Where.Expr)
				} else {
					t.Logf("  WHERE clause is NIL - this is the bug!")
				}
			} else {
				t.Errorf("Expected SelectStatement, got %T", stmt)
			}
		})
	}
}

// TestCockroachParserDirectly tests the CockroachDB parser directly
func TestCockroachParserDirectly(t *testing.T) {
	// Test if the issue is in our ParseSQL function or CockroachDB parser
	sql := "SELECT id FROM user_events WHERE id > 123"

	t.Logf("Testing CockroachDB parser directly with: %s", sql)

	// First test our ParseSQL function
	stmt, err := ParseSQL(sql)
	if err != nil {
		t.Fatalf("Our ParseSQL failed: %v", err)
	}

	t.Logf("Our ParseSQL returned: %T", stmt)

	if selectStmt, ok := stmt.(*SelectStatement); ok {
		if selectStmt.Where == nil {
			t.Errorf("Our ParseSQL is not extracting WHERE clauses!")
			t.Errorf("This means the issue is in our CockroachDB AST conversion")
		} else {
			t.Logf("Our ParseSQL extracted WHERE clause: %T", selectStmt.Where.Expr)
		}
	}
}

// TestParseMethodComparison tests different parsing paths
func TestParseMethodComparison(t *testing.T) {
	sql := "SELECT id FROM user_events WHERE id > 123"

	t.Logf("Comparing parsing methods for: %s", sql)

	// Test 1: Our global ParseSQL function
	stmt1, err1 := ParseSQL(sql)
	t.Logf("Global ParseSQL: %T, error: %v", stmt1, err1)

	if selectStmt, ok := stmt1.(*SelectStatement); ok {
		t.Logf("  WHERE clause: %v", selectStmt.Where != nil)
	}

	// Test 2: Check if we have different parsing paths
	// This will help identify if the issue is in our custom parser vs CockroachDB parser

	engine := NewTestSQLEngine()
	_, err2 := engine.ExecuteSQL(nil, sql)
	t.Logf("ExecuteSQL error (helps identify parsing path): %v", err2)
}
