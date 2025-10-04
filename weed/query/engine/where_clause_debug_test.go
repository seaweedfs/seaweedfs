package engine

import (
	"context"
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// TestWhereParsing tests if WHERE clauses are parsed correctly by CockroachDB parser
func TestWhereParsing(t *testing.T) {

	testCases := []struct {
		name        string
		sql         string
		expectError bool
		desc        string
	}{
		{
			name:        "Simple_Equals",
			sql:         "SELECT id FROM user_events WHERE id = 82460",
			expectError: false,
			desc:        "Simple equality WHERE clause",
		},
		{
			name:        "Greater_Than",
			sql:         "SELECT id FROM user_events WHERE id > 10000000",
			expectError: false,
			desc:        "Greater than WHERE clause",
		},
		{
			name:        "String_Equals",
			sql:         "SELECT id FROM user_events WHERE status = 'active'",
			expectError: false,
			desc:        "String equality WHERE clause",
		},
		{
			name:        "Impossible_Condition",
			sql:         "SELECT id FROM user_events WHERE 1 = 0",
			expectError: false,
			desc:        "Impossible WHERE condition (should parse but return no rows)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test parsing first
			parsedStmt, parseErr := ParseSQL(tc.sql)

			if tc.expectError {
				if parseErr == nil {
					t.Errorf("Expected parse error but got none for: %s", tc.desc)
				} else {
					t.Logf("PASS: Expected parse error: %v", parseErr)
				}
				return
			}

			if parseErr != nil {
				t.Errorf("Unexpected parse error for %s: %v", tc.desc, parseErr)
				return
			}

			// Check if it's a SELECT statement
			selectStmt, ok := parsedStmt.(*SelectStatement)
			if !ok {
				t.Errorf("Expected SelectStatement, got %T", parsedStmt)
				return
			}

			// Check if WHERE clause exists
			if selectStmt.Where == nil {
				t.Errorf("WHERE clause not parsed for: %s", tc.desc)
				return
			}

			t.Logf("PASS: WHERE clause parsed successfully for: %s", tc.desc)
			t.Logf("      WHERE expression type: %T", selectStmt.Where.Expr)
		})
	}
}

// TestPredicateBuilding tests if buildPredicate can handle CockroachDB AST nodes
func TestPredicateBuilding(t *testing.T) {
	engine := NewTestSQLEngine()

	testCases := []struct {
		name        string
		sql         string
		desc        string
		testRecord  *schema_pb.RecordValue
		shouldMatch bool
	}{
		{
			name:        "Simple_Equals_Match",
			sql:         "SELECT id FROM user_events WHERE id = 82460",
			desc:        "Simple equality - should match",
			testRecord:  createTestRecord("82460", "active"),
			shouldMatch: true,
		},
		{
			name:        "Simple_Equals_NoMatch",
			sql:         "SELECT id FROM user_events WHERE id = 82460",
			desc:        "Simple equality - should not match",
			testRecord:  createTestRecord("999999", "active"),
			shouldMatch: false,
		},
		{
			name:        "Greater_Than_Match",
			sql:         "SELECT id FROM user_events WHERE id > 100000",
			desc:        "Greater than - should match",
			testRecord:  createTestRecord("841256", "active"),
			shouldMatch: true,
		},
		{
			name:        "Greater_Than_NoMatch",
			sql:         "SELECT id FROM user_events WHERE id > 100000",
			desc:        "Greater than - should not match",
			testRecord:  createTestRecord("82460", "active"),
			shouldMatch: false,
		},
		{
			name:        "String_Equals_Match",
			sql:         "SELECT id FROM user_events WHERE status = 'active'",
			desc:        "String equality - should match",
			testRecord:  createTestRecord("82460", "active"),
			shouldMatch: true,
		},
		{
			name:        "String_Equals_NoMatch",
			sql:         "SELECT id FROM user_events WHERE status = 'active'",
			desc:        "String equality - should not match",
			testRecord:  createTestRecord("82460", "inactive"),
			shouldMatch: false,
		},
		{
			name:        "Impossible_Condition",
			sql:         "SELECT id FROM user_events WHERE 1 = 0",
			desc:        "Impossible condition - should never match",
			testRecord:  createTestRecord("82460", "active"),
			shouldMatch: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the SQL
			parsedStmt, parseErr := ParseSQL(tc.sql)
			if parseErr != nil {
				t.Fatalf("Parse error: %v", parseErr)
			}

			selectStmt, ok := parsedStmt.(*SelectStatement)
			if !ok || selectStmt.Where == nil {
				t.Fatalf("No WHERE clause found")
			}

			// Try to build the predicate
			predicate, buildErr := engine.buildPredicate(selectStmt.Where.Expr)
			if buildErr != nil {
				t.Errorf("PREDICATE BUILD ERROR: %v", buildErr)
				t.Errorf("This might be the root cause of WHERE clause not working!")
				t.Errorf("WHERE expression type: %T", selectStmt.Where.Expr)
				return
			}

			// Test the predicate against our test record
			actualMatch := predicate(tc.testRecord)

			if actualMatch == tc.shouldMatch {
				t.Logf("PASS: %s - Predicate worked correctly (match=%v)", tc.desc, actualMatch)
			} else {
				t.Errorf("FAIL: %s - Expected match=%v, got match=%v", tc.desc, tc.shouldMatch, actualMatch)
				t.Errorf("This confirms the predicate logic is incorrect!")
			}
		})
	}
}

// TestWhereClauseEndToEnd tests complete WHERE clause functionality
func TestWhereClauseEndToEnd(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Log("END-TO-END WHERE CLAUSE VALIDATION")
	t.Log("===================================")

	// Test 1: Baseline (no WHERE clause)
	baselineResult, err := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events")
	if err != nil {
		t.Fatalf("Baseline query failed: %v", err)
	}
	baselineCount := len(baselineResult.Rows)
	t.Logf("Baseline (no WHERE): %d rows", baselineCount)

	// Test 2: Impossible condition
	impossibleResult, err := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events WHERE 1 = 0")
	if err != nil {
		t.Fatalf("Impossible WHERE query failed: %v", err)
	}
	impossibleCount := len(impossibleResult.Rows)
	t.Logf("WHERE 1 = 0: %d rows", impossibleCount)

	// CRITICAL TEST: This should detect the WHERE clause bug
	if impossibleCount == baselineCount {
		t.Errorf("WHERE CLAUSE BUG CONFIRMED:")
		t.Errorf("   Impossible condition returned same row count as no WHERE clause")
		t.Errorf("   This proves WHERE filtering is not being applied")
	} else if impossibleCount == 0 {
		t.Logf("Impossible WHERE condition correctly returns 0 rows")
	}

	// Test 3: Specific ID filtering
	if baselineCount > 0 {
		firstId := baselineResult.Rows[0][0].ToString()
		specificResult, err := engine.ExecuteSQL(context.Background(),
			"SELECT id FROM user_events WHERE id = "+firstId)
		if err != nil {
			t.Fatalf("Specific ID WHERE query failed: %v", err)
		}
		specificCount := len(specificResult.Rows)
		t.Logf("WHERE id = %s: %d rows", firstId, specificCount)

		if specificCount == baselineCount {
			t.Errorf("WHERE clause bug: Specific ID filter returned all rows")
		} else if specificCount == 1 {
			t.Logf("Specific ID WHERE clause working correctly")
		} else {
			t.Logf("Unexpected: Specific ID returned %d rows", specificCount)
		}
	}

	// Test 4: Range filtering with actual data validation
	rangeResult, err := engine.ExecuteSQL(context.Background(), "SELECT id FROM user_events WHERE id > 10000000")
	if err != nil {
		t.Fatalf("Range WHERE query failed: %v", err)
	}
	rangeCount := len(rangeResult.Rows)
	t.Logf("WHERE id > 10000000: %d rows", rangeCount)

	// Check if the filtering actually worked by examining the data
	nonMatchingCount := 0
	for _, row := range rangeResult.Rows {
		idStr := row[0].ToString()
		if idVal, parseErr := strconv.ParseInt(idStr, 10, 64); parseErr == nil {
			if idVal <= 10000000 {
				nonMatchingCount++
			}
		}
	}

	if nonMatchingCount > 0 {
		t.Errorf("WHERE clause bug: %d rows have id <= 10,000,000 but should be filtered out", nonMatchingCount)
		t.Errorf("   Sample IDs that should be filtered: %v", getSampleIds(rangeResult, 3))
	} else {
		t.Logf("WHERE id > 10000000 correctly filtered results")
	}
}

// Helper function to create test records for predicate testing
func createTestRecord(id string, status string) *schema_pb.RecordValue {
	record := &schema_pb.RecordValue{
		Fields: make(map[string]*schema_pb.Value),
	}

	// Add id field (as int64)
	if idVal, err := strconv.ParseInt(id, 10, 64); err == nil {
		record.Fields["id"] = &schema_pb.Value{
			Kind: &schema_pb.Value_Int64Value{Int64Value: idVal},
		}
	} else {
		record.Fields["id"] = &schema_pb.Value{
			Kind: &schema_pb.Value_StringValue{StringValue: id},
		}
	}

	// Add status field (as string)
	record.Fields["status"] = &schema_pb.Value{
		Kind: &schema_pb.Value_StringValue{StringValue: status},
	}

	return record
}

// Helper function to get sample IDs from result
func getSampleIds(result *QueryResult, count int) []string {
	var ids []string
	for i := 0; i < count && i < len(result.Rows); i++ {
		ids = append(ids, result.Rows[i][0].ToString())
	}
	return ids
}

// TestSpecificWhereClauseBug reproduces the exact issue from real usage
func TestSpecificWhereClauseBug(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Log("REPRODUCING EXACT WHERE CLAUSE BUG")
	t.Log("==================================")

	// The exact query that was failing: WHERE id > 10000000
	sql := "SELECT id FROM user_events WHERE id > 10000000 LIMIT 10 OFFSET 5"
	result, err := engine.ExecuteSQL(context.Background(), sql)

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	t.Logf("Query: %s", sql)
	t.Logf("Returned %d rows:", len(result.Rows))

	// Check each returned ID
	bugDetected := false
	for i, row := range result.Rows {
		idStr := row[0].ToString()
		if idVal, parseErr := strconv.ParseInt(idStr, 10, 64); parseErr == nil {
			t.Logf("Row %d: id = %d", i+1, idVal)
			if idVal <= 10000000 {
				bugDetected = true
				t.Errorf("BUG: id %d should be filtered out (<= 10,000,000)", idVal)
			}
		}
	}

	if !bugDetected {
		t.Log("WHERE clause working correctly - all IDs > 10,000,000")
	} else {
		t.Error("WHERE clause bug confirmed: Returned IDs that should be filtered out")
	}
}
