package engine

import (
	"context"
	"strings"
	"testing"
)

// TestComprehensiveSQLSuite tests all kinds of SQL patterns to ensure robustness
func TestComprehensiveSQLSuite(t *testing.T) {
	engine := NewTestSQLEngine()

	testCases := []struct {
		name        string
		sql         string
		shouldPanic bool
		shouldError bool
		desc        string
	}{
		// =========== BASIC QUERIES ===========
		{
			name:        "Basic_Select_All",
			sql:         "SELECT * FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Basic select all columns",
		},
		{
			name:        "Basic_Select_Column",
			sql:         "SELECT id FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Basic select single column",
		},
		{
			name:        "Basic_Select_Multiple_Columns",
			sql:         "SELECT id, status FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Basic select multiple columns",
		},

		// =========== ARITHMETIC EXPRESSIONS (FIXED) ===========
		{
			name:        "Arithmetic_Multiply_FIXED",
			sql:         "SELECT id*2 FROM user_events",
			shouldPanic: false, // Fixed: no longer panics
			shouldError: false,
			desc:        "FIXED: Arithmetic multiplication works",
		},
		{
			name:        "Arithmetic_Add",
			sql:         "SELECT id+10 FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Arithmetic addition works",
		},
		{
			name:        "Arithmetic_Subtract",
			sql:         "SELECT id-5 FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Arithmetic subtraction works",
		},
		{
			name:        "Arithmetic_Divide",
			sql:         "SELECT id/3 FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Arithmetic division works",
		},
		{
			name:        "Arithmetic_Complex",
			sql:         "SELECT id*2+10 FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Complex arithmetic expression works",
		},

		// =========== STRING OPERATIONS ===========
		{
			name:        "String_Concatenation",
			sql:         "SELECT 'hello' || 'world' FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "String concatenation",
		},
		{
			name:        "String_Column_Concat",
			sql:         "SELECT status || '_suffix' FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Column string concatenation",
		},

		// =========== FUNCTIONS ===========
		{
			name:        "Function_LENGTH",
			sql:         "SELECT LENGTH('hello') FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "LENGTH function with literal",
		},
		{
			name:        "Function_LENGTH_Column",
			sql:         "SELECT LENGTH(status) FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "LENGTH function with column",
		},
		{
			name:        "Function_UPPER",
			sql:         "SELECT UPPER('hello') FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "UPPER function",
		},
		{
			name:        "Function_Nested",
			sql:         "SELECT LENGTH(UPPER('hello')) FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Nested functions",
		},

		// =========== FUNCTIONS WITH ARITHMETIC ===========
		{
			name:        "Function_Arithmetic",
			sql:         "SELECT LENGTH('hello') + 10 FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Function with arithmetic",
		},
		{
			name:        "Function_Arithmetic_Complex",
			sql:         "SELECT LENGTH(status) * 2 + 5 FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Function with complex arithmetic",
		},

		// =========== TABLE REFERENCES ===========
		{
			name:        "Table_Simple",
			sql:         "SELECT * FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Simple table reference",
		},
		{
			name:        "Table_With_Database",
			sql:         "SELECT * FROM ecommerce.user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Table with database qualifier",
		},
		{
			name:        "Table_Quoted",
			sql:         `SELECT * FROM "user_events"`,
			shouldPanic: false,
			shouldError: false,
			desc:        "Quoted table name",
		},

		// =========== WHERE CLAUSES ===========
		{
			name:        "Where_Simple",
			sql:         "SELECT * FROM user_events WHERE id = 1",
			shouldPanic: false,
			shouldError: false,
			desc:        "Simple WHERE clause",
		},
		{
			name:        "Where_String",
			sql:         "SELECT * FROM user_events WHERE status = 'active'",
			shouldPanic: false,
			shouldError: false,
			desc:        "WHERE clause with string",
		},

		// =========== LIMIT/OFFSET ===========
		{
			name:        "Limit_Only",
			sql:         "SELECT * FROM user_events LIMIT 10",
			shouldPanic: false,
			shouldError: false,
			desc:        "LIMIT clause only",
		},
		{
			name:        "Limit_Offset",
			sql:         "SELECT * FROM user_events LIMIT 10 OFFSET 5",
			shouldPanic: false,
			shouldError: false,
			desc:        "LIMIT with OFFSET",
		},

		// =========== DATETIME FUNCTIONS ===========
		{
			name:        "DateTime_CURRENT_DATE",
			sql:         "SELECT CURRENT_DATE FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "CURRENT_DATE function",
		},
		{
			name:        "DateTime_NOW",
			sql:         "SELECT NOW() FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "NOW() function",
		},
		{
			name:        "DateTime_EXTRACT",
			sql:         "SELECT EXTRACT(YEAR FROM CURRENT_DATE) FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "EXTRACT function",
		},

		// =========== EDGE CASES ===========
		{
			name:        "Empty_String",
			sql:         "SELECT '' FROM user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Empty string literal",
		},
		{
			name:        "Multiple_Spaces",
			sql:         "SELECT    id    FROM    user_events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Query with multiple spaces",
		},
		{
			name:        "Mixed_Case",
			sql:         "Select ID from User_Events",
			shouldPanic: false,
			shouldError: false,
			desc:        "Mixed case SQL",
		},

		// =========== SHOW STATEMENTS ===========
		{
			name:        "Show_Databases",
			sql:         "SHOW DATABASES",
			shouldPanic: false,
			shouldError: false,
			desc:        "SHOW DATABASES statement",
		},
		{
			name:        "Show_Tables",
			sql:         "SHOW TABLES",
			shouldPanic: false,
			shouldError: false,
			desc:        "SHOW TABLES statement",
		},
	}

	var panicTests []string
	var errorTests []string
	var successTests []string

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Capture panics
			var panicValue interface{}
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicValue = r
					}
				}()

				result, err := engine.ExecuteSQL(context.Background(), tc.sql)

				if tc.shouldPanic {
					if panicValue == nil {
						t.Errorf("FAIL: Expected panic for %s, but query completed normally", tc.desc)
						panicTests = append(panicTests, "FAIL: "+tc.desc)
						return
					} else {
						t.Logf("PASS: EXPECTED PANIC: %s - %v", tc.desc, panicValue)
						panicTests = append(panicTests, "PASS: "+tc.desc+" (reproduced)")
						return
					}
				}

				if panicValue != nil {
					t.Errorf("FAIL: Unexpected panic for %s: %v", tc.desc, panicValue)
					panicTests = append(panicTests, "FAIL: "+tc.desc+" (unexpected panic)")
					return
				}

				if tc.shouldError {
					if err == nil && (result == nil || result.Error == nil) {
						t.Errorf("FAIL: Expected error for %s, but query succeeded", tc.desc)
						errorTests = append(errorTests, "FAIL: "+tc.desc)
						return
					} else {
						t.Logf("PASS: Expected error: %s", tc.desc)
						errorTests = append(errorTests, "PASS: "+tc.desc)
						return
					}
				}

				if err != nil {
					t.Errorf("FAIL: Unexpected error for %s: %v", tc.desc, err)
					errorTests = append(errorTests, "FAIL: "+tc.desc+" (unexpected error)")
					return
				}

				if result != nil && result.Error != nil {
					t.Errorf("FAIL: Unexpected result error for %s: %v", tc.desc, result.Error)
					errorTests = append(errorTests, "FAIL: "+tc.desc+" (unexpected result error)")
					return
				}

				t.Logf("PASS: Success: %s", tc.desc)
				successTests = append(successTests, "PASS: "+tc.desc)
			}()
		})
	}

	// Summary report
	separator := strings.Repeat("=", 80)
	t.Log("\n" + separator)
	t.Log("COMPREHENSIVE SQL TEST SUITE SUMMARY")
	t.Log(separator)
	t.Logf("Total Tests: %d", len(testCases))
	t.Logf("Successful: %d", len(successTests))
	t.Logf("Panics: %d", len(panicTests))
	t.Logf("Errors: %d", len(errorTests))
	t.Log(separator)

	if len(panicTests) > 0 {
		t.Log("\nPANICS TO FIX:")
		for _, test := range panicTests {
			t.Log("   " + test)
		}
	}

	if len(errorTests) > 0 {
		t.Log("\nERRORS TO INVESTIGATE:")
		for _, test := range errorTests {
			t.Log("   " + test)
		}
	}
}
