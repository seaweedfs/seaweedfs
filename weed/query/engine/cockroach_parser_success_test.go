package engine

import (
	"context"
	"testing"
)

// TestCockroachDBParserSuccess demonstrates the successful integration of CockroachDB's parser
// This test validates that all previously problematic SQL expressions now work correctly
func TestCockroachDBParserSuccess(t *testing.T) {
	engine := NewTestSQLEngine()

	testCases := []struct {
		name     string
		sql      string
		expected string
		desc     string
	}{
		{
			name:     "Basic_Function",
			sql:      "SELECT LENGTH('hello') FROM user_events LIMIT 1",
			expected: "5",
			desc:     "Simple function call",
		},
		{
			name:     "Function_Arithmetic",
			sql:      "SELECT LENGTH('hello') + 10 FROM user_events LIMIT 1",
			expected: "15",
			desc:     "Function with arithmetic operation (original user issue)",
		},
		{
			name:     "User_Original_Query",
			sql:      "SELECT length(trim('  hello world  ')) + 12 FROM user_events LIMIT 1",
			expected: "23",
			desc:     "User's exact original failing query - now fixed!",
		},
		{
			name:     "String_Concatenation",
			sql:      "SELECT 'hello' || 'world' FROM user_events LIMIT 1",
			expected: "helloworld",
			desc:     "Basic string concatenation",
		},
		{
			name:     "Function_With_Concat",
			sql:      "SELECT LENGTH('hello' || 'world') FROM user_events LIMIT 1",
			expected: "10",
			desc:     "Function with string concatenation argument",
		},
		{
			name:     "Multiple_Arithmetic",
			sql:      "SELECT LENGTH('test') * 3 FROM user_events LIMIT 1",
			expected: "12",
			desc:     "Function with multiplication",
		},
		{
			name:     "Nested_Functions",
			sql:      "SELECT LENGTH(UPPER('hello')) FROM user_events LIMIT 1",
			expected: "5",
			desc:     "Nested function calls",
		},
		{
			name:     "Column_Alias",
			sql:      "SELECT LENGTH('test') AS test_length FROM user_events LIMIT 1",
			expected: "4",
			desc:     "Column alias functionality (AS keyword)",
		},
	}

	successCount := 0

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := engine.ExecuteSQL(context.Background(), tc.sql)

			if err != nil {
				t.Errorf("%s - Query failed: %v", tc.desc, err)
				return
			}

			if result.Error != nil {
				t.Errorf("%s - Query result error: %v", tc.desc, result.Error)
				return
			}

			if len(result.Rows) == 0 {
				t.Errorf("%s - Expected at least one row", tc.desc)
				return
			}

			actual := result.Rows[0][0].ToString()

			if actual == tc.expected {
				t.Logf("SUCCESS: %s → %s", tc.desc, actual)
				successCount++
			} else {
				t.Errorf("FAIL %s - Expected '%s', got '%s'", tc.desc, tc.expected, actual)
			}
		})
	}

	t.Logf("CockroachDB Parser Integration: %d/%d tests passed!", successCount, len(testCases))
}
