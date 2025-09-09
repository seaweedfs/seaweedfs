package engine

import (
	"context"
	"testing"
)

// TestArithmeticWithFunctions tests arithmetic operations with function calls
// This validates the complete AST parser and evaluation system for column-level calculations
func TestArithmeticWithFunctions(t *testing.T) {
	engine := NewTestSQLEngine()

	testCases := []struct {
		name     string
		sql      string
		expected string
		desc     string
	}{
		{
			name:     "Simple function arithmetic",
			sql:      "SELECT LENGTH('hello') + 10 FROM user_events LIMIT 1",
			expected: "15",
			desc:     "Basic function call with addition",
		},
		{
			name:     "Nested functions with arithmetic",
			sql:      "SELECT length(trim('  hello world  ')) + 12 FROM user_events LIMIT 1",
			expected: "23",
			desc:     "Complex nested functions with arithmetic operation (user's original failing query)",
		},
		{
			name:     "Function subtraction",
			sql:      "SELECT LENGTH('programming') - 5 FROM user_events LIMIT 1",
			expected: "6",
			desc:     "Function call with subtraction",
		},
		{
			name:     "Function multiplication",
			sql:      "SELECT LENGTH('test') * 3 FROM user_events LIMIT 1",
			expected: "12",
			desc:     "Function call with multiplication",
		},
		{
			name:     "Multiple nested functions",
			sql:      "SELECT LENGTH(UPPER(TRIM('  hello  '))) FROM user_events LIMIT 1",
			expected: "5",
			desc:     "Triple nested functions",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := engine.ExecuteSQL(context.Background(), tc.sql)

			if err != nil {
				t.Errorf("Query failed: %v", err)
				return
			}

			if result.Error != nil {
				t.Errorf("Query result error: %v", result.Error)
				return
			}

			if len(result.Rows) == 0 {
				t.Error("Expected at least one row")
				return
			}

			actual := result.Rows[0][0].ToString()

			if actual != tc.expected {
				t.Errorf("%s: Expected '%s', got '%s'", tc.desc, tc.expected, actual)
			} else {
				t.Logf("PASS %s: %s → %s", tc.desc, tc.sql, actual)
			}
		})
	}
}
