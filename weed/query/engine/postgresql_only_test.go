package engine

import (
	"context"
	"strings"
	"testing"
)

// TestPostgreSQLOnlySupport ensures that non-PostgreSQL syntax is properly rejected
func TestPostgreSQLOnlySupport(t *testing.T) {
	engine := NewTestSQLEngine()

	testCases := []struct {
		name        string
		sql         string
		shouldError bool
		errorMsg    string
		desc        string
	}{
		// Test that MySQL backticks are not supported for identifiers
		{
			name:        "MySQL_Backticks_Table",
			sql:         "SELECT * FROM `user_events` LIMIT 1",
			shouldError: true,
			desc:        "MySQL backticks for table names should be rejected",
		},
		{
			name:        "MySQL_Backticks_Column",
			sql:         "SELECT `column_name` FROM user_events LIMIT 1",
			shouldError: true,
			desc:        "MySQL backticks for column names should be rejected",
		},

		// Test that PostgreSQL double quotes work (should NOT error)
		{
			name:        "PostgreSQL_Double_Quotes_OK",
			sql:         `SELECT "user_id" FROM user_events LIMIT 1`,
			shouldError: false,
			desc:        "PostgreSQL double quotes for identifiers should work",
		},

		// Note: MySQL functions like YEAR(), MONTH() may parse but won't have proper implementations
		// They're removed from the engine so they won't work correctly, but we don't explicitly reject them

		// Test that PostgreSQL EXTRACT works (should NOT error)
		{
			name:        "PostgreSQL_EXTRACT_OK",
			sql:         "SELECT EXTRACT(YEAR FROM CURRENT_DATE) FROM user_events LIMIT 1",
			shouldError: false,
			desc:        "PostgreSQL EXTRACT function should work",
		},

		// Test that single quotes work for string literals but not identifiers
		{
			name:        "Single_Quotes_String_Literal_OK",
			sql:         "SELECT 'hello world' FROM user_events LIMIT 1",
			shouldError: false,
			desc:        "Single quotes for string literals should work",
		},
	}

	passCount := 0
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := engine.ExecuteSQL(context.Background(), tc.sql)

			if tc.shouldError {
				// We expect this query to fail
				if err == nil && result.Error == nil {
					t.Errorf("Expected error for %s, but query succeeded", tc.desc)
					return
				}

				// Check for specific error message if provided
				if tc.errorMsg != "" {
					errorText := ""
					if err != nil {
						errorText = err.Error()
					} else if result.Error != nil {
						errorText = result.Error.Error()
					}

					if !strings.Contains(errorText, tc.errorMsg) {
						t.Errorf("Expected error containing '%s', got: %s", tc.errorMsg, errorText)
						return
					}
				}

				t.Logf("CORRECTLY REJECTED: %s", tc.desc)
				passCount++
			} else {
				// We expect this query to succeed
				if err != nil {
					t.Errorf("Unexpected error for %s: %v", tc.desc, err)
					return
				}

				if result.Error != nil {
					t.Errorf("Unexpected result error for %s: %v", tc.desc, result.Error)
					return
				}

				t.Logf("CORRECTLY ACCEPTED: %s", tc.desc)
				passCount++
			}
		})
	}

	t.Logf("PostgreSQL-only compliance: %d/%d tests passed", passCount, len(testCases))
}
