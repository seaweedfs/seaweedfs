package sqlutil

import (
	"reflect"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Simple single statement",
			input:    "SELECT * FROM users",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "Multiple statements",
			input:    "SELECT * FROM users; SELECT * FROM orders;",
			expected: []string{"SELECT * FROM users", "SELECT * FROM orders"},
		},
		{
			name:     "Semicolon in single quotes",
			input:    "SELECT 'hello;world' FROM users; SELECT * FROM orders;",
			expected: []string{"SELECT 'hello;world' FROM users", "SELECT * FROM orders"},
		},
		{
			name:     "Semicolon in double quotes",
			input:    `SELECT "column;name" FROM users; SELECT * FROM orders;`,
			expected: []string{`SELECT "column;name" FROM users`, "SELECT * FROM orders"},
		},
		{
			name:     "Escaped quotes in strings",
			input:    `SELECT 'don''t split; here' FROM users; SELECT * FROM orders;`,
			expected: []string{`SELECT 'don''t split; here' FROM users`, "SELECT * FROM orders"},
		},
		{
			name:     "Escaped quotes in identifiers",
			input:    `SELECT "column""name" FROM users; SELECT * FROM orders;`,
			expected: []string{`SELECT "column""name" FROM users`, "SELECT * FROM orders"},
		},
		{
			name:     "Single line comment",
			input:    "SELECT * FROM users; -- This is a comment\nSELECT * FROM orders;",
			expected: []string{"SELECT * FROM users", "SELECT * FROM orders"},
		},
		{
			name:     "Single line comment with semicolon",
			input:    "SELECT * FROM users; -- Comment with; semicolon\nSELECT * FROM orders;",
			expected: []string{"SELECT * FROM users", "SELECT * FROM orders"},
		},
		{
			name:     "Multi-line comment",
			input:    "SELECT * FROM users; /* Multi-line\ncomment */ SELECT * FROM orders;",
			expected: []string{"SELECT * FROM users", "SELECT * FROM orders"},
		},
		{
			name:     "Multi-line comment with semicolon",
			input:    "SELECT * FROM users; /* Comment with; semicolon */ SELECT * FROM orders;",
			expected: []string{"SELECT * FROM users", "SELECT * FROM orders"},
		},
		{
			name: "Complex mixed case",
			input: `SELECT 'test;string', "quoted;id" FROM users; -- Comment; here
			/* Another; comment */ 
			INSERT INTO users VALUES ('name''s value', "id""field");`,
			expected: []string{
				`SELECT 'test;string', "quoted;id" FROM users`,
				`INSERT INTO users VALUES ('name''s value', "id""field")`,
			},
		},
		{
			name:     "Empty statements filtered",
			input:    "SELECT * FROM users;;; SELECT * FROM orders;",
			expected: []string{"SELECT * FROM users", "SELECT * FROM orders"},
		},
		{
			name:     "Whitespace handling",
			input:    "  SELECT * FROM users  ;  SELECT * FROM orders  ;  ",
			expected: []string{"SELECT * FROM users", "SELECT * FROM orders"},
		},
		{
			name:     "Single statement without semicolon",
			input:    "SELECT * FROM users",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "Empty query",
			input:    "",
			expected: []string{},
		},
		{
			name:     "Only whitespace",
			input:    "   \n\t   ",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SplitStatements(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("SplitStatements() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestSplitStatements_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Nested comments are not supported but handled gracefully",
			input:    "SELECT * FROM users; /* Outer /* inner */ comment */ SELECT * FROM orders;",
			expected: []string{"SELECT * FROM users", "comment */ SELECT * FROM orders"},
		},
		{
			name:     "Unterminated string (malformed SQL)",
			input:    "SELECT 'unterminated string; SELECT * FROM orders;",
			expected: []string{"SELECT 'unterminated string; SELECT * FROM orders;"},
		},
		{
			name:     "Unterminated comment (malformed SQL)",
			input:    "SELECT * FROM users; /* unterminated comment",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "Multiple semicolons in quotes",
			input:    "SELECT ';;;' FROM users; SELECT ';;;' FROM orders;",
			expected: []string{"SELECT ';;;' FROM users", "SELECT ';;;' FROM orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SplitStatements(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("SplitStatements() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
