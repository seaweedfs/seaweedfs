package sqlutil

import (
	"strings"
)

// SplitStatements splits a query string into individual SQL statements.
// This robust implementation handles SQL comments, quoted strings, and escaped characters.
//
// Features:
// - Handles single-line comments (-- comment)
// - Handles multi-line comments (/* comment */)
// - Properly escapes single quotes in strings ('don”t')
// - Properly escapes double quotes in identifiers ("column""name")
// - Ignores semicolons within quoted strings and comments
// - Returns clean, trimmed statements with empty statements filtered out
func SplitStatements(query string) []string {
	var statements []string
	var current strings.Builder

	query = strings.TrimSpace(query)
	if query == "" {
		return []string{}
	}

	runes := []rune(query)
	i := 0

	for i < len(runes) {
		char := runes[i]

		// Handle single-line comments (-- comment)
		if char == '-' && i+1 < len(runes) && runes[i+1] == '-' {
			// Skip the entire comment without including it in any statement
			for i < len(runes) && runes[i] != '\n' && runes[i] != '\r' {
				i++
			}
			// Skip the newline if present
			if i < len(runes) {
				i++
			}
			continue
		}

		// Handle multi-line comments (/* comment */)
		if char == '/' && i+1 < len(runes) && runes[i+1] == '*' {
			// Skip the /* opening
			i++
			i++

			// Skip to end of comment or end of input without including content
			for i < len(runes) {
				if runes[i] == '*' && i+1 < len(runes) && runes[i+1] == '/' {
					i++ // Skip the *
					i++ // Skip the /
					break
				}
				i++
			}
			continue
		}

		// Handle single-quoted strings
		if char == '\'' {
			current.WriteRune(char)
			i++

			for i < len(runes) {
				char = runes[i]
				current.WriteRune(char)

				if char == '\'' {
					// Check if it's an escaped quote
					if i+1 < len(runes) && runes[i+1] == '\'' {
						i++ // Skip the next quote (it's escaped)
						if i < len(runes) {
							current.WriteRune(runes[i])
						}
					} else {
						break // End of string
					}
				}
				i++
			}
			i++
			continue
		}

		// Handle double-quoted identifiers
		if char == '"' {
			current.WriteRune(char)
			i++

			for i < len(runes) {
				char = runes[i]
				current.WriteRune(char)

				if char == '"' {
					// Check if it's an escaped quote
					if i+1 < len(runes) && runes[i+1] == '"' {
						i++ // Skip the next quote (it's escaped)
						if i < len(runes) {
							current.WriteRune(runes[i])
						}
					} else {
						break // End of identifier
					}
				}
				i++
			}
			i++
			continue
		}

		// Handle semicolon (statement separator)
		if char == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		} else {
			current.WriteRune(char)
		}
		i++
	}

	// Add any remaining statement
	if current.Len() > 0 {
		stmt := strings.TrimSpace(current.String())
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	// If no statements found, return the original query as a single statement
	if len(statements) == 0 {
		return []string{strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";"))}
	}

	return statements
}
