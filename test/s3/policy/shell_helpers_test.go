package policy

import (
	"strings"
	"testing"
)

// requireContains fails the test if substr is not found in output.
func requireContains(t *testing.T, output, substr, context string) {
	t.Helper()
	if !strings.Contains(output, substr) {
		t.Fatalf("%s: expected output to contain %q\n--- output ---\n%s\n--- end ---", context, substr, output)
	}
}

// requireNotContains fails the test if substr IS found in output.
func requireNotContains(t *testing.T, output, substr, context string) {
	t.Helper()
	if strings.Contains(output, substr) {
		t.Fatalf("%s: expected output to NOT contain %q\n--- output ---\n%s\n--- end ---", context, substr, output)
	}
}

// extractFieldAfter returns the first occurrence of the value after a "Prefix: " line.
// Example: extractFieldAfter(out, "Access Key:") -> "AKIAXXXX..."
// Returns "" if not found.
func extractFieldAfter(output, prefix string) string {
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}

// splitLines splits output into trimmed non-empty lines.
func splitLines(output string) []string {
	var lines []string
	for _, line := range strings.Split(output, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

// fieldsOf splits a line on whitespace.
func fieldsOf(line string) []string {
	return strings.Fields(line)
}

// extractServiceAccountID parses the tab-separated output of `s3.serviceaccount.list`
// and returns the ID of the first row whose PARENT column matches parentUser.
// The list output format is:
//
//	ID                                         PARENT     STATUS    DESCRIPTION
//	sa:user-yyy:a1b2c3d4e5f6...                user-yyy   enabled   some desc
func extractServiceAccountID(t *testing.T, listOutput, parentUser string) string {
	t.Helper()
	for _, line := range strings.Split(listOutput, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "ID") || strings.HasPrefix(line, "No service accounts") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == parentUser {
			return fields[0]
		}
	}
	t.Fatalf("could not find service account with parent=%q in output:\n%s", parentUser, listOutput)
	return ""
}
