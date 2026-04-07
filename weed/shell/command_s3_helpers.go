package shell

import "strings"

// joinMax joins up to max strings with ", " and appends "..." if truncated.
func joinMax(items []string, max int) string {
	if len(items) <= max {
		return strings.Join(items, ", ")
	}
	return strings.Join(items[:max], ", ") + "..."
}
