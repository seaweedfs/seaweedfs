package util

import "strings"

// ParseCSVSet splits a comma-separated string into a set of trimmed,
// non-empty values. Returns nil if the input is empty.
func ParseCSVSet(csv string) map[string]bool {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return nil
	}
	set := make(map[string]bool)
	for _, item := range strings.Split(csv, ",") {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			set[trimmed] = true
		}
	}
	return set
}
