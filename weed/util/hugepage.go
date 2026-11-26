package util

import "strings"

// parseTHPMode extracts the active mode from a sysfs transparent huge page
// knob. The kernel prints every supported mode and brackets the active one,
// for example "always [madvise] never".
func parseTHPMode(content string) string {
	for _, field := range strings.Fields(content) {
		if strings.HasPrefix(field, "[") && strings.HasSuffix(field, "]") {
			return strings.Trim(field, "[]")
		}
	}
	return ""
}

// parseTHPValue trims a plain sysfs value such as khugepaged/max_ptes_none.
func parseTHPValue(content string) string {
	return strings.TrimSpace(content)
}
