package util

import (
	"strconv"
	"strings"
)

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

// goHeapHugePagesDisabled reports whether GODEBUG sets disablethp, which makes
// the runtime map the heap with MADV_NOHUGEPAGE. Like the runtime, the last
// disablethp with an integer value wins.
func goHeapHugePagesDisabled(godebug string) bool {
	disabled := false
	for _, setting := range strings.Split(godebug, ",") {
		key, value, ok := strings.Cut(setting, "=")
		if !ok || key != "disablethp" {
			continue
		}
		if n, err := strconv.ParseInt(value, 10, 32); err == nil {
			disabled = n != 0
		}
	}
	return disabled
}
