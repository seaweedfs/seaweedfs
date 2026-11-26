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

// anonHugePagesAlways reports whether the host gives huge pages to anonymous
// memory that was not madvised, which is how the runtime maps the heap (it
// advises only GC metadata). Since Linux 6.8 each size has its own policy and
// only "inherit" follows the top-level one; older kernels have none. An empty
// size mode is one that could not be read, so known is false when it alone
// could have allowed huge pages.
func anonHugePagesAlways(enabled string, sizeModes []string) (always, known bool) {
	if len(sizeModes) == 0 {
		return enabled == "always", true
	}
	known = true
	for _, mode := range sizeModes {
		if mode == "always" || mode == "inherit" && enabled == "always" {
			return true, true
		}
		if mode == "" {
			known = false
		}
	}
	return false, known
}
