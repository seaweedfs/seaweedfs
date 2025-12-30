package util

import (
	"strings"
)

// PathPrefixFilter provides filtering based on include and exclude path prefixes.
// When both include and exclude prefixes match a path, the deepest matching prefix wins.
// This enables fine-grained control like: exclude /buckets/legacy but include /buckets/legacy/important
type PathPrefixFilter struct {
	includePrefixes []string // normalized with trailing /
	excludePrefixes []string // normalized with trailing /
}

// NewPathPrefixFilter creates a new PathPrefixFilter from comma-separated include and exclude prefix strings.
// Each prefix is normalized to have a trailing slash for directory boundary matching.
// Invalid prefixes (empty or not starting with /) are skipped with a warning via the provided warn function.
func NewPathPrefixFilter(includePrefixes, excludePrefixes string, warn func(format string, args ...interface{})) *PathPrefixFilter {
	pf := &PathPrefixFilter{}

	pf.includePrefixes = parsePrefixes(includePrefixes, warn)
	pf.excludePrefixes = parsePrefixes(excludePrefixes, warn)

	return pf
}

// parsePrefixes parses a comma-separated list of prefixes and normalizes them.
func parsePrefixes(prefixList string, warn func(format string, args ...interface{})) []string {
	if prefixList == "" {
		return nil
	}

	var result []string
	for _, p := range strings.Split(prefixList, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !strings.HasPrefix(p, "/") {
			if warn != nil {
				warn("prefix %q does not start with '/', skipping", p)
			}
			continue
		}
		// Normalize: ensure trailing slash for directory boundary matching
		if !strings.HasSuffix(p, "/") {
			p = p + "/"
		}
		result = append(result, p)
	}
	return result
}

// HasFilters returns true if any include or exclude prefixes are configured.
func (pf *PathPrefixFilter) HasFilters() bool {
	return len(pf.includePrefixes) > 0 || len(pf.excludePrefixes) > 0
}

// ShouldInclude returns true if the path should be included based on the configured prefixes.
//
// Logic:
//   - If no filters are configured, include everything.
//   - Find the deepest matching prefix from either include or exclude list.
//   - If the deepest match is in includePrefixes, include the path.
//   - If the deepest match is in excludePrefixes, exclude the path.
//   - If no match is found and includePrefixes is non-empty, exclude (explicit include required).
//   - If no match is found and includePrefixes is empty, include (default allow with excludes).
func (pf *PathPrefixFilter) ShouldInclude(fullpath string) bool {
	if !pf.HasFilters() {
		return true
	}

	// Normalize path for matching
	checkPath := fullpath
	if !strings.HasSuffix(checkPath, "/") {
		checkPath = checkPath + "/"
	}

	// Find deepest matching prefix from each list
	includeMatch := findDeepestMatch(checkPath, pf.includePrefixes)
	excludeMatch := findDeepestMatch(checkPath, pf.excludePrefixes)

	// Determine result based on which match is deeper
	if includeMatch != "" && excludeMatch != "" {
		// Both matched - deeper prefix wins
		return len(includeMatch) >= len(excludeMatch)
	}

	if includeMatch != "" {
		return true
	}

	if excludeMatch != "" {
		return false
	}

	// No match found
	if len(pf.includePrefixes) > 0 {
		// If includes are specified, require explicit include
		return false
	}

	// Default: include if only excludes are specified
	return true
}

// findDeepestMatch finds the longest prefix that matches the path.
func findDeepestMatch(path string, prefixes []string) string {
	var deepest string
	for _, prefix := range prefixes {
		if strings.HasPrefix(path, prefix) {
			if len(prefix) > len(deepest) {
				deepest = prefix
			}
		}
	}
	return deepest
}

// GetIncludePrefixes returns the configured include prefixes.
func (pf *PathPrefixFilter) GetIncludePrefixes() []string {
	return pf.includePrefixes
}

// GetExcludePrefixes returns the configured exclude prefixes.
func (pf *PathPrefixFilter) GetExcludePrefixes() []string {
	return pf.excludePrefixes
}
