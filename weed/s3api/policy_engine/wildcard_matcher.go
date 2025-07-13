package policy_engine

import (
	"regexp"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// WildcardMatcher provides unified wildcard matching functionality
type WildcardMatcher struct {
	// Use regex for complex patterns with ? wildcards
	// Use string manipulation for simple * patterns (better performance)
	useRegex bool
	regex    *regexp.Regexp
	pattern  string
}

// WildcardMatcherCache provides caching for WildcardMatcher instances
type WildcardMatcherCache struct {
	mu       sync.RWMutex
	matchers map[string]*WildcardMatcher
}

// Global cache instance
var wildcardMatcherCache = &WildcardMatcherCache{
	matchers: make(map[string]*WildcardMatcher),
}

// GetCachedWildcardMatcher gets or creates a cached WildcardMatcher for the given pattern
func GetCachedWildcardMatcher(pattern string) (*WildcardMatcher, error) {
	// Fast path: check if already in cache
	wildcardMatcherCache.mu.RLock()
	if matcher, exists := wildcardMatcherCache.matchers[pattern]; exists {
		wildcardMatcherCache.mu.RUnlock()
		return matcher, nil
	}
	wildcardMatcherCache.mu.RUnlock()

	// Slow path: create new matcher and cache it
	wildcardMatcherCache.mu.Lock()
	defer wildcardMatcherCache.mu.Unlock()

	// Double-check after acquiring write lock
	if matcher, exists := wildcardMatcherCache.matchers[pattern]; exists {
		return matcher, nil
	}

	// Create new matcher
	matcher, err := NewWildcardMatcher(pattern)
	if err != nil {
		return nil, err
	}

	// Cache it
	wildcardMatcherCache.matchers[pattern] = matcher
	return matcher, nil
}

// NewWildcardMatcher creates a new wildcard matcher for the given pattern
func NewWildcardMatcher(pattern string) (*WildcardMatcher, error) {
	matcher := &WildcardMatcher{
		pattern: pattern,
	}

	// Determine if we need regex (contains ? wildcards)
	if strings.Contains(pattern, "?") {
		matcher.useRegex = true
		regex, err := compileWildcardPattern(pattern)
		if err != nil {
			return nil, err
		}
		matcher.regex = regex
	} else {
		matcher.useRegex = false
	}

	return matcher, nil
}

// Match checks if a string matches the wildcard pattern
func (m *WildcardMatcher) Match(str string) bool {
	if m.useRegex {
		return m.regex.MatchString(str)
	}
	return matchWildcardString(m.pattern, str)
}

// MatchesWildcard provides a simple function interface for wildcard matching
// This function consolidates the logic from the previous separate implementations
func MatchesWildcard(pattern, str string) bool {
	// Handle simple cases first
	if pattern == "*" {
		return true
	}
	if pattern == str {
		return true
	}

	// Use regex for patterns with ? wildcards, string manipulation for * only
	if strings.Contains(pattern, "?") {
		return matchWildcardRegex(pattern, str)
	}
	return matchWildcardString(pattern, str)
}

// CompileWildcardPattern converts a wildcard pattern to a compiled regex
// This replaces the previous compilePattern function
func CompileWildcardPattern(pattern string) (*regexp.Regexp, error) {
	return compileWildcardPattern(pattern)
}

// matchWildcardString uses string manipulation for * wildcards only (more efficient)
func matchWildcardString(pattern, str string) bool {
	// Handle simple cases
	if pattern == "*" {
		return true
	}
	if pattern == str {
		return true
	}

	// Split pattern by wildcards
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		// No wildcards, exact match
		return pattern == str
	}

	// Check if string starts with first part
	if len(parts[0]) > 0 && !strings.HasPrefix(str, parts[0]) {
		return false
	}

	// Check if string ends with last part
	if len(parts[len(parts)-1]) > 0 && !strings.HasSuffix(str, parts[len(parts)-1]) {
		return false
	}

	// Check middle parts
	searchStr := str
	if len(parts[0]) > 0 {
		searchStr = searchStr[len(parts[0]):]
	}
	if len(parts[len(parts)-1]) > 0 {
		searchStr = searchStr[:len(searchStr)-len(parts[len(parts)-1])]
	}

	for i := 1; i < len(parts)-1; i++ {
		if len(parts[i]) > 0 {
			index := strings.Index(searchStr, parts[i])
			if index == -1 {
				return false
			}
			searchStr = searchStr[index+len(parts[i]):]
		}
	}

	return true
}

// matchWildcardRegex uses WildcardMatcher for patterns with ? wildcards
func matchWildcardRegex(pattern, str string) bool {
	matcher, err := GetCachedWildcardMatcher(pattern)
	if err != nil {
		glog.Errorf("Error getting WildcardMatcher for pattern %s: %v", pattern, err)
		return false
	}
	return matcher.Match(str)
}

// compileWildcardPattern converts a wildcard pattern to regex
func compileWildcardPattern(pattern string) (*regexp.Regexp, error) {
	// Escape special regex characters except * and ?
	escaped := regexp.QuoteMeta(pattern)

	// Replace escaped wildcards with regex equivalents
	escaped = strings.ReplaceAll(escaped, `\*`, `.*`)
	escaped = strings.ReplaceAll(escaped, `\?`, `.`)

	// Anchor the pattern
	escaped = "^" + escaped + "$"

	return regexp.Compile(escaped)
}
