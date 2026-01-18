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
	mu          sync.RWMutex
	matchers    map[string]*WildcardMatcher
	maxSize     int
	accessOrder []string // For LRU eviction
}

// NewWildcardMatcherCache creates a new WildcardMatcherCache with a configurable maxSize
func NewWildcardMatcherCache(maxSize int) *WildcardMatcherCache {
	if maxSize <= 0 {
		maxSize = 1000 // Default value
	}
	return &WildcardMatcherCache{
		matchers: make(map[string]*WildcardMatcher),
		maxSize:  maxSize,
	}
}

// Global cache instance
var wildcardMatcherCache = NewWildcardMatcherCache(1000) // Default maxSize

// GetCachedWildcardMatcher gets or creates a cached WildcardMatcher for the given pattern
func GetCachedWildcardMatcher(pattern string) (*WildcardMatcher, error) {
	// Fast path: check if already in cache
	wildcardMatcherCache.mu.RLock()
	if matcher, exists := wildcardMatcherCache.matchers[pattern]; exists {
		wildcardMatcherCache.mu.RUnlock()
		wildcardMatcherCache.updateAccessOrder(pattern)
		return matcher, nil
	}
	wildcardMatcherCache.mu.RUnlock()

	// Slow path: create new matcher and cache it
	wildcardMatcherCache.mu.Lock()
	defer wildcardMatcherCache.mu.Unlock()

	// Double-check after acquiring write lock
	if matcher, exists := wildcardMatcherCache.matchers[pattern]; exists {
		wildcardMatcherCache.updateAccessOrderLocked(pattern)
		return matcher, nil
	}

	// Create new matcher
	matcher, err := NewWildcardMatcher(pattern)
	if err != nil {
		return nil, err
	}

	// Evict old entries if cache is full
	if len(wildcardMatcherCache.matchers) >= wildcardMatcherCache.maxSize {
		wildcardMatcherCache.evictLeastRecentlyUsed()
	}

	// Cache it
	wildcardMatcherCache.matchers[pattern] = matcher
	wildcardMatcherCache.accessOrder = append(wildcardMatcherCache.accessOrder, pattern)
	return matcher, nil
}

// updateAccessOrder updates the access order for LRU eviction (with read lock)
func (c *WildcardMatcherCache) updateAccessOrder(pattern string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updateAccessOrderLocked(pattern)
}

// updateAccessOrderLocked updates the access order for LRU eviction (without locking)
func (c *WildcardMatcherCache) updateAccessOrderLocked(pattern string) {
	// Remove pattern from its current position
	for i, p := range c.accessOrder {
		if p == pattern {
			c.accessOrder = append(c.accessOrder[:i], c.accessOrder[i+1:]...)
			break
		}
	}
	// Add pattern to the end (most recently used)
	c.accessOrder = append(c.accessOrder, pattern)
}

// evictLeastRecentlyUsed removes the least recently used pattern from the cache
func (c *WildcardMatcherCache) evictLeastRecentlyUsed() {
	if len(c.accessOrder) == 0 {
		return
	}

	// Remove the least recently used pattern (first in the list)
	lruPattern := c.accessOrder[0]
	c.accessOrder = c.accessOrder[1:]
	delete(c.matchers, lruPattern)
}

// ClearCache clears all cached patterns (useful for testing)
func (c *WildcardMatcherCache) ClearCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.matchers = make(map[string]*WildcardMatcher)
	c.accessOrder = c.accessOrder[:0]
}

// GetCacheStats returns cache statistics
func (c *WildcardMatcherCache) GetCacheStats() (size int, maxSize int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.matchers), c.maxSize
}

// NewWildcardMatcher creates a new wildcard matcher for the given pattern
// The matcher uses an efficient string-based algorithm that handles both * and ? wildcards
// without requiring regex compilation.
func NewWildcardMatcher(pattern string) (*WildcardMatcher, error) {
	matcher := &WildcardMatcher{
		pattern:  pattern,
		useRegex: false, // String-based matching now handles both * and ?
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
//
// Rules:
//   - '*' matches any sequence of characters (including empty string)
//   - '?' matches exactly one character (any character)
func MatchesWildcard(pattern, str string) bool {
	// matchWildcardString now handles both * and ? efficiently without regex
	return matchWildcardString(pattern, str)
}

// CompileWildcardPattern converts a wildcard pattern to a compiled regex
// This replaces the previous compilePattern function
func CompileWildcardPattern(pattern string) (*regexp.Regexp, error) {
	return compileWildcardPattern(pattern)
}

// matchWildcardString uses efficient string manipulation for * and ? wildcards
// This implementation uses a backtracking algorithm that handles both wildcard types
// without requiring regex compilation.
//
// Rules:
//   - '*' matches any sequence of characters (including empty string)
//   - '?' matches exactly one character (any character)
func matchWildcardString(pattern, str string) bool {
	// Handle simple cases
	if pattern == "*" {
		return true
	}
	if pattern == str {
		return true
	}

	targetIndex := 0
	patternIndex := 0

	// Index of the most recent '*' in the pattern (-1 if none)
	lastStarIndex := -1

	// Index in target where the last '*' started matching
	lastStarMatchIndex := 0

	for targetIndex < len(str) {
		switch {
		// Case 1: Current characters match directly or '?' matches any single character
		case patternIndex < len(pattern) &&
			(pattern[patternIndex] == '?' || pattern[patternIndex] == str[targetIndex]):

			targetIndex++
			patternIndex++

		// Case 2: Wildcard '*' found in pattern
		case patternIndex < len(pattern) &&
			pattern[patternIndex] == '*':

			lastStarIndex = patternIndex
			lastStarMatchIndex = targetIndex
			patternIndex++

		// Case 3: Previous '*' can absorb one more character
		case lastStarIndex != -1:

			patternIndex = lastStarIndex + 1
			lastStarMatchIndex++
			targetIndex = lastStarMatchIndex

		// Case 4: No match possible
		default:
			return false
		}
	}

	// Consume any trailing '*' in the pattern
	for patternIndex < len(pattern) && pattern[patternIndex] == '*' {
		patternIndex++
	}

	// Match is valid only if the entire pattern is consumed
	return patternIndex == len(pattern)
}

// matchWildcardRegex uses WildcardMatcher for patterns with ? wildcards
func matchWildcardRegex(pattern, str string) bool {
	matcher, err := GetCachedWildcardMatcher(pattern)
	if err != nil {
		glog.Errorf("Error getting WildcardMatcher for pattern %s: %v. Falling back to matchWildcardString.", pattern, err)
		// Fallback to matchWildcardString
		return matchWildcardString(pattern, str)
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
