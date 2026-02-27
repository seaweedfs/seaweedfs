package wildcard

import (
	"regexp"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// WildcardMatcher provides unified wildcard matching functionality.
type WildcardMatcher struct {
	useRegex bool
	regex    *regexp.Regexp
	pattern  string
}

// WildcardMatcherCache provides caching for WildcardMatcher instances.
type WildcardMatcherCache struct {
	mu          sync.RWMutex
	matchers    map[string]*WildcardMatcher
	maxSize     int
	accessOrder []string
}

// NewWildcardMatcherCache creates a new WildcardMatcherCache with a configurable maxSize.
func NewWildcardMatcherCache(maxSize int) *WildcardMatcherCache {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &WildcardMatcherCache{
		matchers: make(map[string]*WildcardMatcher),
		maxSize:  maxSize,
	}
}

var wildcardMatcherCache = NewWildcardMatcherCache(1000)

// GetCachedWildcardMatcher gets or creates a cached WildcardMatcher for the given pattern.
func GetCachedWildcardMatcher(pattern string) (*WildcardMatcher, error) {
	wildcardMatcherCache.mu.RLock()
	if matcher, exists := wildcardMatcherCache.matchers[pattern]; exists {
		wildcardMatcherCache.mu.RUnlock()
		wildcardMatcherCache.updateAccessOrder(pattern)
		return matcher, nil
	}
	wildcardMatcherCache.mu.RUnlock()

	wildcardMatcherCache.mu.Lock()
	defer wildcardMatcherCache.mu.Unlock()

	if matcher, exists := wildcardMatcherCache.matchers[pattern]; exists {
		wildcardMatcherCache.updateAccessOrderLocked(pattern)
		return matcher, nil
	}

	matcher, err := NewWildcardMatcher(pattern)
	if err != nil {
		return nil, err
	}

	if len(wildcardMatcherCache.matchers) >= wildcardMatcherCache.maxSize {
		wildcardMatcherCache.evictLeastRecentlyUsed()
	}

	wildcardMatcherCache.matchers[pattern] = matcher
	wildcardMatcherCache.accessOrder = append(wildcardMatcherCache.accessOrder, pattern)
	return matcher, nil
}

func (c *WildcardMatcherCache) updateAccessOrder(pattern string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updateAccessOrderLocked(pattern)
}

func (c *WildcardMatcherCache) updateAccessOrderLocked(pattern string) {
	for i, p := range c.accessOrder {
		if p == pattern {
			c.accessOrder = append(c.accessOrder[:i], c.accessOrder[i+1:]...)
			break
		}
	}
	c.accessOrder = append(c.accessOrder, pattern)
}

func (c *WildcardMatcherCache) evictLeastRecentlyUsed() {
	if len(c.accessOrder) == 0 {
		return
	}
	lruPattern := c.accessOrder[0]
	c.accessOrder = c.accessOrder[1:]
	delete(c.matchers, lruPattern)
}

func (c *WildcardMatcherCache) ClearCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.matchers = make(map[string]*WildcardMatcher)
	c.accessOrder = c.accessOrder[:0]
}

func (c *WildcardMatcherCache) GetCacheStats() (size int, maxSize int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.matchers), c.maxSize
}

// NewWildcardMatcher creates a new wildcard matcher for the given pattern.
func NewWildcardMatcher(pattern string) (*WildcardMatcher, error) {
	matcher := &WildcardMatcher{pattern: pattern, useRegex: false}
	return matcher, nil
}

// Match checks if a string matches the wildcard pattern.
func (m *WildcardMatcher) Match(str string) bool {
	if m.useRegex {
		return m.regex.MatchString(str)
	}
	return matchWildcardString(m.pattern, str)
}

// MatchesWildcard provides a simple function interface for wildcard matching.
func MatchesWildcard(pattern, str string) bool {
	return matchWildcardString(pattern, str)
}

// FastMatchesWildcard uses a cached WildcardMatcher for repeated pattern matching.
func FastMatchesWildcard(pattern, str string) bool {
	matcher, err := GetCachedWildcardMatcher(pattern)
	if err != nil {
		glog.Errorf("Error getting cached WildcardMatcher for pattern %s: %v. Falling back to MatchesWildcard.", pattern, err)
		return MatchesWildcard(pattern, str)
	}
	return matcher.Match(str)
}

// CompileWildcardPattern converts a wildcard pattern to a compiled regex.
func CompileWildcardPattern(pattern string) (*regexp.Regexp, error) {
	return compileWildcardPattern(pattern)
}

func matchWildcardString(pattern, str string) bool {
	if pattern == "*" {
		return true
	}
	if pattern == str {
		return true
	}

	targetIndex := 0
	patternIndex := 0
	lastStarIndex := -1
	lastStarMatchIndex := 0

	for targetIndex < len(str) {
		switch {
		case patternIndex < len(pattern) && (pattern[patternIndex] == '?' || pattern[patternIndex] == str[targetIndex]):
			targetIndex++
			patternIndex++
		case patternIndex < len(pattern) && pattern[patternIndex] == '*':
			lastStarIndex = patternIndex
			lastStarMatchIndex = targetIndex
			patternIndex++
		case lastStarIndex != -1:
			patternIndex = lastStarIndex + 1
			lastStarMatchIndex++
			targetIndex = lastStarMatchIndex
		default:
			return false
		}
	}

	for patternIndex < len(pattern) && pattern[patternIndex] == '*' {
		patternIndex++
	}
	return patternIndex == len(pattern)
}

func matchWildcardRegex(pattern, str string) bool {
	matcher, err := GetCachedWildcardMatcher(pattern)
	if err != nil {
		glog.Errorf("Error getting WildcardMatcher for pattern %s: %v. Falling back to matchWildcardString.", pattern, err)
		return matchWildcardString(pattern, str)
	}
	return matcher.Match(str)
}

func compileWildcardPattern(pattern string) (*regexp.Regexp, error) {
	escaped := regexp.QuoteMeta(pattern)
	escaped = strings.ReplaceAll(escaped, `\*`, `.*`)
	escaped = strings.ReplaceAll(escaped, `\?`, `.`)
	escaped = "^" + escaped + "$"
	return regexp.Compile(escaped)
}
