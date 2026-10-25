package wildcard

import "strings"

// CompileWildcardMatchers parses comma-separated wildcard patterns and compiles them.
// Empty tokens are ignored. Invalid patterns are skipped.
func CompileWildcardMatchers(filter string) []*WildcardMatcher {
	parts := strings.Split(filter, ",")
	matchers := make([]*WildcardMatcher, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		matcher, err := NewWildcardMatcher(trimmed)
		if err != nil {
			continue
		}
		matchers = append(matchers, matcher)
	}
	if len(matchers) == 0 {
		return nil
	}
	return matchers
}

// MatchesAnyWildcard returns true when no matcher is provided,
// or when any matcher matches the given value.
func MatchesAnyWildcard(matchers []*WildcardMatcher, value string) bool {
	if len(matchers) == 0 {
		return true
	}
	for _, matcher := range matchers {
		if matcher != nil && matcher.Match(value) {
			return true
		}
	}
	return false
}
