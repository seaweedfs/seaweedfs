package s3lifecycle

import "strings"

// MatchesFilter checks if an object matches the rule's filter criteria
// (prefix, tags, and size constraints).
func MatchesFilter(rule Rule, obj ObjectInfo) bool {
	if !matchesPrefix(rule.Prefix, obj.Key) {
		return false
	}
	if !matchesTags(rule.FilterTags, obj.Tags) {
		return false
	}
	if !matchesSize(rule.FilterSizeGreaterThan, rule.FilterSizeLessThan, obj.Size) {
		return false
	}
	return true
}

// matchesPrefix returns true if the object key starts with the given prefix.
// An empty prefix matches all keys.
func matchesPrefix(prefix, key string) bool {
	if prefix == "" {
		return true
	}
	return strings.HasPrefix(key, prefix)
}

// matchesTags returns true if all rule tags are present in the object's tags
// with matching values. An empty or nil rule tag set matches all objects.
func matchesTags(ruleTags, objTags map[string]string) bool {
	if len(ruleTags) == 0 {
		return true
	}
	if len(objTags) == 0 {
		return false
	}
	for k, v := range ruleTags {
		if objVal, ok := objTags[k]; !ok || objVal != v {
			return false
		}
	}
	return true
}

// matchesSize returns true if the object's size falls within the specified
// bounds. Zero values mean no constraint on that side.
func matchesSize(greaterThan, lessThan, objSize int64) bool {
	if greaterThan > 0 && objSize <= greaterThan {
		return false
	}
	if lessThan > 0 && objSize >= lessThan {
		return false
	}
	return true
}
