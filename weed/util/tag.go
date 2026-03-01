package util

import "strings"

// NormalizeTagList normalizes a list of tags by converting to lowercase,
// trimming whitespace, removing duplicates, and filtering empty strings.
func NormalizeTagList(tags []string) []string {
	if len(tags) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(tags))
	seen := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" {
			continue
		}
		if _, exists := seen[tag]; exists {
			continue
		}
		seen[tag] = struct{}{}
		normalized = append(normalized, tag)
	}
	return normalized
}
