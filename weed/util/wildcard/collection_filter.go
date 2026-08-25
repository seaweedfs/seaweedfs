package wildcard

import (
	"fmt"
	"regexp"
	"strings"
)

// CollectionFilterMode controls how collections are interpreted during
// detection. The two recognized sentinels short-circuit matching:
//   - CollectionFilterAll: pool every collection together (default).
//   - CollectionFilterEach: run detection separately per collection.
//
// Any other non-empty value is a comma-separated list of collection patterns.
type CollectionFilterMode string

const (
	CollectionFilterAll  CollectionFilterMode = "ALL_COLLECTIONS"
	CollectionFilterEach CollectionFilterMode = "EACH_COLLECTION"

	// CollectionDefault is the entry that matches the empty-named collection.
	// "_default" avoids colliding with a collection literally named "default".
	CollectionDefault = "_default"
)

// regexMetaCharacters mark a filter entry as a regex; "*" and "?" stay wildcards.
// "." is not one of them: a collection named "my.bucket" is a name, not a
// pattern that would also match "my-bucket". A "." only counts as regex syntax
// when it is quantified, as in "bucket.*".
const regexMetaCharacters = `^$+()[]{}|\`

// CollectionMatcher matches a volume collection against a collection_filter value.
type CollectionMatcher struct {
	filter     string
	matchEmpty bool
	literals   []string
	wildcards  []string
	regexes    []*regexp.Regexp
}

// CompileCollectionMatcher parses a collection_filter into a matcher. A nil
// matcher accepts every collection: that is the empty filter, "*", and both
// mode sentinels. Any other value is a comma-separated list, and a collection
// passes when one entry matches it. An entry is a name, optionally with "*" and
// "?" wildcards, CollectionDefault for the empty-named collection, or a regex
// when it carries regex syntax. A regex entry must match the whole name unless
// it anchors itself with "^" or "$", and always matches its own spelling too,
// so a collection named "logs(2024)" stays reachable by name.
func CompileCollectionMatcher(filter string) (*CollectionMatcher, error) {
	trimmed := strings.TrimSpace(filter)
	mode := CollectionFilterMode(trimmed)
	if trimmed == "" || trimmed == "*" || mode == CollectionFilterAll || mode == CollectionFilterEach {
		return nil, nil
	}

	matcher := &CollectionMatcher{filter: trimmed}
	for _, entry := range splitCollectionEntries(trimmed) {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if entry == CollectionDefault {
			matcher.matchEmpty = true
			continue
		}
		if !isRegexEntry(entry) {
			matcher.wildcards = append(matcher.wildcards, entry)
			continue
		}
		pattern := entry
		if !strings.ContainsAny(entry, "^$") {
			pattern = "^(?:" + entry + ")$"
		}
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid collection_filter entry %q: %w", entry, err)
		}
		matcher.literals = append(matcher.literals, entry)
		matcher.regexes = append(matcher.regexes, compiled)
	}

	if !matcher.matchEmpty && len(matcher.wildcards) == 0 && len(matcher.regexes) == 0 {
		// Only a genuinely empty filter means "every collection". A value like ","
		// is a typo, and matching everything on it would encode or delete far more
		// than the operator asked for.
		return nil, fmt.Errorf("collection_filter %q has no collection names", trimmed)
	}
	return matcher, nil
}

// splitCollectionEntries splits a filter on the commas that separate entries.
// A comma inside a closed regex construct - a character class, a repetition
// count, or a group - belongs to that construct. An unclosed one is a literal
// brace or bracket in a name, so the commas after it still separate entries.
func splitCollectionEntries(filter string) []string {
	entries := make([]string, 0, 4)
	start := 0
	for i := 0; i < len(filter); i++ {
		switch filter[i] {
		case '\\':
			i++
		case '[':
			if end := classEnd(filter, i); end > i {
				i = end
			}
		case '{':
			if end := repeatCountEnd(filter, i); end > i {
				i = end
			}
		case '(':
			if end := groupEnd(filter, i); end > i {
				i = end
			}
		case ',':
			entries = append(entries, filter[start:i])
			start = i + 1
		}
	}
	return append(entries, filter[start:])
}

// classEnd returns the index of the "]" closing the character class opened at
// start, or -1 when it is never closed. Character classes do not nest.
func classEnd(filter string, start int) int {
	for i := start + 1; i < len(filter); i++ {
		switch filter[i] {
		case '\\':
			i++
		case ']':
			return i
		}
	}
	return -1
}

// repeatCountEnd returns the index of the "}" closing the repetition count
// opened at start, or -1 when what follows is not a count.
func repeatCountEnd(filter string, start int) int {
	for i := start + 1; i < len(filter); i++ {
		if filter[i] == '}' {
			return i
		}
		if filter[i] != ',' && (filter[i] < '0' || filter[i] > '9') {
			return -1
		}
	}
	return -1
}

// groupEnd returns the index of the ")" closing the group opened at start, or
// -1 when it is never closed.
func groupEnd(filter string, start int) int {
	depth := 0
	for i := start; i < len(filter); i++ {
		switch filter[i] {
		case '\\':
			i++
		case '(':
			depth++
		case ')':
			if depth--; depth == 0 {
				return i
			}
		}
	}
	return -1
}

// isRegexEntry reports whether an entry carries regex syntax rather than being a
// plain collection name with optional "*" and "?" wildcards.
func isRegexEntry(entry string) bool {
	if strings.ContainsAny(entry, regexMetaCharacters) {
		return true
	}
	for i := 0; i+1 < len(entry); i++ {
		if entry[i] == '.' && (entry[i+1] == '*' || entry[i+1] == '?') {
			return true
		}
	}
	return false
}

// Matches reports whether a collection passes the filter. A nil matcher accepts everything.
func (m *CollectionMatcher) Matches(collection string) bool {
	if m == nil {
		return true
	}
	if m.matchEmpty && collection == "" {
		return true
	}
	for _, literal := range m.literals {
		if literal == collection {
			return true
		}
	}
	for _, pattern := range m.wildcards {
		if MatchesWildcard(pattern, collection) {
			return true
		}
	}
	for _, re := range m.regexes {
		if re.MatchString(collection) {
			return true
		}
	}
	return false
}

// String returns the filter this matcher was compiled from.
func (m *CollectionMatcher) String() string {
	if m == nil {
		return ""
	}
	return m.filter
}
