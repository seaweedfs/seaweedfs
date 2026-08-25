package pluginworker

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
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
)

// regexMetaCharacters mark a filter entry as a regex; "*" and "?" stay wildcards.
const regexMetaCharacters = `.^$+()[]{}|\`

// CollectionMatcher matches a volume collection against a collection_filter value.
type CollectionMatcher struct {
	wildcards []string
	regexes   []*regexp.Regexp
}

// CompileCollectionMatcher parses a collection_filter into a matcher. A nil
// matcher accepts every collection: that is the empty filter, "*", and both
// mode sentinels. Any other value is a comma-separated list, and a collection
// passes when one entry matches it. An entry is a name, optionally with "*" and
// "?" wildcards, or a regex when it carries regex syntax. A regex entry must
// match the whole name unless it anchors itself with "^" or "$".
func CompileCollectionMatcher(filter string) (*CollectionMatcher, error) {
	trimmed := strings.TrimSpace(filter)
	mode := CollectionFilterMode(trimmed)
	if trimmed == "" || trimmed == "*" || mode == CollectionFilterAll || mode == CollectionFilterEach {
		return nil, nil
	}

	matcher := &CollectionMatcher{}
	for _, entry := range strings.Split(trimmed, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if !strings.ContainsAny(entry, regexMetaCharacters) {
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
		matcher.regexes = append(matcher.regexes, compiled)
	}

	if len(matcher.wildcards) == 0 && len(matcher.regexes) == 0 {
		return nil, nil
	}
	return matcher, nil
}

// Matches reports whether a collection passes the filter. A nil matcher accepts everything.
func (m *CollectionMatcher) Matches(collection string) bool {
	if m == nil {
		return true
	}
	for _, pattern := range m.wildcards {
		if wildcard.MatchesWildcard(pattern, collection) {
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
