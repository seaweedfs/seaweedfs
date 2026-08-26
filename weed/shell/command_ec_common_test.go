package shell

import "testing"

// The ec and tier commands read an empty -collection as the collection with no
// name, unlike -collectionPattern, where empty means every collection.
func TestCompileCollectionPattern(t *testing.T) {
	tests := []struct {
		pattern string
		matches []string
		misses  []string
	}{
		{pattern: "", matches: []string{""}, misses: []string{"pictures"}},
		{pattern: CollectionDefault, matches: []string{""}, misses: []string{"pictures"}},
		{pattern: "pictures", matches: []string{"pictures"}, misses: []string{"", "pictures-backup"}},
		{pattern: "pictures,videos", matches: []string{"pictures", "videos"}, misses: []string{"", "clips"}},
		{pattern: "pictures,_default", matches: []string{"pictures", ""}, misses: []string{"clips"}},
		{pattern: "pictures*", matches: []string{"pictures", "pictures-backup"}, misses: []string{"clips"}},
		{pattern: "^pictures", matches: []string{"pictures", "pictures-backup"}, misses: []string{"clips"}},
		{pattern: "*", matches: []string{"", "pictures"}},
	}

	for _, tt := range tests {
		matcher, err := compileCollectionPattern(tt.pattern)
		if err != nil {
			t.Fatalf("compileCollectionPattern(%q): %v", tt.pattern, err)
		}
		for _, collection := range tt.matches {
			if !matcher.Matches(collection) {
				t.Errorf("pattern %q should match collection %q", tt.pattern, collection)
			}
		}
		for _, collection := range tt.misses {
			if matcher.Matches(collection) {
				t.Errorf("pattern %q should not match collection %q", tt.pattern, collection)
			}
		}
	}
}
