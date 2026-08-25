package pluginworker

import "testing"

func TestCompileCollectionMatcher(t *testing.T) {
	cases := []struct {
		filter  string
		matches []string
		misses  []string
	}{
		{filter: "", matches: []string{"", "photos"}},
		{filter: "*", matches: []string{"", "photos"}},
		{filter: string(CollectionFilterAll), matches: []string{"", "photos"}},
		{filter: string(CollectionFilterEach), matches: []string{"", "photos"}},
		{filter: "photos", matches: []string{"photos"}, misses: []string{"", "photos-backup", "myphotos"}},
		{filter: "collection-a,collection-b", matches: []string{"collection-a", "collection-b"}, misses: []string{"collection-c"}},
		{filter: " collection-a , collection-b ,", matches: []string{"collection-a", "collection-b"}, misses: []string{"collection-c"}},
		{filter: "photos*,videos", matches: []string{"photos", "photos-backup", "videos"}, misses: []string{"clips"}},
		{filter: "photo?", matches: []string{"photos"}, misses: []string{"photo", "photos-backup"}},
		// A regex entry matches the whole name unless it anchors itself.
		{filter: "photos|videos", matches: []string{"photos", "videos"}, misses: []string{"photos-backup"}},
		{filter: "^photos", matches: []string{"photos", "photos-backup"}, misses: []string{"videos"}},
		{filter: "photos-.*,videos", matches: []string{"photos-backup", "videos"}, misses: []string{"photos"}},
	}

	for _, c := range cases {
		matcher, err := CompileCollectionMatcher(c.filter)
		if err != nil {
			t.Fatalf("CompileCollectionMatcher(%q): %v", c.filter, err)
		}
		for _, collection := range c.matches {
			if !matcher.Matches(collection) {
				t.Errorf("filter %q should match collection %q", c.filter, collection)
			}
		}
		for _, collection := range c.misses {
			if matcher.Matches(collection) {
				t.Errorf("filter %q should not match collection %q", c.filter, collection)
			}
		}
	}
}

func TestCompileCollectionMatcherInvalidEntry(t *testing.T) {
	if _, err := CompileCollectionMatcher("photos,[invalid"); err == nil {
		t.Fatal("expected an error for an unparsable entry")
	}
}
