package wildcard

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
		// A dot is part of a collection name unless it is quantified.
		{filter: "my.bucket", matches: []string{"my.bucket"}, misses: []string{"my-bucket", "myxbucket"}},
		{filter: "my.bucket,videos", matches: []string{"my.bucket", "videos"}, misses: []string{"my-bucket"}},
		// A name made of regex syntax is still reachable by its own spelling.
		{filter: "logs(2024)", matches: []string{"logs(2024)", "logs2024"}, misses: []string{"logs"}},
		// Escaping reaches a name whose regex syntax does not parse on its own.
		{filter: `logs\(2024`, matches: []string{"logs(2024"}, misses: []string{"logs2024", "logs"}},
		// A comma inside a character class or a repetition count is not a separator.
		{filter: "bucket[0-9]{1,3}", matches: []string{"bucket1", "bucket123"}, misses: []string{"bucket", "bucketx", "bucket1234"}},
		{filter: "[a,b]", matches: []string{"a", "b", ","}, misses: []string{"ab", "c"}},
		{filter: "bucket[0-9]{1,3},videos", matches: []string{"bucket7", "videos"}, misses: []string{"bucketx"}},
		{filter: "bucket(foo,bar)", matches: []string{"bucketfoo,bar", "bucket(foo,bar)"}, misses: []string{"bucketfoo"}},
		{filter: "logs(2024),videos", matches: []string{"logs(2024)", "logs2024", "videos"}, misses: []string{"logs"}},
		{filter: CollectionDefault, matches: []string{""}, misses: []string{"photos"}},
		{filter: "photos," + CollectionDefault, matches: []string{"photos", ""}, misses: []string{"videos"}},
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
	for _, filter := range []string{"photos,[invalid", ",", ", ,"} {
		if _, err := CompileCollectionMatcher(filter); err == nil {
			t.Errorf("filter %q should not compile", filter)
		}
	}
}
