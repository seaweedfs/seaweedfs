package s3api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The component handed to the filer must keep every entry that can match or
// descend toward the requested prefix at that directory level, while excluding
// siblings that cannot: a name holds no slash, so a directory not starting
// with the component cannot contain a matching key.
func TestComputeListPrefix(t *testing.T) {
	cases := []struct {
		prefix       string
		relativePath string
		want         string
	}{
		{"", "", ""},
		{"", "a/b", ""},
		{"a/b/c", "", "a"},
		{"a/b/c", "a", "b"},
		{"a/b/c", "a/b", "c"},
		// at or inside the prefix zone: no constraint
		{"a/b/c", "a/b/c", ""},
		{"a/b/", "a/b", ""},
		{"a", "a/sub", ""},
		// partial component narrows the listing but keeps deeper matches
		// ("a/bc" keeps dir "bc", file "bcd", and "bc.versions" at level "a")
		{"a/bc", "a", "bc"},
		{"abc", "", "abc"},
		// a directory off the prefix path is inside the zone by construction
		{"a/b/c", "x", ""},
	}

	for _, tc := range cases {
		vc := &versionCollector{prefix: tc.prefix}
		assert.Equal(t, tc.want, vc.computeListPrefix(tc.relativePath),
			"prefix %q at %q", tc.prefix, tc.relativePath)
	}
}
