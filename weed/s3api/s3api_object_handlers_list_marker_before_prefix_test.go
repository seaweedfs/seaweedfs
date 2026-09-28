package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
)

// A marker that sorts before the prefix excludes nothing under the prefix, so the
// listing must be the one with no marker. docker/distribution's S3 driver sends
// exactly this shape: prefix "<root>/<path>/" with start-after "<root>".
func Test_markerSortsBeforePrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		marker string
		want   bool
	}{
		{"marker is the root directory the prefix walks under", "docker/registry/", "docker", true},
		{"marker is the prefix directory without its slash", "docker/registry/", "docker/registry", true},
		{"marker is an unrelated earlier key", "docker/registry/", "a", true},
		{"marker is an earlier sibling", "docker/registry/", "docker/regisr", true},
		{"leading slashes are ignored on both sides", "/docker/registry/", "/docker", true},
		{"empty marker is not a cutoff", "docker/registry/", "", false},
		{"empty prefix: every key is in scope, marker stands", "", "docker", false},
		{"marker under the prefix resumes inside it", "docker/registry/", "docker/registry/v2/link", false},
		{"marker equal to the prefix stands", "docker/registry/", "docker/registry/", false},
		{"marker after the prefix's subtree stands", "docker/registry/", "docker/registryx", false},
		{"partial name prefix: a later match-set marker stands", "parent", "parentDir/data/0e", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, markerSortsBeforePrefix(tt.prefix, tt.marker), "markerSortsBeforePrefix(%q, %q)", tt.prefix, tt.marker)
		})
	}
}

// A marker that sorts past the prefix leaves no key under the prefix to resume
// at, so the listing is empty instead of continuing into later prefixes.
func Test_markerSortsPastPrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		marker string
		want   bool
	}{
		{"marker is a later directory", "b/", "c/", true},
		{"marker is a later key without a slash", "b/", "c", true},
		{"marker shares the parent dir but sorts past", "data/a", "data/b", true},
		{"marker diverges after the prefix", "b/", "b0/x", true},
		{"leading slashes are ignored on both sides", "/b/", "/c/", true},
		{"empty marker is in range", "b/", "", false},
		{"empty prefix: every key is in scope", "", "c/", false},
		{"marker under the prefix resumes inside it", "b/", "b/allowed-1", false},
		{"marker equal to the prefix stands", "b/", "b/", false},
		{"partial name prefix: marker under the match set stands", "parent", "parentDir/data/0e", false},
		{"marker before the prefix is not past it", "b/", "a/", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, markerSortsPastPrefix(tt.prefix, tt.marker), "markerSortsPastPrefix(%q, %q)", tt.prefix, tt.marker)
		})
	}
}

// TestListWithMarkerPastPrefix walks the listing the way listFilerEntries does
// for a start position past the requested prefix. normalizePrefixMarker hands
// such a marker through at the bucket root, where the walk's marker descent
// would otherwise drop the prefix filter and continue into a later prefix.
func TestListWithMarkerPastPrefix(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/test":     {newDir("a"), newDir("b"), newDir("c")},
			"/buckets/test/b":   {{Name: "allowed-1", Attributes: &filer_pb.FuseAttributes{}}},
			"/buckets/test/c":   {{Name: "other-2", Attributes: &filer_pb.FuseAttributes{}}},
			"/buckets/test/c/z": {{Name: "deep", Attributes: &filer_pb.FuseAttributes{}}},
		},
	}

	for _, tt := range []struct {
		name   string
		marker string
		want   []string
	}{
		{"marker is a later directory", "c/", nil},
		{"marker is a deeper path in a later directory", "c/z", nil},
		{"marker is a later bare key", "c", nil},
		{"marker inside the prefix resumes normally", "b/", []string{"allowed-1"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requestDir, entryPrefix, entryMarker, prefixEndsOnDelimiter := normalizePrefixMarker("b/", tt.marker)
			dir := "/buckets/test"
			if requestDir != "" {
				dir += "/" + requestDir
			}
			seen := listedNames(t, client, listDirectoryRequest{
				dir:    dir,
				prefix: entryPrefix,
				marker: entryMarker,
				bucket: "test",
			}, &ListingCursor{maxKeys: 1000, prefixEndsOnDelimiter: prefixEndsOnDelimiter})
			assert.Equal(t, tt.want, seen)
		})
	}
}

// A marker ending on the delimiter is trimmed to a shorter cutoff for the walk, which no
// longer excludes the key the client named, so that key is skipped as it streams.
func Test_excludedMarkerKey(t *testing.T) {
	tests := []struct {
		name          string
		requestMarker string
		marker        string
		want          string
	}{
		{"marker trimmed to a shorter cutoff", "docker/", "docker", "docker/"},
		{"leading slashes are dropped, as the keys carry none", "/docker/", "docker", "docker/"},
		{"untrimmed marker: the walk already excludes it", "docker", "docker", ""},
		{"no marker", "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, excludedMarkerKey(tt.requestMarker, tt.marker), "excludedMarkerKey(%q, %q)", tt.requestMarker, tt.marker)
		})
	}
}

// TestListWithMarkerBeforePrefix walks the whole listing path the way listFilerEntries
// does, for the two start-after values a registry sends against the same prefix.
func TestListWithMarkerBeforePrefix(t *testing.T) {
	client := &testFilerClient{
		entriesByDir: map[string][]*filer_pb.Entry{
			"/buckets/registry/docker/registry/v2":              {newDir("repositories")},
			"/buckets/registry/docker/registry/v2/repositories": {newDir("app")},
			"/buckets/registry/docker/registry/v2/repositories/app": {
				{Name: "link", Attributes: &filer_pb.FuseAttributes{}},
			},
		},
	}
	prefix := "docker/registry/v2/repositories/"

	for _, tt := range []struct {
		name   string
		marker string
	}{
		{"start-after is the root directory the walk starts from", "docker"},
		{"start-after is the prefix itself", prefix},
		{"start-after is the prefix with a leading slash", "/" + prefix},
	} {
		t.Run(tt.name, func(t *testing.T) {
			marker := adjustMarkerForDelimiter(tt.marker, prefix, "/")
			requestDir, entryPrefix, entryMarker, prefixEndsOnDelimiter := normalizePrefixMarker(prefix, marker)
			seen := listedNames(t, client, listDirectoryRequest{
				dir:    "/buckets/registry/" + requestDir,
				prefix: entryPrefix,
				marker: entryMarker,
				bucket: "registry",
			}, &ListingCursor{maxKeys: 1000, prefixEndsOnDelimiter: prefixEndsOnDelimiter})
			assert.Equal(t, []string{"link"}, seen)
		})
	}
}
