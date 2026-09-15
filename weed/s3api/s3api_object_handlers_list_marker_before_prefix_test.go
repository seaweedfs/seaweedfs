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
