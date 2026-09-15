package s3api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A marker that sorts before the prefix excludes nothing under the prefix, so the
// listing must be the one with no marker. docker/distribution's S3 driver sends
// exactly this — prefix "<root>/<path>/" with start-after "<root>" — and an empty
// answer made a zot registry on SeaweedFS believe it held no repositories.
func Test_markerSortsBeforePrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		marker string
		want   bool
	}{
		{"distribution walk: start-after is the rootdirectory", "zot/zot/", "zot", true},
		{"marker is the prefix directory without its slash", "zot/zot/", "zot/zot", true},
		{"marker is an unrelated earlier key", "zot/zot/", "a", true},
		{"marker is an earlier sibling", "zot/zot/", "zot/zos", true},
		{"leading slashes are ignored on both sides", "/zot/zot/", "/zot", true},
		{"empty marker is not a cutoff", "zot/zot/", "", false},
		{"empty prefix: every key is in scope, marker stands", "", "zot", false},
		{"marker under the prefix resumes inside it", "zot/zot/", "zot/zot/rob/ourea/index.json", false},
		{"marker equal to the prefix stands", "zot/zot/", "zot/zot/", false},
		{"marker after the prefix's subtree stands (lists nothing)", "zot/zot/", "zot/zotx", false},
		{"partial-name prefix: a later match-set marker stands", "parent", "parentDir/data/0e", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, markerSortsBeforePrefix(tt.prefix, tt.marker), "markerSortsBeforePrefix(%q, %q)", tt.prefix, tt.marker)
		})
	}
}
