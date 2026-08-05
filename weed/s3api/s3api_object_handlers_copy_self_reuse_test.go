package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestSelfCopyReplacesSourceEntry(t *testing.T) {
	cases := []struct {
		name            string
		versioningState string
		srcVersionId    string
		want            bool
	}{
		{"no versioning replaces the bare key", "", "", true},
		{"versioning enabled writes a new version file", s3_constants.VersioningEnabled, "", false},
		{"suspended writes the null version beside live versions", s3_constants.VersioningSuspended, "", false},
		{"pinned source version outlives the copy", "", "6736fb618f225b190c06e5b4fb63c83b", false},
		{"pinned null source version", "", "null", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := selfCopyReplacesSourceEntry(c.versioningState, c.srcVersionId); got != c.want {
				t.Errorf("selfCopyReplacesSourceEntry(%q, %q) = %v, want %v", c.versioningState, c.srcVersionId, got, c.want)
			}
		})
	}
}
