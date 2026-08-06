package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestCopyReplacesSourceEntry(t *testing.T) {
	cases := []struct {
		name            string
		sameDestination bool
		versioningState string
		srcVersionId    string
		want            bool
	}{
		{"no versioning replaces the bare key", true, "", "", true},
		{"a copy to another key writes its own entry", false, "", "", false},
		{"versioning enabled writes a new version file", true, s3_constants.VersioningEnabled, "", false},
		{"suspended writes the null version beside live versions", true, s3_constants.VersioningSuspended, "", false},
		{"pinned source version outlives the copy", true, "", "6736fb618f225b190c06e5b4fb63c83b", false},
		{"pinned null source version", true, "", "null", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := copyReplacesSourceEntry(c.sameDestination, c.versioningState, c.srcVersionId); got != c.want {
				t.Errorf("copyReplacesSourceEntry(%v, %q, %q) = %v, want %v", c.sameDestination, c.versioningState, c.srcVersionId, got, c.want)
			}
		})
	}
}
