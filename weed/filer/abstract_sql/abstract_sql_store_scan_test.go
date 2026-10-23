package abstract_sql

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestBucketTableFor(t *testing.T) {
	cases := map[string]string{
		"/":                         "",
		"/buckets":                  "",
		"/buckets/":                 "",
		"/buckets/mybucket":         "mybucket",
		"/buckets/mybucket/a/b":     "mybucket",
		"/buckets/.system/owners":   "",
		"/buckets/Invalid_Bucket/x": "",
		"/topics/.system/log":       "",
		"/bucketsfoo/mybucket":      "",
	}
	for path, want := range cases {
		if got := bucketTableFor(util.FullPath(path)); got != want {
			t.Errorf("bucketTableFor(%s) = %q, want %q", path, got, want)
		}
	}
}

// Only a missing table may be skipped; any other schema error must fail the
// scan, or a bucket's live chunks would be reported as orphans.
func TestIsMissingTableError(t *testing.T) {
	for _, msg := range []string{
		`ERROR: relation "ghost" does not exist (SQLSTATE 42P01)`,
		`Error 1146 (42S02): Table 'seaweedfs.ghost' doesn't exist`,
		`SQL logic error: no such table: ghost (1)`,
	} {
		if !isMissingTableError(errors.New(msg)) {
			t.Errorf("not recognized as a missing table: %s", msg)
		}
	}
	for _, msg := range []string{
		`ERROR: column "directory" does not exist (SQLSTATE 42703)`,
		`Error 1054 (42S22): Unknown column 'directory' in 'field list'`,
		`SQL logic error: no such column: directory (1)`,
		"connection refused",
	} {
		if isMissingTableError(errors.New(msg)) {
			t.Errorf("wrongly taken for a missing table: %s", msg)
		}
	}
}
