package winfsp

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Event paths are absolute on the filer and cover the whole subscription, so
// rebasing them decides both what Windows is told and what is none of its
// business.
func TestRelativeToMount(t *testing.T) {
	cases := []struct {
		root string
		path string
		want string
		ok   bool
	}{
		{"/", "/a/b.txt", "/a/b.txt", true},
		{"", "/a/b.txt", "/a/b.txt", true},
		{"/buckets/data", "/buckets/data/a/b.txt", "/a/b.txt", true},
		{"/buckets/data", "/buckets/data", "/", true},
		{"/buckets/data", "/buckets/other/b.txt", "", false},
		// A sibling whose name merely starts with the root must not match.
		{"/buckets/data", "/buckets/data2/b.txt", "", false},
		{"/buckets/data", "/elsewhere", "", false},
	}
	for _, c := range cases {
		got, ok := relativeToMount(util.FullPath(c.root), util.FullPath(c.path))
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("root %q path %q = (%q, %v), want (%q, %v)", c.root, c.path, got, ok, c.want, c.ok)
		}
	}
}
