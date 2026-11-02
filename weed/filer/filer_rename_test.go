package filer

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestDetectBucket(t *testing.T) {
	tests := []struct {
		name        string
		fullPath    string
		bucketsPath string
		want        string
	}{
		{"object in bucket", "/buckets/mybucket/a/b.txt", "/buckets", "mybucket"},
		{"bucket root", "/buckets/mybucket", "/buckets", "mybucket"},
		{"not under buckets path", "/other/path/x", "/buckets", ""},
		{"buckets path itself", "/buckets", "/buckets", ""},
		{"custom buckets path", "/data/buckets/mybucket/x", "/data/buckets", "mybucket"},
		{"nested object", "/buckets/b/deep/nested/key", "/buckets", "b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &Filer{DirBucketsPath: tt.bucketsPath}
			if got := f.DetectBucket(util.FullPath(tt.fullPath)); got != tt.want {
				t.Errorf("DetectBucket(%q) with buckets path %q = %q, want %q", tt.fullPath, tt.bucketsPath, got, tt.want)
			}
		})
	}
}

func TestCanRenameIntoItself(t *testing.T) {
	f, store := newTestFilerWithStubStore(t)
	f.DirBucketsPath = "/buckets"
	if err := store.InsertEntry(context.Background(), &Entry{FullPath: "/dir/a", Attr: Attr{Mode: os.ModeDir}}); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		oldName string
		target  string
		reject  bool
	}{
		{"into itself", "a", "/dir/a", true},
		{"into a child", "a", "/dir/a/b", true},
		{"into a deeper descendant", "a", "/dir/a/b/c", true},
		{"into a child with a trailing slash", "a", "/dir/a/b/", true},
		{"old name with a trailing slash into a child", "a/", "/dir/a/b", true},
		{"into a sibling sharing the name prefix", "a", "/dir/a.d", false},
		{"into a descendant of that sibling", "a", "/dir/ab/c", false},
		{"into the parent", "a", "/dir", false},
		{"into an unrelated directory", "a", "/other", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := f.CanRename(context.Background(), "/dir", util.FullPath(tt.target), tt.oldName)
			rejected := err != nil && strings.Contains(err.Error(), "subdirectory of itself")
			if rejected != tt.reject {
				t.Errorf("CanRename(/dir, %q, %q) = %v, want rejected=%v", tt.target, tt.oldName, err, tt.reject)
			}
			if !tt.reject && err != nil {
				t.Errorf("CanRename(/dir, %q, %q) = %v, want nil", tt.target, tt.oldName, err)
			}
		})
	}
}
