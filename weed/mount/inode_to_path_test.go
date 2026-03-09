package mount

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestInodeEntry_removeOnePath(t *testing.T) {
	tests := []struct {
		name  string
		entry InodeEntry
		p     util.FullPath
		want  bool
		count int
	}{
		{
			name: "actual case",
			entry: InodeEntry{
				paths: []util.FullPath{"/pjd/nx", "/pjd/n0"},
			},
			p:     "/pjd/nx",
			want:  true,
			count: 1,
		},
		{
			name:  "empty",
			entry: InodeEntry{},
			p:     "x",
			want:  false,
			count: 0,
		},
		{
			name: "single",
			entry: InodeEntry{
				paths: []util.FullPath{"/x"},
			},
			p:     "/x",
			want:  true,
			count: 0,
		},
		{
			name: "first",
			entry: InodeEntry{
				paths: []util.FullPath{"/x", "/y", "/z"},
			},
			p:     "/x",
			want:  true,
			count: 2,
		},
		{
			name: "middle",
			entry: InodeEntry{
				paths: []util.FullPath{"/x", "/y", "/z"},
			},
			p:     "/y",
			want:  true,
			count: 2,
		},
		{
			name: "last",
			entry: InodeEntry{
				paths: []util.FullPath{"/x", "/y", "/z"},
			},
			p:     "/z",
			want:  true,
			count: 2,
		},
		{
			name: "not found",
			entry: InodeEntry{
				paths: []util.FullPath{"/x", "/y", "/z"},
			},
			p:     "/t",
			want:  false,
			count: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.entry.removeOnePath(tt.p); got != tt.want {
				t.Errorf("removeOnePath() = %v, want %v", got, tt.want)
			}
			if tt.count != len(tt.entry.paths) {
				t.Errorf("removeOnePath path count = %v, want %v", len(tt.entry.paths), tt.count)
			}
			for i, p := range tt.entry.paths {
				if p == tt.p {
					t.Errorf("removeOnePath found path still exists at %v, %v", i, p)
				}
			}
		})
	}
}

func TestRecordDirectoryUpdateSwitchesDirectoryToReadThrough(t *testing.T) {
	root := util.FullPath("/")
	dir := util.FullPath("/data")

	inodeToPath := NewInodeToPath(root, 60)
	inodeToPath.Lookup(dir, time.Now().Unix(), true, false, 0, true)
	inodeToPath.MarkChildrenCached(dir)

	now := time.Now()
	if !inodeToPath.RecordDirectoryUpdate(dir, now, time.Second, 1) {
		t.Fatal("expected directory to switch to read-through mode")
	}
	if inodeToPath.IsChildrenCached(dir) {
		t.Fatal("directory should no longer be marked cached")
	}
	if !inodeToPath.ShouldReadDirectoryDirect(dir) {
		t.Fatal("directory should be served via direct reads after hot invalidation")
	}
}

func TestMarkChildrenCachedClearsReadThroughMode(t *testing.T) {
	root := util.FullPath("/")
	dir := util.FullPath("/data")

	inodeToPath := NewInodeToPath(root, 60)
	inodeToPath.Lookup(dir, time.Now().Unix(), true, false, 0, true)

	if !inodeToPath.MarkDirectoryReadThrough(dir, time.Now()) {
		t.Fatal("expected read-through flag to be set")
	}
	inodeToPath.MarkChildrenCached(dir)

	if !inodeToPath.IsChildrenCached(dir) {
		t.Fatal("directory should be cached after MarkChildrenCached")
	}
	if inodeToPath.ShouldReadDirectoryDirect(dir) {
		t.Fatal("directory should leave read-through mode after caching")
	}
}
