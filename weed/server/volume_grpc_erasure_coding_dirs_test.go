package weed_server

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage"
)

func loc(dir, idxDir string) *storage.DiskLocation {
	return &storage.DiskLocation{Directory: dir, IdxDirectory: idxDir}
}

// The rebuild reads its shards from one directory but resolves the volume's
// layout — ratio and uniform block size — from the .vif or the generation-0
// .ecsum, which on a multi-disk server may sit anywhere. Every directory that
// could hold one has to be in the search list, or the resolution silently
// falls back to 10+4 with the legacy striping and reconstructs through the
// wrong matrix.
func TestRebuildSearchDirs(t *testing.T) {
	tests := []struct {
		name    string
		rebuild *storage.DiskLocation
		others  []*storage.DiskLocation
		want    []string
	}{
		{
			name:    "the rebuild's own index directory is searched",
			rebuild: loc("/data1", "/idx1"),
			want:    []string{"/idx1"},
		},
		{
			name:    "an unsplit rebuild location contributes nothing",
			rebuild: loc("/data1", "/data1"),
			want:    nil,
		},
		{
			// The case two reviewers flagged: a sibling holding only shards
			// while its index directory holds this volume's .vif.
			name:    "a sibling's index directory is searched, not just its data directory",
			rebuild: loc("/data1", "/data1"),
			others:  []*storage.DiskLocation{loc("/data2", "/idx2")},
			want:    []string{"/data2", "/idx2"},
		},
		{
			name:    "shared index directories are listed once",
			rebuild: loc("/data1", "/shared-idx"),
			others:  []*storage.DiskLocation{loc("/data2", "/shared-idx"), loc("/data3", "/shared-idx")},
			want:    []string{"/shared-idx", "/data2", "/data3"},
		},
		{
			name:    "the rebuild's data directory is never repeated",
			rebuild: loc("/data1", "/idx1"),
			others:  []*storage.DiskLocation{loc("/data1", "/idx1"), loc("/data2", "/data1")},
			want:    []string{"/idx1", "/data2"},
		},
		{
			name:    "empty directories are dropped",
			rebuild: loc("/data1", ""),
			others:  []*storage.DiskLocation{loc("/data2", "")},
			want:    []string{"/data2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := rebuildSearchDirs(tc.rebuild, tc.others)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("rebuildSearchDirs() = %v, want %v", got, tc.want)
			}
		})
	}
}
