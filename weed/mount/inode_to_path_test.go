package mount

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"testing"
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
