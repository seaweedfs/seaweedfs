package filersink

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestTargetPathToSourcePath(t *testing.T) {
	tests := []struct {
		name       string
		targetRoot string
		sourceRoot string
		targetPath string
		wantPath   util.FullPath
		wantOK     bool
	}{
		{
			name:       "basic mapping",
			targetRoot: "/target",
			sourceRoot: "/source",
			targetPath: "/target/path/file.txt",
			wantPath:   "/source/path/file.txt",
			wantOK:     true,
		},
		{
			name:       "trailing slash roots",
			targetRoot: "/target/",
			sourceRoot: "/source/",
			targetPath: "/target/path/file.txt",
			wantPath:   "/source/path/file.txt",
			wantOK:     true,
		},
		{
			name:       "root target mapping",
			targetRoot: "/",
			sourceRoot: "/source",
			targetPath: "/path/file.txt",
			wantPath:   "/source/path/file.txt",
			wantOK:     true,
		},
		{
			name:       "target root itself",
			targetRoot: "/target",
			sourceRoot: "/source",
			targetPath: "/target",
			wantPath:   "/source",
			wantOK:     true,
		},
		{
			name:       "outside target root",
			targetRoot: "/target",
			sourceRoot: "/source",
			targetPath: "/other/path/file.txt",
			wantPath:   "",
			wantOK:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := &FilerSink{
				dir: tc.targetRoot,
				filerSource: &source.FilerSource{
					Dir: tc.sourceRoot,
				},
			}

			gotPath, ok := fs.targetPathToSourcePath(tc.targetPath)
			if ok != tc.wantOK {
				t.Fatalf("ok mismatch: got %v, want %v", ok, tc.wantOK)
			}
			if gotPath != tc.wantPath {
				t.Fatalf("path mismatch: got %q, want %q", gotPath, tc.wantPath)
			}
		})
	}
}
