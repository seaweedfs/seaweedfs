package shell

import (
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newTestFileFilter(include, exclude string) *FileFilter {
	defaultInt64 := int64(-1)
	return &FileFilter{
		include:     &include,
		exclude:     &exclude,
		minSize:     &defaultInt64,
		maxSize:     &defaultInt64,
		minAge:      &defaultInt64,
		maxAge:      &defaultInt64,
		minCacheAge: &defaultInt64,
		now:         time.Now().Unix(),
	}
}

func testFileEntry(name string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{},
	}
}

func testDirEntry(name string) *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        name,
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{},
	}
}

func TestPlanLocalToRemoteSync(t *testing.T) {
	tests := []struct {
		name              string
		dirToCopy         string
		localFiles        map[string]*filer_pb.Entry
		remoteFiles       map[string]bool // path -> isDirectory
		forceUpdate       bool
		deleteExtraneous  bool
		fileFilter        *FileFilter
		wantFilesToCopy   []string
		wantFilesToDelete []string
	}{
		{
			name:      "copy local-only files, no delete flag",
			dirToCopy: "/mnt",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/a.txt": testFileEntry("a.txt"),
				"/mnt/b.txt": testFileEntry("b.txt"),
			},
			remoteFiles: map[string]bool{
				"/mnt/b.txt":      false,
				"/mnt/orphan.txt": false,
			},
			fileFilter:      newTestFileFilter("", ""),
			wantFilesToCopy: []string{"/mnt/a.txt"},
		},
		{
			name:      "delete flag removes remote-only files",
			dirToCopy: "/mnt",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/a.txt": testFileEntry("a.txt"),
			},
			remoteFiles: map[string]bool{
				"/mnt/a.txt":       false,
				"/mnt/orphan.txt":  false,
				"/mnt/orphan2.txt": false,
			},
			deleteExtraneous:  true,
			fileFilter:        newTestFileFilter("", ""),
			wantFilesToDelete: []string{"/mnt/orphan.txt", "/mnt/orphan2.txt"},
		},
		{
			name:       "delete with empty local storage removes all remote files",
			dirToCopy:  "/mnt",
			localFiles: map[string]*filer_pb.Entry{},
			remoteFiles: map[string]bool{
				"/mnt/orphan.txt": false,
			},
			deleteExtraneous:  true,
			fileFilter:        newTestFileFilter("", ""),
			wantFilesToDelete: []string{"/mnt/orphan.txt"},
		},
		{
			name:      "delete respects include pattern",
			dirToCopy: "/mnt",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/a.pdf": testFileEntry("a.pdf"),
			},
			remoteFiles: map[string]bool{
				"/mnt/a.pdf":      false,
				"/mnt/orphan.pdf": false,
				"/mnt/keep.txt":   false,
			},
			deleteExtraneous:  true,
			fileFilter:        newTestFileFilter("*.pdf", ""),
			wantFilesToDelete: []string{"/mnt/orphan.pdf"},
		},
		{
			name:      "delete respects exclude pattern",
			dirToCopy: "/mnt",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/a.txt": testFileEntry("a.txt"),
			},
			remoteFiles: map[string]bool{
				"/mnt/a.txt":      false,
				"/mnt/orphan.txt": false,
				"/mnt/keep.bak":   false,
			},
			deleteExtraneous:  true,
			fileFilter:        newTestFileFilter("", "*.bak"),
			wantFilesToDelete: []string{"/mnt/orphan.txt"},
		},
		{
			name:      "delete ignores size/age filters",
			dirToCopy: "/mnt",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/a.txt": testFileEntry("a.txt"),
			},
			remoteFiles: map[string]bool{
				"/mnt/a.txt":      false,
				"/mnt/orphan.txt": false,
			},
			deleteExtraneous: true,
			fileFilter: func() *FileFilter {
				ff := newTestFileFilter("", "")
				minSize := int64(1000000) // would exclude orphan.txt from copying if it applied to deletion
				ff.minSize = &minSize
				return ff
			}(),
			wantFilesToDelete: []string{"/mnt/orphan.txt"},
		},
		{
			name:      "delete removes orphaned files, directory entries left alone",
			dirToCopy: "/mnt",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/a.txt": testFileEntry("a.txt"),
			},
			remoteFiles: map[string]bool{
				"/mnt/a.txt":               false,
				"/mnt/old":                 true,
				"/mnt/old/sub":             true,
				"/mnt/old/sub/orphan.txt":  false,
				"/mnt/old/sub2":            true,
				"/mnt/old/sub2/orphan.txt": false,
			},
			deleteExtraneous: true,
			fileFilter:       newTestFileFilter("", ""),
			wantFilesToDelete: []string{
				"/mnt/old/sub/orphan.txt",
				"/mnt/old/sub2/orphan.txt",
			},
		},
		{
			name:      "exclude pattern protects remote files under nested directory",
			dirToCopy: "/mnt",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/a.txt": testFileEntry("a.txt"),
			},
			remoteFiles: map[string]bool{
				"/mnt/a.txt":          false,
				"/mnt/old":            true,
				"/mnt/old/orphan.txt": false,
				"/mnt/old/keep.bak":   false,
			},
			deleteExtraneous:  true,
			fileFilter:        newTestFileFilter("", "*.bak"),
			wantFilesToDelete: []string{"/mnt/old/orphan.txt"},
		},
		{
			name:      "remote files outside -dir are never deleted",
			dirToCopy: "/mnt/foo",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/foo/a.txt": testFileEntry("a.txt"),
			},
			remoteFiles: map[string]bool{
				"/mnt/foo/a.txt":      false,
				"/mnt/foo/orphan.txt": false,
				"/mnt/foobar/b.txt":   false, // sibling sharing the "foo" key prefix, must be left alone
			},
			deleteExtraneous:  true,
			fileFilter:        newTestFileFilter("", ""),
			wantFilesToDelete: []string{"/mnt/foo/orphan.txt"},
		},
		{
			name:      "directory present locally is not deleted",
			dirToCopy: "/mnt",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/dir": testDirEntry("dir"),
			},
			remoteFiles: map[string]bool{
				"/mnt/dir": true,
			},
			deleteExtraneous: true,
			fileFilter:       newTestFileFilter("", ""),
		},
		{
			name:      "forceUpdate copies files that exist on remote",
			dirToCopy: "/mnt",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/a.txt": testFileEntry("a.txt"),
			},
			remoteFiles: map[string]bool{
				"/mnt/a.txt": false,
			},
			forceUpdate:     true,
			fileFilter:      newTestFileFilter("", ""),
			wantFilesToCopy: []string{"/mnt/a.txt"},
		},
		{
			name:      "local directories are not copied",
			dirToCopy: "/mnt",
			localFiles: map[string]*filer_pb.Entry{
				"/mnt/dir":   testDirEntry("dir"),
				"/mnt/a.txt": testFileEntry("a.txt"),
			},
			remoteFiles:     map[string]bool{},
			fileFilter:      newTestFileFilter("", ""),
			wantFilesToCopy: []string{"/mnt/a.txt"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planLocalToRemoteSync(tt.localFiles, tt.remoteFiles, util.FullPath(tt.dirToCopy), tt.forceUpdate, tt.deleteExtraneous, tt.fileFilter)
			if !reflect.DeepEqual(plan.filesToCopy, tt.wantFilesToCopy) {
				t.Errorf("filesToCopy = %v, want %v", plan.filesToCopy, tt.wantFilesToCopy)
			}
			if !reflect.DeepEqual(plan.filesToDelete, tt.wantFilesToDelete) {
				t.Errorf("filesToDelete = %v, want %v", plan.filesToDelete, tt.wantFilesToDelete)
			}
		})
	}
}

func TestFileFilter_matchesName(t *testing.T) {
	tests := []struct {
		name     string
		include  string
		exclude  string
		fileName string
		want     bool
	}{
		{"no filters", "", "", "a.txt", true},
		{"include match", "*.pdf", "", "a.pdf", true},
		{"include mismatch", "*.pdf", "", "a.txt", false},
		{"exclude match", "", "*.tmp", "a.tmp", false},
		{"exclude mismatch", "", "*.tmp", "a.txt", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ff := newTestFileFilter(tt.include, tt.exclude)
			if got := ff.matchesName(tt.fileName); got != tt.want {
				t.Errorf("matchesName(%q) = %v, want %v", tt.fileName, got, tt.want)
			}
		})
	}
}
