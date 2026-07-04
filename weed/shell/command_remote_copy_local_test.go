package shell

import (
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
		localFiles        map[string]*filer_pb.Entry
		remoteFiles       map[string]bool // path -> isDirectory
		forceUpdate       bool
		deleteExtraneous  bool
		fileFilter        *FileFilter
		wantFilesToCopy   []string
		wantFilesToDelete []string
		wantDirsToDelete  []string
	}{
		{
			name: "copy local-only files, no delete flag",
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
			name: "delete flag removes remote-only files",
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
			localFiles: map[string]*filer_pb.Entry{},
			remoteFiles: map[string]bool{
				"/mnt/orphan.txt": false,
			},
			deleteExtraneous:  true,
			fileFilter:        newTestFileFilter("", ""),
			wantFilesToDelete: []string{"/mnt/orphan.txt"},
		},
		{
			name: "delete respects include pattern",
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
			name: "delete respects exclude pattern",
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
			name: "delete removes orphaned directories deepest first",
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
			wantDirsToDelete: []string{"/mnt/old/sub", "/mnt/old/sub2", "/mnt/old"},
		},
		{
			name: "directories are not deleted when include or exclude filters are set",
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
			name: "directory present locally is not deleted",
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
			name: "forceUpdate copies files that exist on remote",
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
			name: "local directories are not copied",
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
			plan := planLocalToRemoteSync(tt.localFiles, tt.remoteFiles, tt.forceUpdate, tt.deleteExtraneous, tt.fileFilter)
			if !reflect.DeepEqual(plan.filesToCopy, tt.wantFilesToCopy) {
				t.Errorf("filesToCopy = %v, want %v", plan.filesToCopy, tt.wantFilesToCopy)
			}
			if !reflect.DeepEqual(plan.filesToDelete, tt.wantFilesToDelete) {
				t.Errorf("filesToDelete = %v, want %v", plan.filesToDelete, tt.wantFilesToDelete)
			}
			if !reflect.DeepEqual(plan.dirsToDelete, tt.wantDirsToDelete) {
				t.Errorf("dirsToDelete = %v, want %v", plan.dirsToDelete, tt.wantDirsToDelete)
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
