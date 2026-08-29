package filer_pb

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestIsDirectoryKeyObject(t *testing.T) {
	chunk := []*FileChunk{{FileId: "1,01", Size: 75}}

	cases := []struct {
		name string
		e    *Entry
		want bool
	}{
		{"plain directory", &Entry{IsDirectory: true, Attributes: &FuseAttributes{}}, false},
		{"directory marker with mime", &Entry{IsDirectory: true, Attributes: &FuseAttributes{Mime: "application/octet-stream"}}, true},
		{"directory promoted from file keeps chunks", &Entry{IsDirectory: true, Attributes: &FuseAttributes{}, Chunks: chunk}, true},
		{"directory promoted from small file keeps content", &Entry{IsDirectory: true, Attributes: &FuseAttributes{}, Content: []byte("abc")}, true},
		{"directory promoted from remote-tiered file", &Entry{IsDirectory: true, Attributes: &FuseAttributes{}, RemoteEntry: &RemoteEntry{RemoteSize: 100}}, true},
		{"directory with chunks and nil attributes", &Entry{IsDirectory: true, Chunks: chunk}, true},
		{"regular file with chunks", &Entry{IsDirectory: false, Attributes: &FuseAttributes{}, Chunks: chunk}, false},
		{"remote mount directory has no remote size", &Entry{IsDirectory: true, Attributes: &FuseAttributes{}, RemoteEntry: &RemoteEntry{StorageName: "s3"}}, false},
		{"empty prefix object has only the mark", &Entry{IsDirectory: true, Attributes: &FuseAttributes{}, Extended: map[string][]byte{s3_constants.SeaweedFSPrefixObject: []byte("true")}}, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.e.IsDirectoryKeyObject(); got != c.want {
				t.Errorf("IsDirectoryKeyObject() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestMarkPrefixObject(t *testing.T) {
	entry := &Entry{Name: "foo", Attributes: &FuseAttributes{FileMode: 0644}}

	if entry.IsPrefixObject() {
		t.Fatal("a file is not a prefix object")
	}

	entry.MarkPrefixObject()

	if !entry.IsDirectory || !entry.IsPrefixObject() {
		t.Errorf("MarkPrefixObject() left %+v", entry)
	}
	// The filer derives the entry type from the mode, so the directory bit has to be
	// on it for the store to keep this as a directory.
	if mode := os.FileMode(entry.Attributes.FileMode); mode&os.ModeDir == 0 || mode.Perm()&0111 != 0111 {
		t.Errorf("FileMode = %v, want a traversable directory", mode)
	}

	// A directory the mark was stripped from is a plain directory again.
	delete(entry.Extended, s3_constants.SeaweedFSPrefixObject)
	if entry.IsPrefixObject() || entry.IsDirectoryKeyObject() {
		t.Error("a demoted directory names no key")
	}
}
