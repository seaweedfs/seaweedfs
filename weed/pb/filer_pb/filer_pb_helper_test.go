package filer_pb

import (
	"testing"
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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.e.IsDirectoryKeyObject(); got != c.want {
				t.Errorf("IsDirectoryKeyObject() = %v, want %v", got, c.want)
			}
		})
	}
}
