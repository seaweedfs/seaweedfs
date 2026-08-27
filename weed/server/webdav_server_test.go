package weed_server

import (
	"context"
	"os"
	"testing"

	"golang.org/x/net/webdav"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestToFileInfoName(t *testing.T) {
	tests := []struct {
		fullpath string
		want     string
	}{
		{"/photo.jpg", "photo.jpg"},
		{"/Images/photo.jpg", "photo.jpg"},
		{"/Images/2026/photo.jpg", "photo.jpg"},
		{"/Images", "Images"},
		{"/Images/", "Images"},
		{"/", ""},
	}
	for _, tt := range tests {
		entry := &filer_pb.Entry{Name: "photo.jpg", Attributes: &filer_pb.FuseAttributes{}}
		fi := toFileInfo(util.FullPath(tt.fullpath), entry)
		if fi.Name() != tt.want {
			t.Errorf("toFileInfo(%q).Name() = %q, want %q (DAV:displayname must not carry the path)", tt.fullpath, fi.Name(), tt.want)
		}
	}
}

func TestToFileInfoRootIsDirectory(t *testing.T) {
	entry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{}}
	if !toFileInfo("/", entry).IsDir() {
		t.Error("root is not a directory")
	}
}

func TestFileInfoETag(t *testing.T) {
	ctx := context.Background()

	if _, err := (&FileInfo{}).ETag(ctx); err != webdav.ErrNotImplemented {
		t.Errorf("empty etag returned %v, want ErrNotImplemented so webdav derives one", err)
	}
	if etag, err := (&FileInfo{etag: "abc"}).ETag(ctx); err != nil || etag != "abc" {
		t.Errorf("ETag() = %q, %v, want \"abc\", nil", etag, err)
	}

	failed := &FileInfo{err: os.ErrInvalid}
	if _, err := failed.ETag(ctx); err != os.ErrInvalid {
		t.Errorf("ETag() = %v, want the stat error", err)
	}
}
