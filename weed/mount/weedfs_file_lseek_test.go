package mount

import (
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestLseekReturnsENXIOWhenNoDataFollowsOffset(t *testing.T) {
	wfs, fh := newOpenFileHandle(t, 1000)
	fh.SetEntry(&filer_pb.Entry{
		Name: "file.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100644,
			FileSize: 1000,
			Inode:    fh.inode,
		},
		Chunks: []*filer_pb.FileChunk{
			{FileId: "data", Offset: 0, Size: 100},
		},
	})

	in := &fuse.LseekIn{
		Fh:     uint64(fh.fh),
		Offset: 100,
		Whence: SEEK_DATA,
	}
	var out fuse.LseekOut

	if status := wfs.Lseek(nil, in, &out); status != ENXIO {
		t.Fatalf("Lseek(SEEK_DATA) status = %v, want ENXIO", status)
	}
}
