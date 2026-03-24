package mount

import (
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestWholeFileServerCopyCandidate(t *testing.T) {
	wfs := newCopyRangeTestWFS()

	srcPath := util.FullPath("/src.txt")
	dstPath := util.FullPath("/dst.txt")
	srcInode := wfs.inodeToPath.Lookup(srcPath, 1, false, false, 0, true)
	dstInode := wfs.inodeToPath.Lookup(dstPath, 1, false, false, 0, true)

	srcHandle := wfs.fhMap.AcquireFileHandle(wfs, srcInode, &filer_pb.Entry{
		Name: "src.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100644,
			FileSize: 5,
		},
		Content: []byte("hello"),
	})
	dstHandle := wfs.fhMap.AcquireFileHandle(wfs, dstInode, &filer_pb.Entry{
		Name: "dst.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100644,
		},
	})

	srcHandle.RememberPath(srcPath)
	dstHandle.RememberPath(dstPath)

	in := &fuse.CopyFileRangeIn{
		FhIn:   uint64(srcHandle.fh),
		FhOut:  uint64(dstHandle.fh),
		OffIn:  0,
		OffOut: 0,
		Len:    8,
	}

	gotSrc, gotDst, gotSize, ok := wholeFileServerCopyCandidate(srcHandle, dstHandle, in)
	if !ok {
		t.Fatal("expected whole-file server copy candidate")
	}
	if gotSrc != srcPath {
		t.Fatalf("source path = %q, want %q", gotSrc, srcPath)
	}
	if gotDst != dstPath {
		t.Fatalf("destination path = %q, want %q", gotDst, dstPath)
	}
	if gotSize != 5 {
		t.Fatalf("source size = %d, want 5", gotSize)
	}

	srcHandle.dirtyMetadata = true
	if _, _, _, ok := wholeFileServerCopyCandidate(srcHandle, dstHandle, in); ok {
		t.Fatal("dirty source handle should disable whole-file server copy")
	}
	srcHandle.dirtyMetadata = false

	in.Len = 4
	if _, _, _, ok := wholeFileServerCopyCandidate(srcHandle, dstHandle, in); ok {
		t.Fatal("short copy request should disable whole-file server copy")
	}
}

func TestCopyFileRangeUsesServerSideWholeFileCopy(t *testing.T) {
	wfs := newCopyRangeTestWFS()

	srcPath := util.FullPath("/src.txt")
	dstPath := util.FullPath("/dst.txt")
	srcInode := wfs.inodeToPath.Lookup(srcPath, 1, false, false, 0, true)
	dstInode := wfs.inodeToPath.Lookup(dstPath, 1, false, false, 0, true)

	srcHandle := wfs.fhMap.AcquireFileHandle(wfs, srcInode, &filer_pb.Entry{
		Name: "src.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100644,
			FileSize: 5,
		},
		Content: []byte("hello"),
	})
	dstHandle := wfs.fhMap.AcquireFileHandle(wfs, dstInode, &filer_pb.Entry{
		Name: "dst.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100644,
		},
	})

	srcHandle.RememberPath(srcPath)
	dstHandle.RememberPath(dstPath)

	originalCopy := performServerSideWholeFileCopy
	defer func() {
		performServerSideWholeFileCopy = originalCopy
	}()

	var called bool
	performServerSideWholeFileCopy = func(cancel <-chan struct{}, gotWFS *WFS, gotSrc, gotDst util.FullPath) (*filer_pb.Entry, error) {
		called = true
		if gotWFS != wfs {
			t.Fatalf("wfs = %p, want %p", gotWFS, wfs)
		}
		if gotSrc != srcPath {
			t.Fatalf("source path = %q, want %q", gotSrc, srcPath)
		}
		if gotDst != dstPath {
			t.Fatalf("destination path = %q, want %q", gotDst, dstPath)
		}
		return &filer_pb.Entry{
			Name: "dst.txt",
			Attributes: &filer_pb.FuseAttributes{
				FileMode: 0100644,
				FileSize: 5,
				Mime:     "text/plain; charset=utf-8",
			},
			Content: []byte("hello"),
		}, nil
	}

	written, status := wfs.CopyFileRange(make(chan struct{}), &fuse.CopyFileRangeIn{
		FhIn:   uint64(srcHandle.fh),
		FhOut:  uint64(dstHandle.fh),
		OffIn:  0,
		OffOut: 0,
		Len:    8,
	})
	if status != fuse.OK {
		t.Fatalf("CopyFileRange status = %v, want OK", status)
	}
	if written != 5 {
		t.Fatalf("CopyFileRange wrote %d bytes, want 5", written)
	}
	if !called {
		t.Fatal("expected server-side whole-file copy path to be used")
	}

	gotEntry := dstHandle.GetEntry().GetEntry()
	if gotEntry.Attributes == nil || gotEntry.Attributes.FileSize != 5 {
		t.Fatalf("destination size = %v, want 5", gotEntry.GetAttributes().GetFileSize())
	}
	if string(gotEntry.Content) != "hello" {
		t.Fatalf("destination content = %q, want %q", string(gotEntry.Content), "hello")
	}
	if dstHandle.dirtyMetadata {
		t.Fatal("server-side whole-file copy should leave destination handle clean")
	}
}

func newCopyRangeTestWFS() *WFS {
	wfs := &WFS{
		option: &Option{
			ChunkSizeLimit:     1024,
			ConcurrentReaders:  1,
			VolumeServerAccess: "filerProxy",
			FilerAddresses:     []pb.ServerAddress{"127.0.0.1:8888"},
		},
		inodeToPath: NewInodeToPath(util.FullPath("/"), 0),
		fhMap:       NewFileHandleToInode(),
		fhLockTable: util.NewLockTable[FileHandleId](),
	}
	wfs.copyBufferPool.New = func() any {
		return make([]byte, 1024)
	}
	return wfs
}
