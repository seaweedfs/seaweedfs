package mount

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
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
			Inode:    srcInode,
		},
		Content: []byte("hello"),
	})
	dstHandle := wfs.fhMap.AcquireFileHandle(wfs, dstInode, &filer_pb.Entry{
		Name: "dst.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100644,
			Inode:    dstInode,
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

	copyRequest, ok := wholeFileServerCopyCandidate(srcHandle, dstHandle, in)
	if !ok {
		t.Fatal("expected whole-file server copy candidate")
	}
	if copyRequest.srcPath != srcPath {
		t.Fatalf("source path = %q, want %q", copyRequest.srcPath, srcPath)
	}
	if copyRequest.dstPath != dstPath {
		t.Fatalf("destination path = %q, want %q", copyRequest.dstPath, dstPath)
	}
	if copyRequest.sourceSize != 5 {
		t.Fatalf("source size = %d, want 5", copyRequest.sourceSize)
	}
	if copyRequest.srcInode == 0 || copyRequest.dstInode == 0 {
		t.Fatalf("expected inode preconditions, got src=%d dst=%d", copyRequest.srcInode, copyRequest.dstInode)
	}

	srcHandle.dirtyMetadata = true
	if _, ok := wholeFileServerCopyCandidate(srcHandle, dstHandle, in); ok {
		t.Fatal("dirty source handle should disable whole-file server copy")
	}
	srcHandle.dirtyMetadata = false

	in.Len = 4
	if _, ok := wholeFileServerCopyCandidate(srcHandle, dstHandle, in); ok {
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
			Inode:    srcInode,
		},
		Content: []byte("hello"),
	})
	dstHandle := wfs.fhMap.AcquireFileHandle(wfs, dstInode, &filer_pb.Entry{
		Name: "dst.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100644,
			Inode:    dstInode,
		},
	})

	srcHandle.RememberPath(srcPath)
	dstHandle.RememberPath(dstPath)

	originalCopy := performServerSideWholeFileCopy
	defer func() {
		performServerSideWholeFileCopy = originalCopy
	}()

	var called bool
	performServerSideWholeFileCopy = func(cancel <-chan struct{}, gotWFS *WFS, copyRequest wholeFileServerCopyRequest) (*filer_pb.Entry, serverSideWholeFileCopyOutcome, error) {
		called = true
		if gotWFS != wfs {
			t.Fatalf("wfs = %p, want %p", gotWFS, wfs)
		}
		if copyRequest.srcPath != srcPath {
			t.Fatalf("source path = %q, want %q", copyRequest.srcPath, srcPath)
		}
		if copyRequest.dstPath != dstPath {
			t.Fatalf("destination path = %q, want %q", copyRequest.dstPath, dstPath)
		}
		return &filer_pb.Entry{
			Name: "dst.txt",
			Attributes: &filer_pb.FuseAttributes{
				FileMode: 0100644,
				FileSize: 5,
				Mime:     "text/plain; charset=utf-8",
			},
			Content: []byte("hello"),
		}, serverSideWholeFileCopyCommitted, nil
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

func TestCopyFileRangeDoesNotFallbackAfterCommittedServerCopyRefreshFailure(t *testing.T) {
	wfs := newCopyRangeTestWFSWithMetaCache(t)

	srcPath := util.FullPath("/src.txt")
	dstPath := util.FullPath("/dst.txt")
	srcInode := wfs.inodeToPath.Lookup(srcPath, 1, false, false, 0, true)
	dstInode := wfs.inodeToPath.Lookup(dstPath, 1, false, false, 0, true)

	srcHandle := wfs.fhMap.AcquireFileHandle(wfs, srcInode, &filer_pb.Entry{
		Name: "src.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100644,
			FileSize: 5,
			Mime:     "text/plain; charset=utf-8",
			Md5:      []byte("abcde"),
			Inode:    srcInode,
		},
		Content: []byte("hello"),
	})
	dstHandle := wfs.fhMap.AcquireFileHandle(wfs, dstInode, &filer_pb.Entry{
		Name: "dst.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100600,
			Inode:    dstInode,
		},
	})

	srcHandle.RememberPath(srcPath)
	dstHandle.RememberPath(dstPath)

	originalCopy := performServerSideWholeFileCopy
	defer func() {
		performServerSideWholeFileCopy = originalCopy
	}()

	performServerSideWholeFileCopy = func(cancel <-chan struct{}, gotWFS *WFS, copyRequest wholeFileServerCopyRequest) (*filer_pb.Entry, serverSideWholeFileCopyOutcome, error) {
		if gotWFS != wfs || copyRequest.srcPath != srcPath || copyRequest.dstPath != dstPath {
			t.Fatalf("unexpected server-side copy call: wfs=%p src=%q dst=%q", gotWFS, copyRequest.srcPath, copyRequest.dstPath)
		}
		return nil, serverSideWholeFileCopyCommitted, errors.New("reload copied entry: transient filer read failure")
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
	if dstHandle.dirtyMetadata {
		t.Fatal("committed server-side copy should not fall back to dirty-page copy")
	}

	gotEntry := dstHandle.GetEntry().GetEntry()
	if gotEntry.GetAttributes().GetFileSize() != 5 {
		t.Fatalf("destination size = %d, want 5", gotEntry.GetAttributes().GetFileSize())
	}
	if gotEntry.GetAttributes().GetFileMode() != 0100600 {
		t.Fatalf("destination mode = %#o, want %#o", gotEntry.GetAttributes().GetFileMode(), uint32(0100600))
	}
	if string(gotEntry.GetContent()) != "hello" {
		t.Fatalf("destination content = %q, want %q", string(gotEntry.GetContent()), "hello")
	}

	cachedEntry, err := wfs.metaCache.FindEntry(context.Background(), dstPath)
	if err != nil {
		t.Fatalf("metaCache find entry: %v", err)
	}
	if cachedEntry.FileSize != 5 {
		t.Fatalf("metaCache destination size = %d, want 5", cachedEntry.FileSize)
	}
	if cachedEntry.Mime != "text/plain; charset=utf-8" {
		t.Fatalf("metaCache destination mime = %q, want %q", cachedEntry.Mime, "text/plain; charset=utf-8")
	}
}

func TestCopyFileRangeReturnsEIOForAmbiguousServerSideCopy(t *testing.T) {
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
			Inode:    srcInode,
		},
		Content: []byte("hello"),
	})
	dstHandle := wfs.fhMap.AcquireFileHandle(wfs, dstInode, &filer_pb.Entry{
		Name: "dst.txt",
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0100600,
			Inode:    dstInode,
		},
	})

	srcHandle.RememberPath(srcPath)
	dstHandle.RememberPath(dstPath)

	originalCopy := performServerSideWholeFileCopy
	defer func() {
		performServerSideWholeFileCopy = originalCopy
	}()

	performServerSideWholeFileCopy = func(cancel <-chan struct{}, gotWFS *WFS, copyRequest wholeFileServerCopyRequest) (*filer_pb.Entry, serverSideWholeFileCopyOutcome, error) {
		if gotWFS != wfs || copyRequest.srcPath != srcPath || copyRequest.dstPath != dstPath {
			t.Fatalf("unexpected server-side copy call: wfs=%p src=%q dst=%q", gotWFS, copyRequest.srcPath, copyRequest.dstPath)
		}
		return nil, serverSideWholeFileCopyAmbiguous, errors.New("transport timeout after request dispatch")
	}

	written, status := wfs.CopyFileRange(make(chan struct{}), &fuse.CopyFileRangeIn{
		FhIn:   uint64(srcHandle.fh),
		FhOut:  uint64(dstHandle.fh),
		OffIn:  0,
		OffOut: 0,
		Len:    8,
	})
	if status != fuse.EIO {
		t.Fatalf("CopyFileRange status = %v, want EIO", status)
	}
	if written != 0 {
		t.Fatalf("CopyFileRange wrote %d bytes, want 0", written)
	}
	if dstHandle.dirtyMetadata {
		t.Fatal("ambiguous server-side copy should not fall back to dirty-page copy")
	}
	if dstHandle.GetEntry().GetEntry().GetAttributes().GetFileSize() != 0 {
		t.Fatalf("destination size = %d, want 0", dstHandle.GetEntry().GetEntry().GetAttributes().GetFileSize())
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

func newCopyRangeTestWFSWithMetaCache(t *testing.T) *WFS {
	t.Helper()

	wfs := newCopyRangeTestWFS()
	root := util.FullPath("/")
	wfs.inodeToPath.MarkChildrenCached(root)
	uidGidMapper, err := meta_cache.NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("create uid/gid mapper: %v", err)
	}
	wfs.metaCache = meta_cache.NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		uidGidMapper,
		root,
		false,
		func(path util.FullPath) {
			wfs.inodeToPath.MarkChildrenCached(path)
		},
		func(path util.FullPath) bool {
			return wfs.inodeToPath.IsChildrenCached(path)
		},
		func(util.FullPath, *filer_pb.Entry) {},
		nil,
	)
	t.Cleanup(func() {
		wfs.metaCache.Shutdown()
	})

	return wfs
}
