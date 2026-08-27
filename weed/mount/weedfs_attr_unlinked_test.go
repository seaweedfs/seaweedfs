package mount

import (
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newUnlinkedOpenFile builds a WFS holding one open handle whose name has
// already been removed, the state a file is in between unlink() and the last
// close() of a descriptor still pointing at it.
func newUnlinkedOpenFile(t *testing.T) (*WFS, uint64, *FileHandle) {
	t.Helper()

	wfs := &WFS{
		option:         &Option{ChunkSizeLimit: 1024, ConcurrentReaders: 1},
		inodeToPath:    NewInodeToPath(util.FullPath("/"), 0),
		fhMap:          NewFileHandleToInode(),
		fhLockTable:    util.NewLockTable[FileHandleId](),
		openMtimeCache: make(map[uint64][2]int64, 8),
	}

	const inode = uint64(42)
	fullPath := util.FullPath("/dir/file")
	wfs.inodeToPath.Lookup(fullPath, 1, false, false, inode, true)

	entry := &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
	}
	chunkGroup, err := filer.NewChunkGroup(nil, nil, nil, 1, nil)
	if err != nil {
		t.Fatalf("NewChunkGroup: %v", err)
	}
	fh := &FileHandle{
		fh:              FileHandleId(1),
		inode:           inode,
		wfs:             wfs,
		entry:           &LockedEntry{Entry: entry},
		entryChunkGroup: chunkGroup,
	}
	fh.dirtyPages = newPageWriter(fh, 1<<20)
	fh.RememberPath(fullPath)
	wfs.fhMap.inode2fh[inode] = fh
	wfs.fhMap.fh2inode[fh.fh] = inode

	wfs.inodeToPath.RemovePath(fullPath)
	fh.isDeleted = true

	return wfs, inode, fh
}

// TestSetAttrOnUnlinkedOpenFile covers ftruncate/fchmod on a descriptor whose
// file has been unlinked: POSIX keeps the open file alive, so these must not
// fail with ENOENT.
func TestSetAttrOnUnlinkedOpenFile(t *testing.T) {
	wfs, inode, fh := newUnlinkedOpenFile(t)

	in := &fuse.SetAttrIn{}
	in.NodeId = inode
	in.Valid = fuse.FATTR_SIZE
	in.Size = 128
	var out fuse.AttrOut
	if status := wfs.SetAttr(nil, in, &out); status != fuse.OK {
		t.Fatalf("SetAttr size on unlinked open file: got %v, want OK", status)
	}
	if out.Attr.Size != 128 {
		t.Fatalf("SetAttr size: got %d, want 128", out.Attr.Size)
	}
	if got := fh.GetEntry().GetEntry().Attributes.FileSize; got != 128 {
		t.Fatalf("handle entry FileSize: got %d, want 128", got)
	}
	if !fh.dirtyMetadata {
		t.Fatal("SetAttr did not mark the handle dirty")
	}

	in = &fuse.SetAttrIn{}
	in.NodeId = inode
	in.Valid = fuse.FATTR_MODE
	in.Mode = 0600
	if status := wfs.SetAttr(nil, in, &out); status != fuse.OK {
		t.Fatalf("SetAttr mode on unlinked open file: got %v, want OK", status)
	}
	if got := fh.GetEntry().GetEntry().Attributes.FileMode & 0777; got != 0600 {
		t.Fatalf("handle entry FileMode: got %o, want 600", got)
	}
}

// TestGetAttrOnUnlinkedOpenFile pins fstat on an unlinked descriptor: the size
// stays visible and nlink drops to zero.
func TestGetAttrOnUnlinkedOpenFile(t *testing.T) {
	wfs, inode, fh := newUnlinkedOpenFile(t)
	fh.GetEntry().GetEntry().Attributes.FileSize = 128

	in := &fuse.GetAttrIn{}
	in.NodeId = inode
	var out fuse.AttrOut
	if status := wfs.GetAttr(nil, in, &out); status != fuse.OK {
		t.Fatalf("GetAttr on unlinked open file: got %v, want OK", status)
	}
	if out.Attr.Size != 128 {
		t.Fatalf("GetAttr size: got %d, want 128", out.Attr.Size)
	}
	if out.Attr.Nlink != 0 {
		t.Fatalf("GetAttr nlink: got %d, want 0", out.Attr.Nlink)
	}
}

// TestXAttrOnUnlinkedOpenFile covers fsetxattr/fgetxattr/fremovexattr on a
// descriptor whose file has been unlinked.
func TestXAttrOnUnlinkedOpenFile(t *testing.T) {
	wfs, inode, _ := newUnlinkedOpenFile(t)

	setIn := &fuse.SetXAttrIn{}
	setIn.NodeId = inode
	if status := wfs.SetXAttr(nil, setIn, "user.k", []byte("v")); status != fuse.OK {
		t.Fatalf("SetXAttr on unlinked open file: got %v, want OK", status)
	}

	header := &fuse.InHeader{NodeId: inode}
	dest := make([]byte, 8)
	n, status := wfs.GetXAttr(nil, header, "user.k", dest)
	if status != fuse.OK {
		t.Fatalf("GetXAttr on unlinked open file: got %v, want OK", status)
	}
	if string(dest[:n]) != "v" {
		t.Fatalf("GetXAttr value: got %q, want %q", dest[:n], "v")
	}

	if status := wfs.RemoveXAttr(nil, header, "user.k"); status != fuse.OK {
		t.Fatalf("RemoveXAttr on unlinked open file: got %v, want OK", status)
	}
}
