package mount

import (
	"os"
	"testing"
	"time"

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
	// The kernel caches what this reply carries, so it has to agree with fstat.
	if out.Attr.Nlink != 0 {
		t.Fatalf("SetAttr nlink: got %d, want 0", out.Attr.Nlink)
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

// newRemovedOpenDir builds a WFS holding one kernel-referenced directory whose
// name rmdir has already dropped, the state between rmdir() and releasedir().
func newRemovedOpenDir(t *testing.T) (*WFS, uint64) {
	t.Helper()

	wfs := &WFS{
		option:      &Option{},
		inodeToPath: NewInodeToPath(util.FullPath("/"), 0),
		fhMap:       NewFileHandleToInode(),
		fhLockTable: util.NewLockTable[FileHandleId](),
		atimeMap:    make(map[uint64]time.Time, 8),
		dirMtimeMap: make(map[uint64]time.Time, 8),
	}

	const inode = uint64(7)
	fullPath := util.FullPath("/dir")
	wfs.inodeToPath.Lookup(fullPath, 1, true, false, inode, true)

	removedInode, stillReferenced := wfs.inodeToPath.RemovePath(fullPath)
	if removedInode != inode || !stillReferenced {
		t.Fatalf("RemovePath: got inode %d referenced %v, want %d true", removedInode, stillReferenced, inode)
	}
	wfs.rememberRemovedDir(inode, &filer_pb.Entry{
		Name:        "dir",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{FileMode: uint32(os.ModeDir | 0755), Uid: 1, Gid: 2},
	})

	return wfs, inode
}

// TestSetAttrOnRemovedOpenDir covers fchmod/futimens on a descriptor whose
// directory has been removed: the inode lives on until the final forget, so
// these must not fail with ENOENT, and the change must be visible to a
// following fstat.
func TestSetAttrOnRemovedOpenDir(t *testing.T) {
	wfs, inode := newRemovedOpenDir(t)

	in := &fuse.SetAttrIn{}
	in.NodeId = inode
	in.Valid = fuse.FATTR_MODE
	in.Mode = 0770
	var out fuse.AttrOut
	if status := wfs.SetAttr(nil, in, &out); status != fuse.OK {
		t.Fatalf("SetAttr mode on removed open dir: got %v, want OK", status)
	}
	if out.Attr.Mode&0777 != 0770 {
		t.Fatalf("SetAttr mode: got %o, want 770", out.Attr.Mode&0777)
	}
	if out.Attr.Nlink != 0 {
		t.Fatalf("SetAttr nlink: got %d, want 0", out.Attr.Nlink)
	}

	in = &fuse.SetAttrIn{}
	in.NodeId = inode
	in.Valid = fuse.FATTR_MTIME
	in.Mtime = 12345
	if status := wfs.SetAttr(nil, in, &out); status != fuse.OK {
		t.Fatalf("SetAttr mtime on removed open dir: got %v, want OK", status)
	}

	gin := &fuse.GetAttrIn{}
	gin.NodeId = inode
	if status := wfs.GetAttr(nil, gin, &out); status != fuse.OK {
		t.Fatalf("GetAttr on removed open dir: got %v, want OK", status)
	}
	if out.Attr.Mode&0777 != 0770 {
		t.Fatalf("GetAttr mode after chmod: got %o, want 770", out.Attr.Mode&0777)
	}
	if out.Attr.Mode&fuse.S_IFDIR == 0 {
		t.Fatalf("GetAttr lost the directory type: %o", out.Attr.Mode)
	}
	if out.Attr.Mtime != 12345 {
		t.Fatalf("GetAttr mtime after utimens: got %d, want 12345", out.Attr.Mtime)
	}
	if out.Attr.Uid != 1 || out.Attr.Gid != 2 {
		t.Fatalf("GetAttr uid/gid: got %d/%d, want 1/2", out.Attr.Uid, out.Attr.Gid)
	}
	if out.Attr.Nlink != 0 {
		t.Fatalf("GetAttr nlink: got %d, want 0", out.Attr.Nlink)
	}
}

// TestXAttrOnRemovedOpenDir covers fsetxattr/fgetxattr/fremovexattr on a
// descriptor whose directory has been removed.
func TestXAttrOnRemovedOpenDir(t *testing.T) {
	wfs, inode := newRemovedOpenDir(t)

	setIn := &fuse.SetXAttrIn{}
	setIn.NodeId = inode
	if status := wfs.SetXAttr(nil, setIn, "user.k", []byte("v")); status != fuse.OK {
		t.Fatalf("SetXAttr on removed open dir: got %v, want OK", status)
	}

	header := &fuse.InHeader{NodeId: inode}
	dest := make([]byte, 8)
	n, status := wfs.GetXAttr(nil, header, "user.k", dest)
	if status != fuse.OK {
		t.Fatalf("GetXAttr on removed open dir: got %v, want OK", status)
	}
	if string(dest[:n]) != "v" {
		t.Fatalf("GetXAttr value: got %q, want %q", dest[:n], "v")
	}

	if status := wfs.RemoveXAttr(nil, header, "user.k"); status != fuse.OK {
		t.Fatalf("RemoveXAttr on removed open dir: got %v, want OK", status)
	}
	if _, status := wfs.GetXAttr(nil, header, "user.k", dest); status != fuse.ENOATTR {
		t.Fatalf("GetXAttr after remove: got %v, want ENOATTR", status)
	}
}

// TestForgetReleasesRemovedOpenDir pins the cleanup: once the kernel drops its
// last reference, the remembered entry goes with it.
func TestForgetReleasesRemovedOpenDir(t *testing.T) {
	wfs, inode := newRemovedOpenDir(t)

	wfs.Forget(inode, 1)

	in := &fuse.SetAttrIn{}
	in.NodeId = inode
	in.Valid = fuse.FATTR_MODE
	in.Mode = 0700
	var out fuse.AttrOut
	if status := wfs.SetAttr(nil, in, &out); status != fuse.ENOENT {
		t.Fatalf("SetAttr after forget: got %v, want ENOENT", status)
	}
	if len(wfs.removedDirs) != 0 {
		t.Fatalf("removedDirs not cleaned up: %d entries", len(wfs.removedDirs))
	}
}

// TestRememberRemovedDirAfterForget pins the insert-forget race: an insert
// that loses to the final forget must not outlive the inode.
func TestRememberRemovedDirAfterForget(t *testing.T) {
	wfs, inode := newRemovedOpenDir(t)

	wfs.Forget(inode, 1)
	wfs.rememberRemovedDir(inode, &filer_pb.Entry{
		Name:        "dir",
		IsDirectory: true,
		Attributes:  &filer_pb.FuseAttributes{},
	})

	if len(wfs.removedDirs) != 0 {
		t.Fatalf("removedDirs kept an entry past the final forget: %d entries", len(wfs.removedDirs))
	}
}
