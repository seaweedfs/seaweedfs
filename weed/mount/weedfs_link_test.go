package mount

import (
	"context"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// linkTestSource seeds the meta cache with an entry and registers it with
// inodeToPath the way a kernel Lookup would, returning the node id the kernel
// would then use as LinkIn.Oldnodeid.
func linkTestSource(t *testing.T, wfs *WFS, name string, persistedInode uint64) (util.FullPath, uint64) {
	t.Helper()

	entry := &filer_pb.Entry{
		Name: name,
		Attributes: &filer_pb.FuseAttributes{
			FileMode: 0o644,
			FileSize: 12,
			Inode:    persistedInode,
			Crtime:   1,
			Mtime:    1,
			Uid:      99,
			Gid:      100,
		},
	}
	if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry("/", entry), 0); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}

	fullPath := util.FullPath("/").Child(name)
	inode := wfs.inodeToPath.Lookup(fullPath, entry.Attributes.Crtime, false, false, persistedInode, true)
	if inode == 0 {
		t.Fatal("Lookup handed out node id 0")
	}
	return fullPath, inode
}

func doLink(t *testing.T, wfs *WFS, oldNodeId uint64, name string) *fuse.EntryOut {
	t.Helper()

	out := &fuse.EntryOut{}
	status := wfs.Link(make(chan struct{}), &fuse.LinkIn{
		InHeader: fuse.InHeader{
			NodeId: 1,
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}},
		},
		Oldnodeid: oldNodeId,
	}, name, out)
	if status != fuse.OK {
		t.Fatalf("Link status = %v, want OK", status)
	}
	return out
}

// Entries written outside a mount (S3 API, WebDAV, a direct filer call) persist
// Attributes.Inode == 0, because the inode is a mount-runtime number that only
// lives in inodeToPath. Replying to LINK with NodeId 0 makes the kernel fail the
// call with EIO (invalid_nodeid), which is what
// https://github.com/seaweedfs/seaweedfs/issues/8404 reports. The reply has to
// name the node id the kernel already holds for the source.
func TestLinkReportsKernelNodeIdWhenEntryHasNoPersistedInode(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)

	sourcePath, sourceInode := linkTestSource(t, wfs, "s3-uploaded.txt", 0)

	out := doLink(t, wfs, sourceInode, "hardlink.txt")

	if out.NodeId == 0 {
		t.Fatal("Link replied with NodeId 0; the kernel rejects that with EIO")
	}
	if out.NodeId != sourceInode {
		t.Fatalf("Link NodeId = %d, want the source node id %d", out.NodeId, sourceInode)
	}
	if out.Attr.Ino != sourceInode {
		t.Fatalf("Link Attr.Ino = %d, want %d", out.Attr.Ino, sourceInode)
	}
	if out.Attr.Nlink != 2 {
		t.Fatalf("Link Attr.Nlink = %d, want 2", out.Attr.Nlink)
	}

	// The new name must resolve to the same inode, or a later Lookup on it
	// hands the kernel node id 0 and the link reads back as missing.
	linkPath := util.FullPath("/hardlink.txt")
	linkInode, found := wfs.inodeToPath.GetInode(linkPath)
	if !found {
		t.Fatal("new link was not registered in inodeToPath")
	}
	if linkInode != sourceInode {
		t.Fatalf("new link inode = %d, want the source inode %d", linkInode, sourceInode)
	}
	if wfs.inodeToPath.HasInode(0) {
		t.Fatal("inode 0 was registered in inodeToPath")
	}

	paths := wfs.inodeToPath.GetAllPaths(sourceInode)
	if len(paths) != 2 {
		t.Fatalf("inode %d has paths %v, want both %s and %s", sourceInode, paths, sourcePath, linkPath)
	}

	// The source still resolves, and it now carries a hard link id.
	if _, status := wfs.inodeToPath.GetPath(sourceInode); status != fuse.OK {
		t.Fatalf("GetPath(%d) = %v, want OK", sourceInode, status)
	}
	linked, _, status := wfs.maybeLoadEntry(sourcePath)
	if status != fuse.OK {
		t.Fatalf("reload source: %v", status)
	}
	if len(linked.HardLinkId) == 0 {
		t.Fatal("source entry was not converted to hard link mode")
	}
	if linked.HardLinkCounter != 2 {
		t.Fatalf("source HardLinkCounter = %d, want 2", linked.HardLinkCounter)
	}
}

// A source created through the mount does carry a persisted inode. The reply
// must keep naming the node id the kernel holds, which for this case is the
// same number.
func TestLinkReportsKernelNodeIdWhenEntryHasPersistedInode(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)

	_, sourceInode := linkTestSource(t, wfs, "mount-created.txt", 8404)
	if sourceInode != 8404 {
		t.Fatalf("source node id = %d, want the persisted inode 8404", sourceInode)
	}

	out := doLink(t, wfs, sourceInode, "hardlink.txt")

	if out.NodeId != sourceInode {
		t.Fatalf("Link NodeId = %d, want %d", out.NodeId, sourceInode)
	}
	if out.Attr.Ino != sourceInode {
		t.Fatalf("Link Attr.Ino = %d, want %d", out.Attr.Ino, sourceInode)
	}
	if linkInode, found := wfs.inodeToPath.GetInode(util.FullPath("/hardlink.txt")); !found || linkInode != sourceInode {
		t.Fatalf("new link inode = %d (found=%v), want %d", linkInode, found, sourceInode)
	}
}
