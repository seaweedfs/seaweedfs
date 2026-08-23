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

// cachedLink returns what the mount would serve for path out of its own meta
// cache, which is what a stat on that name reads before the kernel's attr
// cache expires.
func cachedLink(t *testing.T, wfs *WFS, path util.FullPath) *filer.Entry {
	t.Helper()

	entry, _, err := wfs.metaCache.FindEntry(context.Background(), path)
	if err != nil {
		t.Fatalf("FindEntry(%s): %v", path, err)
	}
	if entry == nil {
		t.Fatalf("FindEntry(%s): not cached", path)
	}
	return entry
}

// A third link is the first case that reaches the body of
// syncHardLinkSiblings: with two links the source alias and the name just
// created are both in skipPaths, so the loop iterates over nothing. Three
// links leave one name that no other part of Link() writes and that only the
// sibling sync visits.
//
// The source is written outside a mount, so it persists Attributes.Inode 0,
// the number Link() used to key the sync by. GetAllPaths(0) yields nothing,
// which is what the second half of this test pins down: the sync has to be
// keyed by the kernel node id, the number inodeToPath is indexed by.
func TestLinkSyncsHardLinkSiblingsOnThirdLink(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)

	sourcePath, sourceInode := linkTestSource(t, wfs, "s3-uploaded.txt", 0)

	firstLinkPath := util.FullPath("/hardlink-1.txt")
	doLink(t, wfs, sourceInode, "hardlink-1.txt")

	// Two links so far: nothing has exercised the sibling loop yet, Link()
	// wrote both of these names itself.
	if counter := cachedLink(t, wfs, firstLinkPath).HardLinkCounter; counter != 2 {
		t.Fatalf("after the first link, %s HardLinkCounter = %d, want 2", firstLinkPath, counter)
	}

	// Link() resolves its source through GetPath and skips that path plus the
	// name it is about to create. Whatever is left over is the sibling loop's
	// work.
	skippedSource, status := wfs.inodeToPath.GetPath(sourceInode)
	if status != fuse.OK {
		t.Fatalf("GetPath(%d) = %v, want OK", sourceInode, status)
	}
	secondLinkPath := util.FullPath("/hardlink-2.txt")

	doLink(t, wfs, sourceInode, "hardlink-2.txt")

	paths := wfs.inodeToPath.GetAllPaths(sourceInode)
	if len(paths) != 3 {
		t.Fatalf("inode %d has paths %v, want %s, %s and %s",
			sourceInode, paths, sourcePath, firstLinkPath, secondLinkPath)
	}

	// Guard against this test covering nothing: at least one name has to fall
	// outside skipPaths, or the sibling loop is a no-op again and everything
	// below only re-checks Link()'s own two writes.
	var siblings []util.FullPath
	for _, p := range paths {
		if p == skippedSource || p == secondLinkPath {
			continue
		}
		siblings = append(siblings, p)
	}
	if len(siblings) == 0 {
		t.Fatalf("every path in %v is a skipPath (%s, %s); the sibling loop has no work to do",
			paths, skippedSource, secondLinkPath)
	}

	// Every name of the file reports three links, the sibling included. A stale
	// counter here is the pjdfstest link/00.t failure: stat on an older link
	// still says nlink 2.
	for _, p := range paths {
		if counter := cachedLink(t, wfs, p).HardLinkCounter; counter != 3 {
			t.Errorf("%s HardLinkCounter = %d, want 3", p, counter)
		}
	}

	// Which inode the sync is keyed by, driven directly. Going through Link()
	// cannot tell the two keys apart: the meta cache keeps one blob per hard
	// link id (FilerStoreWrapper.setHardLink/maybeReadHardLink), so a read of
	// any sibling returns the attributes of the last write to any of them,
	// synced or not. Feeding the loop an authoritative counter that no other
	// write has stored is what makes its work visible.
	authoritative, _, status := wfs.maybeLoadEntry(sourcePath)
	if status != fuse.OK {
		t.Fatalf("reload source: %v", status)
	}
	if authoritative.Attributes.Inode != 0 {
		t.Fatalf("source Attributes.Inode = %d, want 0: an entry written outside a mount carries no inode",
			authoritative.Attributes.Inode)
	}
	authoritative.HardLinkCounter = 4

	wfs.syncHardLinkSiblings(authoritative.Attributes.Inode, authoritative, skippedSource, secondLinkPath)
	for _, p := range siblings {
		if counter := cachedLink(t, wfs, p).HardLinkCounter; counter != 3 {
			t.Errorf("keyed by the persisted inode the sync reached %s (counter %d), which it cannot: "+
				"GetAllPaths(%d) has no paths", p, counter, authoritative.Attributes.Inode)
		}
	}

	wfs.syncHardLinkSiblings(sourceInode, authoritative, skippedSource, secondLinkPath)
	for _, p := range siblings {
		if counter := cachedLink(t, wfs, p).HardLinkCounter; counter != 4 {
			t.Errorf("%s HardLinkCounter = %d after the sibling sync, want 4", p, counter)
		}
	}
}
