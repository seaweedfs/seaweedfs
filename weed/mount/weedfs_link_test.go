package mount

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
	linked, _, status := wfs.maybeLoadEntry(context.Background(), sourcePath)
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
	authoritative, _, status := wfs.maybeLoadEntry(context.Background(), sourcePath)
	if status != fuse.OK {
		t.Fatalf("reload source: %v", status)
	}
	if authoritative.Attributes.Inode != 0 {
		t.Fatalf("source Attributes.Inode = %d, want 0: an entry written outside a mount carries no inode",
			authoritative.Attributes.Inode)
	}
	authoritative.HardLinkCounter = 4

	wfs.syncHardLinkSiblings(context.Background(), authoritative.Attributes.Inode, authoritative, 0, skippedSource, secondLinkPath)
	for _, p := range siblings {
		if counter := cachedLink(t, wfs, p).HardLinkCounter; counter != 3 {
			t.Errorf("keyed by the persisted inode the sync reached %s (counter %d), which it cannot: "+
				"GetAllPaths(%d) has no paths", p, counter, authoritative.Attributes.Inode)
		}
	}

	wfs.syncHardLinkSiblings(context.Background(), sourceInode, authoritative, 0, skippedSource, secondLinkPath)
	for _, p := range siblings {
		if counter := cachedLink(t, wfs, p).HardLinkCounter; counter != 4 {
			t.Errorf("%s HardLinkCounter = %d after the sibling sync, want 4", p, counter)
		}
	}
}

// When the sibling refresh cannot reach the filer, the fallback patches the
// cached counter without proving the content reflects the mutation — so it
// keeps the version the sibling already held: claiming the mutation's
// position would fence an event still in flight for the sibling, while
// claiming nothing would let a replayed older delete remove the live link.
// The test filer answers every LookupDirectoryEntry with Unimplemented, so
// the sync always lands on that fallback.
func TestSyncHardLinkSiblingFetchFailureKeepsVersion(t *testing.T) {
	wfs, _ := newCreateTestWFS(t)

	hardLinkId := filer.HardLinkId([]byte{9, 9, 9, 9})
	sibPath := util.FullPath("/sib.txt")
	if err := wfs.metaCache.InsertEntry(context.Background(), &filer.Entry{
		FullPath:        sibPath,
		Attr:            filer.Attr{Crtime: time.Unix(1, 0), Mtime: time.Unix(1, 0), Mode: 0o644, FileSize: 12, Uid: 99, Gid: 100},
		HardLinkId:      hardLinkId,
		HardLinkCounter: 2,
	}, 7000); err != nil {
		t.Fatalf("seed sibling: %v", err)
	}
	inode := wfs.inodeToPath.Lookup(sibPath, 1, false, false, 77, true)
	if inode == 0 {
		t.Fatal("Lookup handed out node id 0")
	}

	authoritative := &filer_pb.Entry{
		HardLinkId:      hardLinkId,
		HardLinkCounter: 4,
		Attributes:      &filer_pb.FuseAttributes{Ctime: 8},
	}
	wfs.syncHardLinkSiblings(context.Background(), inode, authoritative, 9000)

	if counter := cachedLink(t, wfs, sibPath).HardLinkCounter; counter != 4 {
		t.Fatalf("sibling HardLinkCounter = %d after the fallback patch, want 4", counter)
	}

	// The patch claimed only the version the sibling already held (7000), so
	// a delayed delete replayed from before that point is fenced out instead
	// of removing the live link.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
		Directory: "/",
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "sib.txt"},
		},
		TsNs: 6000,
	}, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply delayed delete: %v", err)
	}
	if sib := cachedLink(t, wfs, sibPath); sib == nil {
		t.Fatal("replayed older delete removed the live link")
	}

	// The mutation committed at 9000 but the patch proves nothing about the
	// sibling's content: an event at 8000 still in flight must land.
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), &filer_pb.SubscribeMetadataResponse{
		Directory: "/",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:            "sib.txt",
				HardLinkId:      hardLinkId,
				HardLinkCounter: 2,
				Attributes:      &filer_pb.FuseAttributes{Crtime: 1, Mtime: 5, FileMode: 0o644, FileSize: 99, Uid: 99, Gid: 100},
			},
		},
		TsNs: 8000,
	}, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply pending event: %v", err)
	}
	if sib := cachedLink(t, wfs, sibPath); sib.FileSize != 99 {
		t.Errorf("pending event was fenced by the patched version: FileSize = %d, want 99", sib.FileSize)
	}
}

// Two mounts link a file that has one name at the same moment. Each gives the
// file a link id of its own, and the filer refuses the second: the file has
// the first one's by then. The mount reads the entry again and makes its name
// under the id the file has, instead of failing the link or, as before the
// filer refused, moving the file to an id of its own and splitting its names
// over two.
func TestFirstLinkTakesTheLinkIdAnotherClientGaveTheFile(t *testing.T) {
	t.Run("one filer", func(t *testing.T) {
		wfs, server := newCreateTestWFS(t)
		testFirstLinkTakesTheLinkId(t, wfs, server)
	})
	// the refusal stays the answer when the next filer cannot be reached
	t.Run("next filer down", func(t *testing.T) {
		wfs, server := newCreateTestWFS(t)
		down, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		downPort := down.Addr().(*net.TCPAddr).Port
		down.Close()
		wfs.option.FilerAddresses = append(wfs.option.FilerAddresses, pb.NewServerAddressWithGrpcPort("127.0.0.1:1", downPort))
		testFirstLinkTakesTheLinkId(t, wfs, server)
	})
}

func testFirstLinkTakesTheLinkId(t *testing.T, wfs *WFS, server *createEntryTestServer) {
	sourcePath, sourceInode := linkTestSource(t, wfs, "shared.txt", 0)

	theirs := filer.NewHardLinkId()
	var mu sync.Mutex
	var updates []*filer_pb.Entry
	server.updateEntry = func(req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		updates = append(updates, proto.Clone(req.GetEntry()).(*filer_pb.Entry))
		if !bytes.Equal(req.GetEntry().GetHardLinkId(), theirs) {
			return nil, status.Errorf(codes.FailedPrecondition, "already a hard link: %s", sourcePath)
		}
		return &filer_pb.UpdateEntryResponse{}, nil
	}
	server.lookupEntry = func(req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{
			Name:            req.GetName(),
			Attributes:      &filer_pb.FuseAttributes{FileMode: 0o600, FileSize: 12, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100},
			Chunks:          []*filer_pb.FileChunk{{FileId: "3,0c03", Size: 12}},
			Content:         []byte("inline"),
			RemoteEntry:     &filer_pb.RemoteEntry{StorageName: "s3", RemoteETag: "etag"},
			HardLinkId:      theirs,
			HardLinkCounter: 2,
		}}, nil
	}

	out := doLink(t, wfs, sourceInode, "mine.txt")
	if out.NodeId != sourceInode {
		t.Fatalf("Link NodeId = %d, want the source node id %d", out.NodeId, sourceInode)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(updates) != 2 {
		t.Fatalf("the source was updated %d times, want the refused update and one more", len(updates))
	}
	if len(updates[0].GetHardLinkId()) == 0 || bytes.Equal(updates[0].GetHardLinkId(), theirs) {
		t.Fatalf("first update carried link id %x, want one made by this mount", updates[0].GetHardLinkId())
	}
	if !bytes.Equal(updates[1].GetHardLinkId(), theirs) || updates[1].GetHardLinkCounter() != 3 {
		t.Fatalf("second update carried link id %x count %d, want the file's %x and 3", updates[1].GetHardLinkId(), updates[1].GetHardLinkCounter(), theirs)
	}
	if updates[1].GetAttributes().GetFileMode() != 0o600 || len(updates[1].GetChunks()) != 1 || string(updates[1].GetContent()) != "inline" {
		t.Fatalf("second update carried mode %o, %d chunks and content %q, want the stored 600, 1 and \"inline\"", updates[1].GetAttributes().GetFileMode(), len(updates[1].GetChunks()), updates[1].GetContent())
	}
	server.mu.Lock()
	created := server.lastEntry
	server.mu.Unlock()
	if created.GetName() != "mine.txt" || !bytes.Equal(created.GetHardLinkId(), theirs) {
		t.Fatalf("new name %q was made under link id %x, want %x", created.GetName(), created.GetHardLinkId(), theirs)
	}
	if created.GetAttributes().GetFileMode() != 0o600 || len(created.GetChunks()) != 1 || string(created.GetContent()) != "inline" {
		t.Fatalf("new name has mode %o, %d chunks and content %q, want the stored 600, 1 and \"inline\"", created.GetAttributes().GetFileMode(), len(created.GetChunks()), created.GetContent())
	}
	if created.GetRemoteEntry().GetRemoteETag() != "etag" {
		t.Fatalf("new name has remote entry %v, want the stored one", created.GetRemoteEntry())
	}
	linked, _, st := wfs.maybeLoadEntry(context.Background(), sourcePath)
	if st != fuse.OK {
		t.Fatalf("reload source: %v", st)
	}
	if !bytes.Equal(linked.HardLinkId, theirs) {
		t.Fatalf("the mount has the source under link id %x, want %x", linked.HardLinkId, theirs)
	}
}

// A refusal with nothing behind it (the entry read again has no link id)
// fails the link: nothing is made under an id the filer did not take.
func TestFirstLinkRefusedWithoutALinkToTakeFails(t *testing.T) {
	wfs, server := newCreateTestWFS(t)
	_, sourceInode := linkTestSource(t, wfs, "shared.txt", 0)

	server.updateEntry = func(req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
		return nil, status.Errorf(codes.FailedPrecondition, "precondition failed")
	}
	server.lookupEntry = func(req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{
			Name:       req.GetName(),
			Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, FileSize: 12, Crtime: 1, Mtime: 1, Uid: 99, Gid: 100},
		}}, nil
	}

	st := wfs.Link(make(chan struct{}), &fuse.LinkIn{
		InHeader:  fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 99, Gid: 100}}},
		Oldnodeid: sourceInode,
	}, "mine.txt", &fuse.EntryOut{})
	if st == fuse.OK {
		t.Fatal("Link reported success after the filer refused the source's update")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.lastEntry != nil {
		t.Fatalf("name %q was made after the refusal", server.lastEntry.GetName())
	}
}
