//go:build !freebsd

package mount

import (
	"context"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// peerUnlink removes name on the filer the way another mount does and
// delivers the delete event this mount's subscription receives.
func peerUnlink(t *testing.T, wfs *WFS, fake *hardLinkFiler, name string) {
	t.Helper()
	applyPeerUnlink(t, wfs, peerUnlinkEvent(t, fake, name))
}

func peerUnlinkEvent(t *testing.T, fake *hardLinkFiler, name string) *filer_pb.SubscribeMetadataResponse {
	t.Helper()
	old := fake.stored(name)
	if _, err := fake.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{Directory: "/", Name: name}); err != nil {
		t.Fatalf("peer delete %s: %v", name, err)
	}
	fake.mu.Lock()
	fake.stamp++
	ts := fake.stamp
	fake.mu.Unlock()
	return &filer_pb.SubscribeMetadataResponse{Directory: "/", TsNs: ts, EventNotification: &filer_pb.EventNotification{OldEntry: old}}
}

func applyPeerUnlink(t *testing.T, wfs *WFS, del *filer_pb.SubscribeMetadataResponse) {
	t.Helper()
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), del, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply peer delete: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
}

// Another mount removes one of the two names of a file open here. The handle
// keeps publishing through the other name, over the link count and ctime the
// filer stored there, and the removed name stays removed.
func TestPeerUnlinkedAliasKeepsOpenHandlePublishing(t *testing.T) {
	for _, removed := range []string{"tmp", "final"} {
		for _, dirty := range []bool{false, true} {
			t.Run(removed+map[bool]string{false: "/clean", true: "/dirty"}[dirty], func(t *testing.T) {
				wfs, fake, fh, inode := openLinkTestFile(t)
				linkInRoot(t, wfs, inode, "final")
				chunks := 2
				if dirty {
					fh.UpdateEntry(func(e *filer_pb.Entry) {
						e.Chunks = append(e.Chunks, linkHandleChunk(9, 100))
						e.Attributes.FileSize = 200
					})
					fh.dirtyMetadata = true
					chunks = 3
				}
				survivor := map[string]string{"tmp": "final", "final": "tmp"}[removed]
				peerUnlink(t, wfs, fake, removed)
				if fh.isDeleted {
					t.Fatal("a peer unlinking one of two names marked the open handle deleted")
				}
				unlinked := fake.stored(survivor).Attributes
				if code := appendAndFlush(t, wfs, fh, 2); code != fuse.OK {
					t.Fatalf("flush after the peer unlink: %v", code)
				}
				wantStored(t, fake, survivor, chunks, 1)
				if got := fake.stored(survivor).Attributes; got.Ctime != unlinked.Ctime || got.CtimeNs != unlinked.CtimeNs {
					t.Fatalf("the flush moved the ctime from the unlink's %d.%09d back to %d.%09d", unlinked.Ctime, unlinked.CtimeNs, got.Ctime, got.CtimeNs)
				}
				if fake.stored(removed) != nil {
					t.Fatalf("the flush recreated %s", removed)
				}
			})
		}
	}
}

// The surviving name was found by a lookup, not linked by this mount.
func TestPeerUnlinkedAliasKeepsLookedUpSurvivor(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	linkInRoot(t, wfs, inode, "final")
	wfs.inodeToPath.RemovePath("/final", nil)
	wfs.inodeToPath.Lookup("/final", 1, false, true, inode, true)
	peerUnlink(t, wfs, fake, "tmp")
	if fh.isDeleted {
		t.Fatal("a peer unlinking the open name of a file with a looked-up second name marked the handle deleted")
	}
	if code := appendAndFlush(t, wfs, fh, 2); code != fuse.OK {
		t.Fatalf("flush after the peer unlink: %v", code)
	}
	wantStored(t, fake, "final", 2, 1)
	if fake.stored("tmp") != nil {
		t.Fatal("the flush recreated tmp")
	}
}

// With no other name known here, the handle stops rather than recreating the
// removed one.
func TestPeerUnlinkedAliasWithUnknownSurvivorStops(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	linkInRoot(t, wfs, inode, "final")
	wfs.inodeToPath.RemovePath("/final", nil)
	peerUnlink(t, wfs, fake, "tmp")
	if !fh.isDeleted {
		t.Fatal("a handle with no known surviving name was left publishing")
	}
	if code := appendAndFlush(t, wfs, fh, 2); code != fuse.OK {
		t.Fatalf("flush: %v", code)
	}
	if fake.stored("tmp") != nil {
		t.Fatal("the flush recreated tmp")
	}
}

// A peer removes one name, then this mount unlinks the last one.
func TestPeerThenLocalUnlinkKeepsHandleDeleted(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	linkInRoot(t, wfs, inode, "final")
	peerUnlink(t, wfs, fake, "final")
	unlinkInRoot(t, wfs, "tmp")
	if !fh.isDeleted {
		t.Fatal("the handle of a file with no names left is not marked deleted")
	}
	if code := appendAndFlush(t, wfs, fh, 2); code != fuse.OK {
		t.Fatalf("flush after the last unlink: %v", code)
	}
	for _, name := range []string{"tmp", "final"} {
		if fake.stored(name) != nil {
			t.Fatalf("the flush recreated %s", name)
		}
	}
}

func TestPeerUnlinkLastNameKeepsHandleDeleted(t *testing.T) {
	wfs, fake, fh, _ := openLinkTestFile(t)
	peerUnlink(t, wfs, fake, "tmp")
	if !fh.isDeleted {
		t.Fatal("a peer unlinking the only name did not mark the handle deleted")
	}
}

// The periodic metadata flush publishes through the survivor as well.
func TestPeerUnlinkedAliasKeepsPeriodicFlushPublishing(t *testing.T) {
	wfs, fake, fh, inode := openLinkTestFile(t)
	linkInRoot(t, wfs, inode, "final")
	peerUnlink(t, wfs, fake, "tmp")
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		e.Chunks = append(e.Chunks, linkHandleChunk(2, 100))
		e.Attributes.FileSize = 200
	})
	fh.dirtyMetadata = true
	if err := wfs.flushFileMetadata(fh); err != nil {
		t.Fatalf("periodic flush after the peer unlink: %v", err)
	}
	wantStored(t, fake, "final", 2, 1)
	if fake.stored("tmp") != nil {
		t.Fatal("the periodic flush recreated tmp")
	}
}

// A peer unlink rotates the preferred name even when the mount has no open
// handle. A subsequent Open by inode then finds the surviving name.
func TestPeerUnlinkedAliasKeepsClosedInodeReadable(t *testing.T) {
	wfs, fake, inode := closedLinkTestFile(t, "final")
	fake.mu.Lock()
	entry := fake.resolve("tmp")
	entry.Attributes.Inode = 0 // entries created outside FUSE can omit this
	fake.store(entry)
	fake.mu.Unlock()
	peerUnlink(t, wfs, fake, "tmp")
	requireClosedLinkOpensAt(t, wfs, inode, "/final")
}

func closedLinkTestFile(t *testing.T, names ...string) (*WFS, *hardLinkFiler, uint64) {
	t.Helper()
	wfs, fake, fh, inode := openLinkTestFile(t)
	for _, name := range names {
		linkInRoot(t, wfs, inode, name)
	}
	wfs.fhMap.ReleaseByHandle(fh.fh)
	wfs.openMtimeCache = make(map[uint64][2]int64)
	return wfs, fake, inode
}

func requireClosedLinkOpensAt(t *testing.T, wfs *WFS, inode uint64, want util.FullPath) {
	t.Helper()
	var opened fuse.OpenOut
	if status := wfs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: inode}}, &opened); status != fuse.OK {
		t.Fatalf("open survivor: %v", status)
	}
	defer wfs.ReleaseHandle(FileHandleId(opened.Fh))
	handle := wfs.GetHandle(FileHandleId(opened.Fh))
	if path := util.NewFullPath(handle.savedDir, handle.savedName); path != want {
		t.Fatalf("opened %q, want %q", path, want)
	}
}

func TestPeerUnlinkedAliasLeavesRenamedSurvivor(t *testing.T) {
	wfs, fake, inode := closedLinkTestFile(t, "final", "other")
	del := peerUnlinkEvent(t, fake, "tmp")

	fake.mu.Lock()
	fake.records["tmp"] = fake.records["final"]
	delete(fake.records, "final")
	fake.mu.Unlock()
	wfs.inodeToPath.MovePath("/final", "/tmp")

	applyPeerUnlink(t, wfs, del)
	requireClosedLinkOpensAt(t, wfs, inode, "/tmp")
}

func TestPeerUnlinkedAliasOpensOriginalAfterNameReused(t *testing.T) {
	wfs, fake, inode := closedLinkTestFile(t, "final")
	del := peerUnlinkEvent(t, fake, "tmp")

	fake.mu.Lock()
	fake.store(&filer_pb.Entry{Name: "tmp", Attributes: &filer_pb.FuseAttributes{Inode: 43, FileMode: 0o600}})
	fake.mu.Unlock()
	applyPeerUnlink(t, wfs, del)

	var replacement fuse.EntryOut
	if status := wfs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "tmp", &replacement); status != fuse.OK {
		t.Fatalf("lookup replacement: %v", status)
	}
	if replacement.NodeId == inode {
		t.Fatal("replacement is still routed to the old inode")
	}
	requireClosedLinkOpensAt(t, wfs, replacement.NodeId, "/tmp")
	requireClosedLinkOpensAt(t, wfs, inode, "/final")
}

func TestPeerUnlinkedAliasCanBeCreatedAgain(t *testing.T) {
	wfs, fake, inode := closedLinkTestFile(t, "final")
	peerUnlink(t, wfs, fake, "tmp")
	peerUnlink(t, wfs, fake, "final")

	parent := fuse.InHeader{NodeId: 1}
	if status := wfs.Lookup(nil, &parent, "tmp", &fuse.EntryOut{}); status != fuse.ENOENT {
		t.Errorf("lookup vacant name: %v, want ENOENT", status)
	}
	wfs.option.DefaultPermissions = true
	wfs.option.EagerFilerCreate = true
	var created fuse.CreateOut
	if status := wfs.Create(nil, &fuse.CreateIn{InHeader: parent, Mode: 0o600}, "tmp", &created); status != fuse.OK {
		t.Fatalf("create vacant name: %v", status)
	}
	defer wfs.ReleaseHandle(FileHandleId(created.Fh))
	if created.NodeId == inode {
		t.Fatal("create reused the old node ID")
	}
	if status := wfs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: inode}}, &fuse.OpenOut{}); status != fuse.ENOENT {
		t.Fatalf("open old inode: %v, want ENOENT", status)
	}
}

func TestPeerUnlinkedAliasCanNameNewHardLinkGroup(t *testing.T) {
	wfs, fake, inode := closedLinkTestFile(t, "final", "other")
	wfs.inodeToPath.RemovePath("/other", nil)
	peerUnlink(t, wfs, fake, "tmp")
	peerUnlink(t, wfs, fake, "final")
	fake.mu.Lock()
	for _, name := range []string{"tmp", "final"} {
		fake.store(&filer_pb.Entry{
			Name: name, HardLinkId: []byte("replacement"), HardLinkCounter: 2,
			Attributes: &filer_pb.FuseAttributes{Inode: inode, FileMode: 0o600},
		})
	}
	fake.mu.Unlock()

	if status := wfs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: inode}}, &fuse.OpenOut{}); status != fuse.ENOENT {
		t.Errorf("open old inode before pathname lookup: %v, want ENOENT", status)
	}
	parent := fuse.InHeader{NodeId: 1}
	var replacement fuse.EntryOut
	if status := wfs.Lookup(nil, &parent, "tmp", &replacement); status != fuse.OK {
		t.Fatalf("lookup replacement: %v", status)
	}
	if replacement.NodeId == inode {
		t.Fatal("replacement reused the old node ID")
	}
}

func TestPeerUnlinkedAliasRecoversDelayedRenameAfterNameLookup(t *testing.T) {
	wfs, fake, inode := closedLinkTestFile(t, "final")
	peerUnlink(t, wfs, fake, "tmp")
	fake.mu.Lock()
	oldEntry := fake.resolve("final")
	fake.records["renamed"] = fake.records["final"]
	delete(fake.records, "final")
	newEntry := fake.resolve("renamed")
	fake.stamp++
	ts := fake.stamp
	fake.mu.Unlock()

	if status := wfs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "tmp", &fuse.EntryOut{}); status != fuse.ENOENT {
		t.Fatalf("lookup vacant name before rename event: %v, want ENOENT", status)
	}
	event := &filer_pb.SubscribeMetadataResponse{Directory: "/", TsNs: ts, EventNotification: &filer_pb.EventNotification{
		OldEntry: oldEntry, NewEntry: newEntry, NewParentPath: "/",
	}}
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), event, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply peer rename: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
	requireClosedLinkOpensAt(t, wfs, inode, "/renamed")
}

func TestPeerUnlinkedAliasListingWaitsForLookup(t *testing.T) {
	dir := util.FullPath("/d")
	wfs := newPagingWFS(t, dir, []string{"tmp", "final"}, 0)
	inode := wfs.inodeToPath.Lookup("/d/tmp", 1, false, false, 42, true)
	wfs.inodeToPath.AddPath(inode, "/d/final")
	wfs.inodeToPath.rememberPeerUnlink("/d/tmp", []byte("deleted-group"))
	parent, _ := wfs.inodeToPath.GetInode(dir)
	handle, _ := wfs.AcquireDirectoryHandle()
	defer wfs.ReleaseDirectoryHandle(handle)
	sink := &benchSink{sinkLimit: 4, takesRef: true}
	in := &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: parent}, Fh: uint64(handle), Offset: directoryStreamBaseOffset, Size: 4096}
	if status := wfs.ReadDirectoryInto(in, sink, true); status != fuse.OK {
		t.Fatalf("readdirplus: %v", status)
	}
	if len(sink.attrs) != 2 || sink.attrs[0].NodeId != inode || sink.attrs[1].NodeId != 0 {
		t.Fatalf("readdirplus cached a pending name or lost the survivor: %+v", sink.attrs)
	}
}

func TestPeerUnlinkedAliasRetriesAfterFilerError(t *testing.T) {
	wfs, fake, inode := closedLinkTestFile(t, "final")
	fake.mu.Lock()
	fake.failName, fake.failLookups = "tmp", 100
	fake.mu.Unlock()
	peerUnlink(t, wfs, fake, "tmp")
	if _, _, status := wfs.AcquireHandle(context.Background(), inode, 0, 0, 0); status != fuse.EIO {
		t.Fatalf("open while filer lookup fails: %v, want EIO", status)
	}
	if got, found := wfs.inodeToPath.GetInode("/tmp"); !found || got != inode {
		t.Fatalf("failed filer lookup removed /tmp from inode %d", inode)
	}

	fake.mu.Lock()
	fake.failLookups = 0
	fake.mu.Unlock()
	requireClosedLinkOpensAt(t, wfs, inode, "/final")
}

func TestPeerUnlinkedAliasRetriesAfterDiscoveringAnotherName(t *testing.T) {
	wfs, fake, inode := closedLinkTestFile(t, "final", "other")
	wfs.inodeToPath.RemovePath("/other", nil)
	peerUnlink(t, wfs, fake, "tmp")

	fake.mu.Lock()
	missing := 0
	fake.onMissing = func(name string) {
		if name == "tmp" {
			missing++
			if missing == 1 {
				wfs.inodeToPath.AddPath(inode, "/other")
			}
		}
	}
	fake.mu.Unlock()

	requireClosedLinkOpensAt(t, wfs, inode, "/final")
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if missing != 2 {
		t.Fatalf("verified the removed name %d times, want 2", missing)
	}
	if got, found := wfs.inodeToPath.GetInode("/other"); !found || got != inode {
		t.Fatalf("discovered alias inode = %d (%t), want %d", got, found, inode)
	}
}
