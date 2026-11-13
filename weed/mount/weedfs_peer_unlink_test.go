//go:build !freebsd

package mount

import (
	"context"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// peerUnlink removes name on the filer the way another mount does and
// delivers the delete event this mount's subscription receives.
func peerUnlink(t *testing.T, wfs *WFS, fake *hardLinkFiler, name string) {
	t.Helper()
	old := fake.stored(name)
	if _, err := fake.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{Directory: "/", Name: name}); err != nil {
		t.Fatalf("peer delete %s: %v", name, err)
	}
	fake.mu.Lock()
	fake.stamp++
	ts := fake.stamp
	fake.mu.Unlock()
	del := &filer_pb.SubscribeMetadataResponse{Directory: "/", TsNs: ts, EventNotification: &filer_pb.EventNotification{OldEntry: old}}
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
