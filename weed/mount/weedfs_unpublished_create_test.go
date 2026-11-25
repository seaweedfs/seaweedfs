package mount

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// openUnpublishedCreate sets up the handle Create leaves for a deferred
// create: dirty, with no base, holding writes the filer has not seen.
func openUnpublishedCreate(t *testing.T, wfs *WFS, path util.FullPath, data []byte) (*FileHandle, uint64) {
	t.Helper()
	inode := wfs.inodeToPath.Lookup(path, time.Now().Unix(), false, false, 0, true)
	_, name := path.DirAndName()
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       name,
		Attributes: &filer_pb.FuseAttributes{FileMode: 0o644, Inode: inode, Crtime: 1, Mtime: 1},
	}, 0, 0)
	fh.RememberPath(path)
	fh.dirtyMetadata = true
	fh.baseEntry.Store(nil)
	if len(data) > 0 {
		if _, err := fh.dirtyPages.AddPage(0, data, true, time.Now().UnixNano()); err != nil {
			t.Fatalf("AddPage: %v", err)
		}
	}
	fh.UpdateEntry(func(entry *filer_pb.Entry) {
		entry.Attributes.FileSize = uint64(len(data))
	})
	return fh, inode
}

func applyPeerEvent(t *testing.T, wfs *WFS, resp *filer_pb.SubscribeMetadataResponse) {
	t.Helper()
	if err := wfs.metaCache.ApplyMetadataResponse(context.Background(), resp, meta_cache.SubscriberMetadataResponseApplyOptions); err != nil {
		t.Fatalf("apply event: %v", err)
	}
	wfs.metaCache.WaitForEntryInvalidations()
}

// requireWritesKept checks the handle still holds its own unflushed writes,
// its own entry and no base, so its flush stays a create-if-not-exists.
func requireWritesKept(t *testing.T, fh *FileHandle, pages *PageWriter, data []byte) {
	t.Helper()
	if fh.dirtyPages != pages {
		t.Fatal("the handle's unflushed writes were discarded")
	}
	buf := make([]byte, len(data))
	if maxStop := fh.dirtyPages.ReadDirtyDataAt(buf, 0, time.Now().UnixNano()); maxStop != int64(len(data)) || !bytes.Equal(buf, data) {
		t.Fatalf("dirty data = %q (maxStop %d), want %q", buf, maxStop, data)
	}
	if fh.baseEntry.Load() != nil {
		t.Fatal("the handle took the peer's entry as its base")
	}
	if size := fh.GetEntry().GetEntry().Attributes.FileSize; size != uint64(len(data)) {
		t.Fatalf("live entry size = %d, want %d", size, len(data))
	}
	if fh.isDeleted {
		t.Fatal("the handle was marked deleted; its flush would be skipped")
	}
}

// Two mounts create the same name, each before the other publishes. The
// peer's create reaching this mount must not replace the writes of the file
// this mount created: its flush then publishes nothing and close() succeeds
// although the writes are gone.
func TestPeerCreateKeepsUnpublishedCreateWrites(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fh, inode := openUnpublishedCreate(t, wfs, util.FullPath("/dir/file"), []byte("BBBB"))
	pages := fh.dirtyPages

	applyPeerEvent(t, wfs, &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{
				Name:       "file",
				Attributes: &filer_pb.FuseAttributes{FileSize: 10, Inode: inode + 1000, Crtime: 2, Mtime: 2},
			},
			NewParentPath: "/dir",
		},
	})

	requireWritesKept(t, fh, pages, []byte("BBBB"))
}

// The peer deleted its own file at the name; the file this mount created was
// never on the filer and is not unlinked.
func TestPeerDeleteDoesNotUnlinkUnpublishedCreate(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fh, _ := openUnpublishedCreate(t, wfs, util.FullPath("/dir/file"), []byte("BBBB"))
	pages := fh.dirtyPages

	applyPeerEvent(t, wfs, &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: "file"},
		},
	})

	requireWritesKept(t, fh, pages, []byte("BBBB"))
}

// The peer renamed its own file away from the name; the file this mount
// created stays on its name instead of following the peer's file.
func TestPeerRenameAwayLeavesUnpublishedCreateOnItsName(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	path := util.FullPath("/dir/file")
	fh, inode := openUnpublishedCreate(t, wfs, path, []byte("BBBB"))
	pages := fh.dirtyPages

	applyPeerEvent(t, wfs, &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "file"},
			NewEntry:      &filer_pb.Entry{Name: "moved", Attributes: &filer_pb.FuseAttributes{FileSize: 10}},
			NewParentPath: "/dir",
		},
	})

	requireWritesKept(t, fh, pages, []byte("BBBB"))
	if got, status := wfs.inodeToPath.GetPath(inode); status != fuse.OK || got != path {
		t.Fatalf("inode resolves to %q (%v), want %s", got, status, path)
	}
}

// The peer renamed another file onto the name. That replaces nothing of the
// file this mount created, so its handle is not marked deleted; its flush is
// refused instead of skipped, so the writes are not dropped silently.
func TestPeerRenameOntoUnpublishedCreateDoesNotMarkItDeleted(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	fh, _ := openUnpublishedCreate(t, wfs, util.FullPath("/dir/file"), []byte("BBBB"))
	pages := fh.dirtyPages
	wfs.inodeToPath.Lookup(util.FullPath("/dir/other"), time.Now().Unix(), false, false, 0, true)

	applyPeerEvent(t, wfs, &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "other"},
			NewEntry:      &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 10}},
			NewParentPath: "/dir",
		},
	})

	requireWritesKept(t, fh, pages, []byte("BBBB"))
}

// Control: a file the filer has is destroyed by a rename over it, and its
// handle must not flush the old content back over what took the name.
func TestPeerRenameOntoPublishedFileMarksItDeleted(t *testing.T) {
	wfs := newInvalidateTestWFS(t)
	inode := wfs.inodeToPath.Lookup(util.FullPath("/dir/file"), time.Now().Unix(), false, false, 0, true)
	fh, _, _, _ := wfs.fhMap.AcquireFileHandle(wfs, inode, &filer_pb.Entry{
		Name:       "file",
		Attributes: &filer_pb.FuseAttributes{FileSize: 200, Inode: inode},
	}, 0, 0)
	fh.dirtyMetadata = true
	wfs.inodeToPath.Lookup(util.FullPath("/dir/other"), time.Now().Unix(), false, false, 0, true)

	applyPeerEvent(t, wfs, &filer_pb.SubscribeMetadataResponse{
		Directory: "/dir",
		TsNs:      1500,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: "other"},
			NewEntry:      &filer_pb.Entry{Name: "file", Attributes: &filer_pb.FuseAttributes{FileSize: 10}},
			NewParentPath: "/dir",
		},
	})

	if !fh.isDeleted {
		t.Fatal("a published file replaced by a rename was not marked deleted")
	}
}
