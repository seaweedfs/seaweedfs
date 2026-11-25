package leveldb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A filer on a real store, sent what two mounts send when both add a name to
// a file at once: each read the file's count as 2 and sends 3. The filer
// counts the four names itself and keeps the count at no less, a later write
// with an old count changes nothing, and the file outlives the three names a
// count of 3 would have taken for all of them.
func TestTwoClientsLinkingOneFileAreBothCounted(t *testing.T) {
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	store := &LevelDBStore{}
	if err := store.initialize(t.TempDir()); err != nil {
		t.Fatalf("initialize leveldb: %v", err)
	}
	t.Cleanup(store.Shutdown)
	testFiler.SetStore(store)
	ctx := context.Background()

	name := func(p util.FullPath, id filer.HardLinkId, count int32) *filer.Entry {
		return &filer.Entry{
			FullPath:        p,
			Attr:            filer.Attr{Mode: 0644, Mtime: time.Unix(1700000000, 0)},
			Chunks:          []*filer_pb.FileChunk{{FileId: "3,0c03", Size: 4}},
			HardLinkId:      id,
			HardLinkCounter: count,
		}
	}
	create := func(p util.FullPath, id filer.HardLinkId, count int32) {
		t.Helper()
		if err := testFiler.CreateEntry(ctx, name(p, id, count), nil, false, false, nil, false, testFiler.MaxFilenameLength); err != nil {
			t.Fatalf("create %s: %v", p, err)
		}
	}
	update := func(p util.FullPath, id filer.HardLinkId, count int32) {
		t.Helper()
		old, err := testFiler.FindEntry(ctx, p)
		if err != nil {
			t.Fatalf("find %s: %v", p, err)
		}
		if err := testFiler.UpdateEntry(ctx, old, name(p, id, count), false); err != nil {
			t.Fatalf("update %s: %v", p, err)
		}
	}
	check := func(want int32, paths ...util.FullPath) {
		t.Helper()
		for _, p := range paths {
			e, err := testFiler.FindEntry(ctx, p)
			if err != nil {
				t.Fatalf("find %s: %v", p, err)
			}
			if e.HardLinkCounter != want || len(e.GetChunks()) != 1 {
				t.Fatalf("%s: link count %d with %d chunk(s), want %d with 1", p, e.HardLinkCounter, len(e.GetChunks()), want)
			}
		}
	}
	remove := func(p util.FullPath) {
		t.Helper()
		if err := testFiler.DeleteEntryMetaAndData(ctx, p, false, false, true, false, nil, time.Time{}); err != nil {
			t.Fatalf("delete %s: %v", p, err)
		}
	}

	create("/d/x", nil, 0)
	id := filer.NewHardLinkId()

	// the first link, as a mount sends it
	update("/d/x", id, 2)
	create("/d/y", id, 2)
	check(2, "/d/x", "/d/y")

	// two mounts, each from a count of 2
	update("/d/x", id, 3)
	update("/d/y", id, 3)
	create("/d/a", id, 3)
	create("/d/b", id, 3)
	check(4, "/d/x", "/d/y", "/d/a", "/d/b")

	// a mount that has not heard of either link changes the file through y
	update("/d/y", id, 2)
	check(4, "/d/x", "/d/y", "/d/a", "/d/b")

	remove("/d/x")
	remove("/d/y")
	remove("/d/a")
	check(1, "/d/b")

	remove("/d/b")
	if _, err := testFiler.FindEntry(ctx, "/d/b"); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("find /d/b after its delete: %v", err)
	}
	if _, err := store.KvGet(ctx, id); !errors.Is(err, filer.ErrKvNotFound) {
		t.Fatalf("the file's record after its last name went: %v", err)
	}
}

// A link id is random bytes and can start as the keys of the root's entries
// do here: the root still lists.
func TestRootListsWithALinkIdLikeItsKeys(t *testing.T) {
	testFiler := filer.NewFiler(pb.ServerDiscovery{}, nil, "", "", "", "", "", 255, nil)
	store := &LevelDBStore{}
	if err := store.initialize(t.TempDir()); err != nil {
		t.Fatalf("initialize leveldb: %v", err)
	}
	t.Cleanup(store.Shutdown)
	testFiler.SetStore(store)
	ctx := context.Background()

	id := filer.HardLinkId("/\x00abcdefghijklmn\x01")
	for _, p := range []util.FullPath{"/d/x", "/d/y"} {
		entry := &filer.Entry{
			FullPath:        p,
			Attr:            filer.Attr{Mode: 0644, Mtime: time.Unix(1700000000, 0)},
			Chunks:          []*filer_pb.FileChunk{{FileId: "3,0c03", Size: 4}},
			HardLinkId:      id,
			HardLinkCounter: 2,
		}
		if err := testFiler.CreateEntry(ctx, entry, nil, false, false, nil, false, testFiler.MaxFilenameLength); err != nil {
			t.Fatalf("create %s: %v", p, err)
		}
	}
	if _, err := store.ListDirectoryEntries(ctx, "/", "", true, 100, func(*filer.Entry) (bool, error) { return true, nil }); err != nil {
		t.Fatalf("list /: %v", err)
	}
}
