package weed_server

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// watchTarget records every store write after which path was missing: what a
// lookup through another filer would have seen at that moment on a store
// without transactions.
func watchTarget(store *renameTestStore, path string) (writes, missing *[]string) {
	writes, missing = &[]string{}, &[]string{}
	store.afterOp = func(op string, p util.FullPath) {
		*writes = append(*writes, op+" "+string(p))
		store.mu.Lock()
		_, found := store.entries[path]
		store.mu.Unlock()
		if !found {
			*missing = append(*missing, op+" "+string(p))
		}
	}
	return writes, missing
}

// onLookup makes fn run right after the n-th lookup of path, as another filer
// acting just then would: what that lookup returned is already out of date.
func onLookup(store *renameTestStore, path util.FullPath, n int, fn func()) {
	lookups := 0
	store.afterFind = func(p util.FullPath) {
		if p == path {
			if lookups++; lookups == n {
				fn()
			}
		}
	}
}

// newInPlaceRenameStore is a store that says its insert replaces an entry at
// once and whole, as the Redis stores do, holding /dir/src.txt.
func newInPlaceRenameStore() *renameTestStore {
	store := newRenameTestStore()
	store.overwritesAtOnce = true
	store.entries["/dir"] = newDirectoryEntry("/dir", 100)
	store.entries["/dir/src.txt"] = fileEntryWithChunk("/dir/src.txt", 101, "1,0a01")
	return store
}

func renameSrcOverDst(t *testing.T, server *FilerServer) {
	t.Helper()
	if _, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/dir", OldName: "src.txt", NewDirectory: "/dir", NewName: "dst.txt",
	}); err != nil {
		t.Fatalf("AtomicRenameEntry: %v", err)
	}
}

// checkReplaced: /dir/src.txt is gone and /dir/dst.txt is the renamed file.
func checkReplaced(t *testing.T, f *filer.Filer, store *renameTestStore) {
	t.Helper()
	if _, err := store.FindEntry(context.Background(), "/dir/src.txt"); err != filer_pb.ErrNotFound {
		t.Fatalf("source after rename: %v, want %v", err, filer_pb.ErrNotFound)
	}
	dst, err := f.FindEntry(context.Background(), "/dir/dst.txt")
	if err != nil {
		t.Fatalf("find target: %v", err)
	}
	if dst.Attr.Inode != 101 {
		t.Fatalf("target inode = %d, want the renamed file's 101", dst.Attr.Inode)
	}
	if len(dst.HardLinkId) != 0 {
		t.Fatalf("target still carries hard link id %x", dst.HardLinkId)
	}
	if chunks := dst.GetChunks(); len(chunks) != 1 || chunks[0].GetFileIdString() != "1,0a01" {
		t.Fatalf("target chunks = %v, want the renamed file's 1,0a01", chunks)
	}
}

// On a store whose insert replaces an entry at once and whole, the target never
// goes missing while a file is renamed over it, and subscribers still see the
// target's delete, then the rename.
func TestAtomicRenameEntryOverwriteKeepsTargetResolvable(t *testing.T) {
	store := newInPlaceRenameStore()
	store.entries["/dir/dst.txt"] = fileEntryWithChunk("/dir/dst.txt", 202, "2,0b02")
	writes, missing := watchTarget(store, "/dir/dst.txt")

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)
	f := newRenameTestFiler(t, store)
	renameSrcOverDst(t, &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()})

	if len(*missing) != 0 {
		t.Fatalf("target missing after writes %v (all writes %v)", *missing, *writes)
	}
	checkReplaced(t, f, store)
	if got := queuedDeletions(f); len(got) != 1 || got[0] != "2,0b02" {
		t.Fatalf("queued chunk deletions = %v, want the replaced file's 2,0b02", got)
	}
	events := queue.snapshot()
	if len(events) != 2 || events[0].key != "/dir/dst.txt" || events[0].notification.NewEntry != nil ||
		!events[0].notification.DeleteChunks || events[1].key != "/dir/src.txt" ||
		events[1].notification.NewEntry.GetName() != "dst.txt" {
		t.Fatalf("events = %+v, want the target's delete then the rename", events)
	}
}

type inPlaceRenameStream struct {
	grpc.ServerStream
	sent []*filer_pb.StreamRenameEntryResponse
}

func (s *inPlaceRenameStream) Context() context.Context { return context.Background() }
func (s *inPlaceRenameStream) Send(resp *filer_pb.StreamRenameEntryResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

// weed mount renames through StreamRenameEntry.
func TestStreamRenameEntryOverwriteKeepsTargetResolvable(t *testing.T) {
	store := newInPlaceRenameStore()
	store.entries["/dir/dst.txt"] = fileEntryWithChunk("/dir/dst.txt", 202, "2,0b02")
	writes, missing := watchTarget(store, "/dir/dst.txt")

	swapNotificationQueue(t, &captureQueue{})
	f := newRenameTestFiler(t, store)
	server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}
	stream := &inPlaceRenameStream{}
	if err := server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
		OldDirectory: "/dir", OldName: "src.txt", NewDirectory: "/dir", NewName: "dst.txt",
	}, stream); err != nil {
		t.Fatalf("StreamRenameEntry: %v", err)
	}

	if len(*missing) != 0 {
		t.Fatalf("target missing after writes %v (all writes %v)", *missing, *writes)
	}
	checkReplaced(t, f, store)
	if len(stream.sent) != 2 || stream.sent[0].EventNotification.NewEntry.GetName() != "dst.txt" {
		t.Fatalf("stream responses = %+v, want the rename then the source removal", stream.sent)
	}
}

// A directory renamed over an empty directory still goes through
// delete-then-create: in-place replacement is for files only.
func TestAtomicRenameEntryDirectoryOverEmptyDirectory(t *testing.T) {
	store := newRenameTestStore()
	store.overwritesAtOnce = true
	store.entries["/a"] = newDirectoryEntry("/a", 10)
	store.entries["/a/f.txt"] = newFileEntry("/a/f.txt", 11)
	store.entries["/b"] = newDirectoryEntry("/b", 20)

	swapNotificationQueue(t, &captureQueue{})
	f := newRenameTestFiler(t, store)
	server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}
	if _, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/", OldName: "a", NewDirectory: "/", NewName: "b",
	}); err != nil {
		t.Fatalf("AtomicRenameEntry: %v", err)
	}
	for _, p := range []string{"/a", "/a/f.txt"} {
		if _, err := store.FindEntry(context.Background(), util.FullPath(p)); err != filer_pb.ErrNotFound {
			t.Fatalf("%s after rename: %v, want %v", p, err, filer_pb.ErrNotFound)
		}
	}
	b, err := store.FindEntry(context.Background(), "/b")
	if err != nil || b.Attr.Inode != 10 {
		t.Fatalf("/b = %+v, %v; want the renamed directory (inode 10)", b, err)
	}
	if _, err := store.FindEntry(context.Background(), "/b/f.txt"); err != nil {
		t.Fatalf("/b/f.txt: %v", err)
	}
}

// Another filer deletes the target right after this filer looked it up. The
// renamed file must still be stored at the target and listed in its
// directory: on Redis an update after that delete would store it unlisted.
func TestAtomicRenameEntryOverATargetDeletedMeanwhile(t *testing.T) {
	store := newInPlaceRenameStore()
	store.entries["/dir/dst.txt"] = fileEntryWithChunk("/dir/dst.txt", 202, "2,0b02")
	store.listing = map[string]bool{"/dir/src.txt": true, "/dir/dst.txt": true}
	onLookup(store, "/dir/dst.txt", 1, func() {
		if err := store.DeleteEntry(context.Background(), "/dir/dst.txt"); err != nil {
			t.Errorf("delete the target as another filer: %v", err)
		}
	})

	swapNotificationQueue(t, &captureQueue{})
	f := newRenameTestFiler(t, store)
	renameSrcOverDst(t, &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()})

	checkReplaced(t, f, store)
	if !store.listing["/dir/dst.txt"] || store.listing["/dir/src.txt"] {
		t.Fatalf("listing = %v, want dst.txt listed and src.txt gone", store.listing)
	}
	// the rename saw the target, so it queued the target's chunks
	if got := queuedDeletions(f); len(got) != 1 || got[0] != "2,0b02" {
		t.Fatalf("queued chunk deletions = %v, want the target's 2,0b02", got)
	}
}

// A store that does not say its insert replaces an entry keeps
// delete-then-create: with transactions nothing else sees the target missing
// before the commit, and an insert over an existing row can abort one
// (Postgres); without them, the insert may leave part of the old row in place.
func TestAtomicRenameEntryOverwriteOnOtherStoresDeletesThenCreates(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strictTx bool
	}{
		{"without transactions", false},
		{"with transactions", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newInPlaceRenameStore()
			store.overwritesAtOnce = false
			store.strictTx = tc.strictTx
			store.entries["/dir/dst.txt"] = fileEntryWithChunk("/dir/dst.txt", 202, "2,0b02")
			writes, _ := watchTarget(store, "/dir/dst.txt")

			swapNotificationQueue(t, &captureQueue{})
			f := newRenameTestFiler(t, store)
			renameSrcOverDst(t, &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()})

			if want := []string{"delete /dir/dst.txt", "insert /dir/dst.txt", "delete /dir/src.txt"}; !reflect.DeepEqual(*writes, want) {
				t.Fatalf("store writes = %v, want %v", *writes, want)
			}
			checkReplaced(t, f, store)
			if got := queuedDeletions(f); len(got) != 1 || got[0] != "2,0b02" {
				t.Fatalf("queued chunk deletions = %v, want the replaced file's 2,0b02", got)
			}
		})
	}
}

// A target in a path-specific store keeps delete-then-create: what the default
// store says of its insert does not hold for that store. A path-specific store
// elsewhere changes nothing for a rename within the default store.
func TestAtomicRenameEntryOverwriteWithPathSpecificStore(t *testing.T) {
	t.Run("holding the target", func(t *testing.T) {
		store := newInPlaceRenameStore()
		store.entries["/dir/sub"] = newDirectoryEntry("/dir/sub", 110)
		writes, _ := watchTarget(store, "/dir/src.txt")
		// the store for /dir/sub/ keeps its entries by their path under it;
		// even one that says the same of its insert is not asked
		subStore := newRenameTestStore()
		subStore.overwritesAtOnce = true
		subStore.entries["/dst.txt"] = fileEntryWithChunk("/dst.txt", 202, "2,0b02")
		subWrites, _ := watchTarget(subStore, "/dst.txt")

		swapNotificationQueue(t, &captureQueue{})
		f := newRenameTestFiler(t, store)
		f.Store.(*filer.FilerStoreWrapper).AddPathSpecificStore("/dir/sub/", "sub", subStore)
		server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}
		if _, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: "/dir", OldName: "src.txt", NewDirectory: "/dir/sub", NewName: "dst.txt",
		}); err != nil {
			t.Fatalf("AtomicRenameEntry: %v", err)
		}

		if want := []string{"delete /dst.txt", "insert /dst.txt"}; !reflect.DeepEqual(*subWrites, want) {
			t.Fatalf("writes to the target's store = %v, want %v", *subWrites, want)
		}
		if want := []string{"delete /dir/src.txt"}; !reflect.DeepEqual(*writes, want) {
			t.Fatalf("writes to the default store = %v, want %v", *writes, want)
		}
		dst, err := f.FindEntry(context.Background(), "/dir/sub/dst.txt")
		if err != nil || dst.Attr.Inode != 101 {
			t.Fatalf("/dir/sub/dst.txt = %+v, %v; want the renamed file (inode 101)", dst, err)
		}
	})
	t.Run("elsewhere", func(t *testing.T) {
		store := newInPlaceRenameStore()
		store.entries["/dir/dst.txt"] = fileEntryWithChunk("/dir/dst.txt", 202, "2,0b02")
		writes, missing := watchTarget(store, "/dir/dst.txt")

		swapNotificationQueue(t, &captureQueue{})
		f := newRenameTestFiler(t, store)
		f.Store.(*filer.FilerStoreWrapper).AddPathSpecificStore("/archive/", "archive", newRenameTestStore())
		renameSrcOverDst(t, &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()})

		if len(*missing) != 0 {
			t.Fatalf("target missing after writes %v (all writes %v)", *missing, *writes)
		}
		checkReplaced(t, f, store)
	})
}

// The in-place write over a hard-linked target takes that name off its file:
// the file's chunks stay while another name holds them, and go with its last
// name.
func TestAtomicRenameEntryInPlaceOverHardLink(t *testing.T) {
	for _, links := range []int32{2, 1} {
		t.Run(map[int32]string{2: "one of two links", 1: "the last link"}[links], func(t *testing.T) {
			store := newInPlaceRenameStore()
			id := hardLinkedTarget(t, store, links)
			writes, missing := watchTarget(store, "/dir/dst.txt")

			swapNotificationQueue(t, &captureQueue{})
			f := newRenameTestFiler(t, store)
			renameSrcOverDst(t, &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()})

			if len(*missing) != 0 {
				t.Fatalf("target missing after writes %v (all writes %v)", *missing, *writes)
			}
			checkReplaced(t, f, store)
			_, recordKept := store.kv[string(id)]
			if links == 2 {
				if got := queuedDeletions(f); len(got) != 0 {
					t.Fatalf("queued chunk deletions = %v, want none: /dir/other.txt still uses %s", got, replacedChunk)
				}
				if other, err := f.FindEntry(context.Background(), "/dir/other.txt"); err != nil || other.HardLinkCounter != 1 {
					t.Fatalf("/dir/other.txt = %+v, %v; want it with a link count of 1", other, err)
				}
				if !recordKept {
					t.Fatal("hard link record deleted while a link remains")
				}
			} else {
				if got := queuedDeletions(f); len(got) != 1 || got[0] != replacedChunk {
					t.Fatalf("queued chunk deletions = %v, want the last link's %s", got, replacedChunk)
				}
				if recordKept {
					t.Fatal("hard link record kept after its last link was replaced")
				}
			}
		})
	}
}

// Another filer unlinks the target's other hard link after this filer read
// the link count (2), but before its write drops the target's link. No link is
// left then, so the file's chunks must go.
func TestAtomicRenameEntryOverHardLinkWhoseOtherLinkGoesMeanwhile(t *testing.T) {
	store := newInPlaceRenameStore()
	id := hardLinkedTarget(t, store, 2)

	swapNotificationQueue(t, &captureQueue{})
	f := newRenameTestFiler(t, store)
	// lookup 1 of the target is the rename's own; lookup 2 comes from the
	// store write, just before it drops the target's link
	onLookup(store, "/dir/dst.txt", 2, func() {
		if err := f.Store.DeleteEntry(context.Background(), "/dir/other.txt"); err != nil {
			t.Errorf("unlink the other link as another filer: %v", err)
		}
	})
	renameSrcOverDst(t, &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()})

	checkReplaced(t, f, store)
	if got := queuedDeletions(f); len(got) != 1 || got[0] != replacedChunk {
		t.Fatalf("queued chunk deletions = %v, want %s: no link to it is left", got, replacedChunk)
	}
	if _, found := store.kv[string(id)]; found {
		t.Fatal("hard link record kept after its last link was replaced")
	}
}

// Another filer turns the target into a directory right after this filer
// looked it up, by storing a child under it. Writing the renamed file over it
// would strand the child, so the rename is refused, as it is when the lookup
// already finds a directory.
func TestAtomicRenameEntryOverATargetThatBecameADirectory(t *testing.T) {
	store := newInPlaceRenameStore()
	store.entries["/dir/dst.txt"] = fileEntryWithChunk("/dir/dst.txt", 202, "2,0b02")
	onLookup(store, "/dir/dst.txt", 1, func() {
		store.mu.Lock()
		defer store.mu.Unlock()
		store.entries["/dir/dst.txt"] = newDirectoryEntry("/dir/dst.txt", 202)
		store.entries["/dir/dst.txt/child"] = fileEntryWithChunk("/dir/dst.txt/child", 203, "4,0d04")
	})

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)
	f := newRenameTestFiler(t, store)
	server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}
	_, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/dir", OldName: "src.txt", NewDirectory: "/dir", NewName: "dst.txt",
	})
	if !errors.Is(err, filer_pb.ErrExistingIsDirectory) && (err == nil || !strings.Contains(err.Error(), filer_pb.ErrExistingIsDirectory.Error())) {
		t.Fatalf("AtomicRenameEntry = %v, want %v", err, filer_pb.ErrExistingIsDirectory)
	}
	if dst, err := store.FindEntry(context.Background(), "/dir/dst.txt"); err != nil || !dst.IsDirectory() {
		t.Fatalf("/dir/dst.txt = %+v, %v; want the directory left as it is", dst, err)
	}
	if _, err := store.FindEntry(context.Background(), "/dir/dst.txt/child"); err != nil {
		t.Fatalf("the directory's child: %v", err)
	}
	if _, err := store.FindEntry(context.Background(), "/dir/src.txt"); err != nil {
		t.Fatalf("source after the refused rename: %v", err)
	}
	if got := queuedDeletions(f); len(got) != 0 {
		t.Fatalf("queued chunk deletions = %v for a rename that did not happen", got)
	}
	if events := queue.snapshot(); len(events) != 0 {
		t.Fatalf("events = %+v for a rename that did not happen", events)
	}
}

// Another filer deletes the target and its directory right after this filer
// looked the target up. The renamed file must not be left under a directory
// that does not exist: the directory is created again, as a create does, and
// announced before the rename.
func TestAtomicRenameEntryOverATargetWhoseDirectoryWentMeanwhile(t *testing.T) {
	store := newInPlaceRenameStore()
	store.entries["/dir/sub"] = newDirectoryEntry("/dir/sub", 300)
	store.entries["/dir/sub/dst.txt"] = fileEntryWithChunk("/dir/sub/dst.txt", 202, "2,0b02")
	onLookup(store, "/dir/sub/dst.txt", 1, func() {
		for _, p := range []util.FullPath{"/dir/sub/dst.txt", "/dir/sub"} {
			if err := store.DeleteEntry(context.Background(), p); err != nil {
				t.Errorf("delete %s as another filer: %v", p, err)
			}
		}
	})

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)
	f := newRenameTestFiler(t, store)
	server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}
	if _, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/dir", OldName: "src.txt", NewDirectory: "/dir/sub", NewName: "dst.txt",
	}); err != nil {
		t.Fatalf("AtomicRenameEntry: %v", err)
	}

	if dir, err := store.FindEntry(context.Background(), "/dir/sub"); err != nil || !dir.IsDirectory() {
		t.Fatalf("/dir/sub = %+v, %v; want the directory created again", dir, err)
	}
	if dst, err := store.FindEntry(context.Background(), "/dir/sub/dst.txt"); err != nil || dst.Attr.Inode != 101 {
		t.Fatalf("/dir/sub/dst.txt = %+v, %v; want the renamed file (inode 101)", dst, err)
	}
	if _, err := store.FindEntry(context.Background(), "/dir/src.txt"); err != filer_pb.ErrNotFound {
		t.Fatalf("source after rename: %v, want %v", err, filer_pb.ErrNotFound)
	}
	createdAt, renamedAt := -1, -1
	for i, event := range queue.snapshot() {
		switch {
		case event.notification.OldEntry == nil && event.notification.NewEntry.GetName() == "sub":
			createdAt = i
		case event.notification.OldEntry.GetName() == "src.txt" && event.notification.NewEntry.GetName() == "dst.txt":
			renamedAt = i
		}
	}
	if createdAt < 0 || renamedAt < 0 || createdAt > renamedAt {
		t.Fatalf("events = %+v, want /dir/sub's create before the rename", queue.snapshot())
	}
}
