package filer

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// A name of a hard-linked file comes off the file's link count only once the
// store no longer holds the name (dropLink). These tests have the store refuse
// the delete or the write, as TiKV does after write conflicts it gave up on,
// and run it again, as a client does after the error.

// storeRefusal is a store refusing a delete or a write, as TiKV does on a
// write conflict: the change is not made, and the client may send it again.
var storeRefusal = errors.New("write conflict { start_ts:463, conflict_start_ts:462, conflict_commit_ts:464 }")

const linkedChunk = "3,0c03"

// linkedFile puts in store a file with the given names: the hard link record
// counting them all and holding the file's one chunk, and a row for each name.
func linkedFile(t *testing.T, store *stubFilerStore, names ...util.FullPath) HardLinkId {
	t.Helper()
	id := NewHardLinkId()
	shared := &Entry{
		FullPath:        names[0],
		Attr:            Attr{Mode: 0o644, Mtime: time.Unix(1700000000, 0)},
		HardLinkId:      id,
		HardLinkCounter: int32(len(names)),
		Chunks:          []*filer_pb.FileChunk{{FileId: linkedChunk, Size: 4}},
	}
	store.kv[string(id)] = encodeLinkRecord(t, shared)
	for _, name := range names {
		row := shared.ShallowClone()
		row.FullPath = name
		store.entries[string(name)] = row
	}
	return id
}

func encodeLinkRecord(t *testing.T, shared *Entry) []byte {
	t.Helper()
	blob, err := shared.EncodeAttributesAndChunks()
	require.NoError(t, err)
	return blob
}

// setLinkCount rewrites the count in id's record, as a link or a delete
// through another filer does. Callers hold store.mu (kvGetHook).
func setLinkCount(t *testing.T, store *stubFilerStore, id HardLinkId, count int32) {
	t.Helper()
	shared := &Entry{}
	require.NoError(t, shared.DecodeAttributesAndChunks(store.kv[string(id)]))
	shared.HardLinkCounter = count
	store.kv[string(id)] = encodeLinkRecord(t, shared)
}

// linkCount is the count in id's record, 0 once the record is gone.
func linkCount(t *testing.T, store *stubFilerStore, id HardLinkId) int32 {
	t.Helper()
	blob, err := store.KvGet(context.Background(), id)
	if errors.Is(err, ErrKvNotFound) {
		return 0
	}
	require.NoError(t, err)
	shared := &Entry{}
	require.NoError(t, shared.DecodeAttributesAndChunks(blob))
	return shared.HardLinkCounter
}

// onSecondRecordRead runs f when the store reads id's record the second time:
// a delete reads it first in its lookup, then to take the link off.
func onSecondRecordRead(store *stubFilerStore, id HardLinkId, f func()) {
	onRecordRead(store, id, 2, f)
}

// onRecordRead runs f when the store reads id's record the nth time.
func onRecordRead(store *stubFilerStore, id HardLinkId, nth int, f func()) {
	reads := 0
	store.kvGetHook = func(key []byte) {
		if string(key) == string(id) {
			if reads++; reads == nth {
				f()
			}
		}
	}
}

func TestDeleteRefusedByStoreKeepsHardLinkCount(t *testing.T) {
	for _, del := range []struct {
		name string
		run  func(*FilerStoreWrapper, context.Context, *Entry) error
	}{
		{"DeleteOneEntry", (*FilerStoreWrapper).DeleteOneEntry},
		{"DeleteEntry", func(w *FilerStoreWrapper, ctx context.Context, e *Entry) error { return w.DeleteEntry(ctx, e.FullPath) }},
	} {
		t.Run(del.name, func(t *testing.T) {
			store := newStubFilerStore()
			id := linkedFile(t, store, "/d/a", "/d/b")
			wrapper := NewFilerStoreWrapper(store)
			ctx := context.Background()
			a, err := wrapper.FindEntry(ctx, "/d/a")
			require.NoError(t, err)

			store.deleteErrByPath["/d/a"] = storeRefusal
			require.ErrorIs(t, del.run(wrapper, ctx, a), storeRefusal)
			require.Equal(t, int32(2), linkCount(t, store, id), "the refused delete took a link off")

			delete(store.deleteErrByPath, "/d/a")
			require.NoError(t, del.run(wrapper, ctx, a))
			require.Equal(t, int32(1), linkCount(t, store, id))
			b, err := wrapper.FindEntry(ctx, "/d/b")
			require.NoError(t, err)
			require.Equal(t, int32(1), b.HardLinkCounter)
		})
	}
}

// The same for a write of another file over one of the names.
func TestWriteOverHardLinkRefusedByStoreKeepsHardLinkCount(t *testing.T) {
	for _, write := range []struct {
		name   string
		refuse func(*stubFilerStore, error)
		run    func(*FilerStoreWrapper, context.Context, *Entry) error
	}{
		{"InsertEntry", func(s *stubFilerStore, err error) { s.insertErr = err }, (*FilerStoreWrapper).InsertEntry},
		{"UpdateEntry", func(s *stubFilerStore, err error) { s.updateErr = err }, (*FilerStoreWrapper).UpdateEntry},
	} {
		t.Run(write.name, func(t *testing.T) {
			store := newStubFilerStore()
			id := linkedFile(t, store, "/d/a", "/d/b")
			wrapper := NewFilerStoreWrapper(store)
			ctx := context.Background()
			another := func() *Entry {
				return &Entry{FullPath: "/d/a", Attr: Attr{Mode: 0o644}, Chunks: []*filer_pb.FileChunk{{FileId: "4,0d04", Size: 4}}}
			}

			write.refuse(store, storeRefusal)
			require.ErrorIs(t, write.run(wrapper, ctx, another()), storeRefusal)
			require.Equal(t, int32(2), linkCount(t, store, id), "the refused write took a link off")

			write.refuse(store, nil)
			require.NoError(t, write.run(wrapper, ctx, another()))
			require.Equal(t, int32(1), linkCount(t, store, id))
		})
	}
}

// A rename deletes the old name of a file it has stored under the new one
// (OP "MV"): the link went with the name, so the count stays.
func TestRenameDeleteKeepsHardLinkCount(t *testing.T) {
	store := newStubFilerStore()
	id := linkedFile(t, store, "/d/a", "/d/b")
	wrapper := NewFilerStoreWrapper(store)
	ctx := context.WithValue(context.Background(), "OP", "MV")
	a, err := wrapper.FindEntry(ctx, "/d/a")
	require.NoError(t, err)

	require.NoError(t, wrapper.DeleteOneEntry(ctx, a))
	require.NoError(t, wrapper.DeleteEntry(ctx, "/d/b"))
	require.Equal(t, int32(2), linkCount(t, store, id))
}

// When the link cannot be taken off after the delete, the name still goes and
// the count stays one too high: the record and the chunks outlive the file's
// last name rather than go before it.
func TestDeleteKeepsHardLinkItCannotTakeOff(t *testing.T) {
	store := newStubFilerStore()
	id := linkedFile(t, store, "/d/a", "/d/b")
	wrapper := NewFilerStoreWrapper(store)
	ctx := context.Background()
	a, err := wrapper.FindEntry(ctx, "/d/a")
	require.NoError(t, err)

	store.setKvPutErr(errors.New("kv put: store unavailable"))
	require.NoError(t, wrapper.DeleteOneEntry(ctx, a))
	store.setKvPutErr(nil)
	_, err = store.FindEntry(ctx, "/d/a")
	require.ErrorIs(t, err, filer_pb.ErrNotFound)
	require.Equal(t, int32(2), linkCount(t, store, id))
}

// A client sends again a delete the store refused, then deletes the file's
// other name. Only that last delete takes the chunks.
func TestDeleteEntryRefusedAndSentAgainKeepsChunksForOtherName(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	id := linkedFile(t, store, "/d/a", "/d/b")

	store.deleteErrByPath["/d/a"] = storeRefusal
	require.ErrorContains(t, f.DeleteEntryMetaAndData(ctx, "/d/a", false, false, true, false, nil, time.Time{}), "write conflict")
	delete(store.deleteErrByPath, "/d/a")
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/a", false, false, true, false, nil, time.Time{}))

	require.Equal(t, 0, f.pendingDeletionCount(), "chunks deleted while /d/b still reads them")
	b, err := f.FindEntry(ctx, "/d/b")
	require.NoError(t, err)
	require.Equal(t, int32(1), b.HardLinkCounter)
	require.Equal(t, linkedChunk, b.GetChunks()[0].GetFileIdString())

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/b", false, false, true, false, nil, time.Time{}))
	require.Equal(t, 1, f.pendingDeletionCount())
	require.Equal(t, int32(0), linkCount(t, store, id))
}

// Another filer links the file under a new name after this delete read the
// count of its only name: the record outlives the delete, and so do the
// chunks, which the new name reads.
func TestDeleteEntryKeepsChunksOfLinkAddedMeanwhile(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	id := linkedFile(t, store, "/d/a")
	onSecondRecordRead(store, id, func() {
		setLinkCount(t, store, id, 2)
		store.entries["/d/c"] = &Entry{FullPath: "/d/c", Attr: Attr{Mode: 0o644}, HardLinkId: id, HardLinkCounter: 2}
	})

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/a", false, false, true, false, nil, time.Time{}))
	store.kvGetHook = nil
	require.Equal(t, 0, f.pendingDeletionCount(), "chunks deleted while /d/c reads them")
	require.Equal(t, int32(1), linkCount(t, store, id))
}

// One name deleted twice at once through two filers: both read the file's two
// links before either took one off, so both take one off and the record goes
// while /d/b still holds the file. The count each delete read still shows
// /d/b, and the chunks stay.
func TestDeleteEntryTwiceAtOnceKeepsChunks(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	id := linkedFile(t, store, "/d/a", "/d/b")
	onSecondRecordRead(store, id, func() {
		// the other delete of /d/a takes its link off first
		setLinkCount(t, store, id, 1)
	})

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/a", false, false, true, false, nil, time.Time{}))
	store.kvGetHook = nil
	require.Equal(t, 0, f.pendingDeletionCount(), "chunks deleted while /d/b still reads them")
}

// Another filer deletes /d/a, name and link, after this delete read the name
// and before it read the count: the count it reads already lacks /d/a, so its
// link must not come off again, or the record and chunks go with /d/b left.
func TestDeleteEntryOfANameGoneSinceItsLookupKeepsTheOtherLink(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	id := linkedFile(t, store, "/d/a", "/d/b")
	onRecordRead(store, id, 1, func() {
		delete(store.entries, "/d/a")
		setLinkCount(t, store, id, 1)
	})

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/a", false, false, true, false, nil, time.Time{}))
	store.kvGetHook = nil
	require.Equal(t, 0, f.pendingDeletionCount(), "chunks deleted while /d/b still reads them")
	require.Equal(t, int32(1), linkCount(t, store, id))
}

// The second lookup of a hard-linked name fails: the delete reports the error,
// and the name stays with its link.
func TestDeleteEntryReportsAFailedSecondLookup(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	id := linkedFile(t, store, "/d/a", "/d/b")
	onRecordRead(store, id, 1, func() {
		store.findErrByPath = map[string]error{"/d/a": storeRefusal}
	})

	require.Error(t, f.DeleteEntryMetaAndData(ctx, "/d/a", false, false, true, false, nil, time.Time{}))
	store.kvGetHook = nil
	require.Contains(t, store.entries, "/d/a")
	require.Equal(t, int32(2), linkCount(t, store, id))
	require.Equal(t, 0, f.pendingDeletionCount())
}

// onRecordReadAfterDelete runs f once, when the store reads id's record after
// name is gone from it: the delete that removed name has come to take its link
// off.
func onRecordReadAfterDelete(store *stubFilerStore, id HardLinkId, name util.FullPath, f func()) {
	done := false
	store.kvGetHook = func(key []byte) {
		if _, kept := store.entries[string(name)]; string(key) == string(id) && !kept && !done {
			done = true
			f()
		}
	}
}

// A recursive delete takes a hard-linked child as a single delete takes it.
// Refused by the store, the name stays with its link; run again, the delete
// takes the link off once, and the chunks stay with /d/b.
func TestRecursiveDeleteRefusedByStoreKeepsHardLinkCount(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	store.entries["/d/sub"] = &Entry{FullPath: "/d/sub", Attr: Attr{Mode: os.ModeDir | 0o755}}
	id := linkedFile(t, store, "/d/sub/a", "/d/b")

	store.deleteErrByPath["/d/sub/a"] = storeRefusal
	require.ErrorContains(t, f.DeleteEntryMetaAndData(ctx, "/d/sub", true, false, true, false, nil, time.Time{}), "write conflict")
	require.Equal(t, int32(2), linkCount(t, store, id), "the refused delete took a link off")

	delete(store.deleteErrByPath, "/d/sub/a")
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/sub", true, false, true, false, nil, time.Time{}))
	require.Equal(t, int32(1), linkCount(t, store, id))
	require.Equal(t, 0, f.pendingDeletionCount(), "chunks deleted while /d/b still reads them")
}

// The folder's other children are refused after its hard-linked child went:
// that child's link is off already, so once /d/b goes too the chunks go with
// it. A store that deletes a folder's children in batches can fail after some
// of them; a hard-linked name is never among those.
func TestRecursiveDeleteFailingAfterItsLinkedChildTookItsLinkOff(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	store.entries["/d/sub"] = &Entry{FullPath: "/d/sub", Attr: Attr{Mode: os.ModeDir | 0o755}}
	store.entries["/d/sub/c"] = &Entry{FullPath: "/d/sub/c", Attr: Attr{Mode: 0o644}}
	id := linkedFile(t, store, "/d/sub/a", "/d/b")

	store.deleteFolderErr = storeRefusal
	require.ErrorContains(t, f.DeleteEntryMetaAndData(ctx, "/d/sub", true, false, true, false, nil, time.Time{}), "write conflict")
	require.Equal(t, int32(1), linkCount(t, store, id))

	store.deleteFolderErr = nil
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/sub", true, false, true, false, nil, time.Time{}))
	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/b", false, false, true, false, nil, time.Time{}))
	require.Equal(t, int32(0), linkCount(t, store, id))
	require.Equal(t, 1, f.pendingDeletionCount())
}

// The folder's other children are refused after the file's last link went
// with its hard-linked child. Nothing names those chunks any more, so they
// are deleted then, not with the folder's other chunks at the end.
func TestRecursiveDeleteFailingAfterALastLinkWentDeletesItsChunks(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	store.entries["/d/sub"] = &Entry{FullPath: "/d/sub", Attr: Attr{Mode: os.ModeDir | 0o755}}
	store.entries["/d/sub/c"] = &Entry{FullPath: "/d/sub/c", Attr: Attr{Mode: 0o644}}
	id := linkedFile(t, store, "/d/sub/a")

	store.deleteFolderErr = storeRefusal
	require.ErrorContains(t, f.DeleteEntryMetaAndData(ctx, "/d/sub", true, false, true, false, nil, time.Time{}), "write conflict")
	require.Equal(t, int32(0), linkCount(t, store, id))
	require.Equal(t, 1, f.pendingDeletionCount(), "the chunks of the file's last link were left behind")
}

// Every name of the file is in the deleted tree, in two folders: the second
// name takes the last link off, and the chunks go once.
func TestRecursiveDeleteOfEveryNameDeletesChunksOnce(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	for _, dir := range []util.FullPath{"/d/sub", "/d/sub/x", "/d/sub/y"} {
		store.entries[string(dir)] = &Entry{FullPath: dir, Attr: Attr{Mode: os.ModeDir | 0o755}}
	}
	id := linkedFile(t, store, "/d/sub/x/a", "/d/sub/y/b")

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/sub", true, false, true, false, nil, time.Time{}))
	require.Equal(t, int32(0), linkCount(t, store, id))
	require.Equal(t, 1, f.pendingDeletionCount())
}

// Two filers delete /d/sub at once, and both read the count of /d/sub/a
// before either took its link off: both take it off, and the record goes
// while /d/b still holds the file. The count each delete read still shows
// /d/b, and the chunks stay.
func TestRecursiveDeletesAtOnceKeepChunksOfANameOutside(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	store.entries["/d/sub"] = &Entry{FullPath: "/d/sub", Attr: Attr{Mode: os.ModeDir | 0o755}}
	id := linkedFile(t, store, "/d/sub/a", "/d/b")
	onRecordReadAfterDelete(store, id, "/d/sub/a", func() {
		// the other delete of /d/sub takes the link of /d/sub/a off first
		setLinkCount(t, store, id, 1)
	})

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/sub", true, false, true, false, nil, time.Time{}))
	store.kvGetHook = nil
	require.Equal(t, 0, f.pendingDeletionCount(), "chunks deleted while /d/b still reads them")
}

// The recursive delete's take on the race above: /d/sub/a goes through another
// filer between this delete's lookup of the name and its read of the count.
func TestRecursiveDeleteOfANameGoneSinceItsLookupKeepsTheOtherLink(t *testing.T) {
	store := newStubFilerStore()
	f := newTestFiler(t, store, NewFilerRemoteStorage())
	ctx := context.Background()
	store.entries["/d/sub"] = &Entry{FullPath: "/d/sub", Attr: Attr{Mode: os.ModeDir | 0o755}}
	id := linkedFile(t, store, "/d/sub/a", "/d/b")
	// the listing reads the record first, the lookup of /d/sub/a second
	onRecordRead(store, id, 2, func() {
		delete(store.entries, "/d/sub/a")
		setLinkCount(t, store, id, 1)
	})

	require.NoError(t, f.DeleteEntryMetaAndData(ctx, "/d/sub", true, false, true, false, nil, time.Time{}))
	store.kvGetHook = nil
	require.Equal(t, 0, f.pendingDeletionCount(), "chunks deleted while /d/b still reads them")
	require.Equal(t, int32(1), linkCount(t, store, id))
}
