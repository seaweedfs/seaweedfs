package weed_server

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// An update reads the entry, then writes it. Its path lock is this filer's and
// covers that one path: a rename or a delete on another filer, or of a
// directory above the entry, can commit between the two. These tests have one
// commit just then, on a store that refuses the update's commit for it as TiKV
// does, and check that the update answers from what the other writer left
// instead of storing the entry again where it no longer is.

// conflictRefusingStore is txnRenameStore declaring, as the TiKV store does,
// that its commits are refused for a concurrent write.
type conflictRefusingStore struct{ *txnRenameStore }

func (conflictRefusingStore) RefusesConflictingCommits() bool { return true }

const updatedChunk, storedChunk, otherChunk = "2,0b02", "1,0a01", "3,0c03"

// updateConflictServer has /ckpt/run/f in the store with the chunk 1,0a01.
// meanwhile runs against the store as another writer's committed transaction,
// after the update read the entry and before its first commit, which the store
// then refuses.
func updateConflictServer(t *testing.T, meanwhile func(entries map[string]*filer.Entry)) (*FilerServer, *filer.Filer, *txnRenameStore, *captureQueue) {
	t.Helper()
	store := newTxnRenameStore()
	store.entries["/ckpt/run"] = newDirectoryEntry("/ckpt/run", 110)
	store.entries["/ckpt/run/f"] = fileEntryWithChunk("/ckpt/run/f", 101, storedChunk)
	store.refuse = func(commit int) error {
		if commit > 1 {
			return nil
		}
		store.mu.Lock()
		meanwhile(store.entries)
		store.mu.Unlock()
		return writeConflict(commit)
	}
	queue := &captureQueue{}
	swapNotificationQueue(t, queue)
	f := newRenameTestFiler(t, conflictRefusingStore{store})
	return &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}, f, store, queue
}

func updateOfF(chunk string) *filer_pb.UpdateEntryRequest {
	return &filer_pb.UpdateEntryRequest{
		Directory: "/ckpt/run",
		Entry: &filer_pb.Entry{
			Name:       "f",
			Attributes: &filer_pb.FuseAttributes{Mtime: 1700000100, FileMode: 0644, Inode: 101, FileSize: 4},
			Chunks:     []*filer_pb.FileChunk{written(chunk, 2)},
		},
	}
}

func moveInStore(entries map[string]*filer.Entry, from, to string) {
	entry := entries[from].ShallowClone()
	entry.FullPath = util.FullPath(to)
	entries[to] = entry
	delete(entries, from)
}

// checkGone: the entry the update was for left its path, so the update finds
// nothing, stores nothing there, tells no subscriber, and deletes no chunk:
// the one it would have replaced is still read under the entry's new name.
func checkGone(t *testing.T, err error, f *filer.Filer, store *txnRenameStore, queue *captureQueue, gone ...string) {
	t.Helper()
	if status.Code(err) != codes.NotFound {
		t.Fatalf("UpdateEntry: %v, want NotFound", err)
	}
	for _, p := range gone {
		if _, err := store.renameTestStore.FindEntry(context.Background(), util.FullPath(p)); !errors.Is(err, filer_pb.ErrNotFound) {
			t.Fatalf("%s after the update: %v, want it to stay gone", p, err)
		}
	}
	if events := queue.snapshot(); len(events) != 0 {
		t.Fatalf("metadata events = %+v, want none", events)
	}
	checkDeletions(t, f)
}

func checkChunk(t *testing.T, store *txnRenameStore, p string, want string) {
	t.Helper()
	entry, err := store.renameTestStore.FindEntry(context.Background(), util.FullPath(p))
	if err != nil {
		t.Fatalf("%s: %v", p, err)
	}
	if ids := chunkIds(entry); len(ids) != 1 || ids[0] != want {
		t.Fatalf("%s chunks = %v, want [%s]", p, ids, want)
	}
}

// mv run run2 while a file under run is flushed: the update must not leave
// run/f under a directory that no longer exists, where no listing reaches it,
// nor delete the chunk run2/f reads.
func TestUpdateEntryFindsNothingAfterItsDirectoryIsRenamed(t *testing.T) {
	server, f, store, queue := updateConflictServer(t, func(entries map[string]*filer.Entry) {
		moveInStore(entries, "/ckpt/run", "/ckpt/run2")
		moveInStore(entries, "/ckpt/run/f", "/ckpt/run2/f")
	})

	_, err := server.UpdateEntry(context.Background(), updateOfF(updatedChunk))

	checkGone(t, err, f, store, queue, "/ckpt/run", "/ckpt/run/f")
	checkChunk(t, store, "/ckpt/run2/f", storedChunk)
}

// mv f g on another filer while f is flushed: the update must not bring f back
// as a second name for the file, sharing a chunk that deleting either name
// deletes for both.
func TestUpdateEntryFindsNothingAfterTheEntryIsRenamed(t *testing.T) {
	server, f, store, queue := updateConflictServer(t, func(entries map[string]*filer.Entry) {
		moveInStore(entries, "/ckpt/run/f", "/ckpt/run/g")
	})

	_, err := server.UpdateEntry(context.Background(), updateOfF(updatedChunk))

	checkGone(t, err, f, store, queue, "/ckpt/run/f")
	checkChunk(t, store, "/ckpt/run/g", storedChunk)
}

// rm f on another filer while f is flushed: the update must not bring the
// removed file back.
func TestUpdateEntryFindsNothingAfterTheEntryIsDeleted(t *testing.T) {
	server, f, store, queue := updateConflictServer(t, func(entries map[string]*filer.Entry) {
		delete(entries, "/ckpt/run/f")
	})

	_, err := server.UpdateEntry(context.Background(), updateOfF(updatedChunk))

	checkGone(t, err, f, store, queue, "/ckpt/run/f")
}

// Another file renamed over f while f is flushed. An update without a
// condition still writes over whatever is at the path, but it replaces the
// file that is there now: that file's chunk is the one to delete, and the one
// subscribers are told went.
func TestUpdateEntryReplacesWhatWasRenamedOverTheEntry(t *testing.T) {
	server, f, store, queue := updateConflictServer(t, func(entries map[string]*filer.Entry) {
		entries["/ckpt/run/f"] = fileEntryWithChunk("/ckpt/run/f", 202, otherChunk)
	})

	if _, err := server.UpdateEntry(context.Background(), updateOfF(updatedChunk)); err != nil {
		t.Fatalf("UpdateEntry: %v, want it to run again after the conflict", err)
	}

	if got := store.commitCount(); got != 2 {
		t.Fatalf("commits = %d, want 2", got)
	}
	checkChunk(t, store, "/ckpt/run/f", updatedChunk)
	checkDeletions(t, f, otherChunk)
	events := queue.snapshot()
	if len(events) != 1 {
		t.Fatalf("metadata events = %+v, want the one committed update", events)
	}
	if old := events[0].notification.GetOldEntry().GetChunks(); len(old) != 1 || old[0].GetFileIdString() != otherChunk {
		t.Fatalf("event's old entry chunks = %v, want [%s]", old, otherChunk)
	}
}

// The same, for an update that names the entry it means to change: the file
// renamed over it is not that entry, and stays as it is.
func TestUpdateEntryChecksItsPreconditionAgainAfterAConflict(t *testing.T) {
	server, f, store, queue := updateConflictServer(t, func(entries map[string]*filer.Entry) {
		other := fileEntryWithChunk("/ckpt/run/f", 202, otherChunk)
		other.Extended = map[string][]byte{"owner": []byte("b")}
		entries["/ckpt/run/f"] = other
	})
	store.entries["/ckpt/run/f"].Extended = map[string][]byte{"owner": []byte("a")}
	req := updateOfF(updatedChunk)
	req.ExpectedExtended = map[string][]byte{"owner": []byte("a")}

	_, err := server.UpdateEntry(context.Background(), req)

	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UpdateEntry: %v, want FailedPrecondition", err)
	}
	checkChunk(t, store, "/ckpt/run/f", otherChunk)
	if events := queue.snapshot(); len(events) != 0 {
		t.Fatalf("metadata events = %+v, want none", events)
	}
	checkDeletions(t, f)
}

// A store that keeps refusing: the update gives up with the store's error and
// leaves the entry as the other writers have it.
func TestUpdateEntryGivesUpWhileTheStoreKeepsRefusing(t *testing.T) {
	server, f, store, queue := updateConflictServer(t, func(map[string]*filer.Entry) {})
	store.refuse = writeConflict

	_, err := server.UpdateEntry(context.Background(), updateOfF(updatedChunk))

	if !errors.Is(err, filer.ErrTransactionConflict) {
		t.Fatalf("UpdateEntry: %v, want the store's conflict", err)
	}
	if got := store.commitCount(); got != entryUpdateAttempts {
		t.Fatalf("commits = %d, want %d", got, entryUpdateAttempts)
	}
	checkChunk(t, store, "/ckpt/run/f", storedChunk)
	if events := queue.snapshot(); len(events) != 0 {
		t.Fatalf("metadata events = %+v, want none", events)
	}
	checkDeletions(t, f)
}

// A store that does not refuse conflicting commits gains nothing from a
// transaction around the update, and gets none.
func TestUpdateEntryUsesNoTransactionWhereTheStoreRefusesNone(t *testing.T) {
	store := newTxnRenameStore()
	store.entries["/ckpt/run"] = newDirectoryEntry("/ckpt/run", 110)
	store.entries["/ckpt/run/f"] = fileEntryWithChunk("/ckpt/run/f", 101, storedChunk)
	swapNotificationQueue(t, &captureQueue{})
	f := newRenameTestFiler(t, store)
	server := &FilerServer{filer: f, option: &FilerOption{}, entryLockTable: util.NewLockTable[util.FullPath]()}

	if _, err := server.UpdateEntry(context.Background(), updateOfF(updatedChunk)); err != nil {
		t.Fatalf("UpdateEntry: %v", err)
	}

	if got := store.commitCount(); got != 0 {
		t.Fatalf("commits = %d, want none", got)
	}
	checkChunk(t, store, "/ckpt/run/f", updatedChunk)
	checkDeletions(t, f, storedChunk)
}
