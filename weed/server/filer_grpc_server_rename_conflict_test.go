package weed_server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// txnRenameStore keeps a transaction's writes to itself until the commit, the
// way TiKV does: no reader sees them before, a rolled back transaction leaves
// nothing behind, and refuse can turn a commit down the way an optimistic
// store does when another writer committed one of its keys meanwhile.
type txnRenameStore struct {
	*renameTestStore
	refuse  func(commit int) error // commit counts from 1; nil lets every commit through
	commits int
}

type txnRenameWrites map[util.FullPath]*filer.Entry // nil: deleted

type txnRenameKey struct{}

// newTxnRenameStore starts with the directory /ckpt.
func newTxnRenameStore() *txnRenameStore {
	store := &txnRenameStore{renameTestStore: newRenameTestStore()}
	store.entries["/ckpt"] = newDirectoryEntry("/ckpt", 100)
	return store
}

// IsolatesTransactions declares what the store does, as the TiKV store does.
func (s *txnRenameStore) IsolatesTransactions() bool { return true }

func txnWrites(ctx context.Context) txnRenameWrites {
	writes, _ := ctx.Value(txnRenameKey{}).(txnRenameWrites)
	return writes
}

func (s *txnRenameStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return context.WithValue(ctx, txnRenameKey{}, txnRenameWrites{}), nil
}

func (s *txnRenameStore) CommitTransaction(ctx context.Context) error {
	writes := txnWrites(ctx)
	if writes == nil {
		return nil
	}
	s.mu.Lock()
	s.commits++
	commit := s.commits
	s.mu.Unlock()
	if s.refuse != nil {
		if err := s.refuse(commit); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for p, entry := range writes {
		if entry == nil {
			delete(s.entries, string(p))
		} else {
			s.entries[string(p)] = entry
		}
	}
	clear(writes)
	return nil
}

func (s *txnRenameStore) RollbackTransaction(ctx context.Context) error {
	clear(txnWrites(ctx))
	return nil
}

func (s *txnRenameStore) commitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits
}

func (s *txnRenameStore) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	if writes := txnWrites(ctx); writes != nil {
		writes[entry.FullPath] = entry.ShallowClone()
		return nil
	}
	return s.renameTestStore.InsertEntry(ctx, entry)
}

func (s *txnRenameStore) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	return s.InsertEntry(ctx, entry)
}

func (s *txnRenameStore) DeleteEntry(ctx context.Context, p util.FullPath) error {
	if writes := txnWrites(ctx); writes != nil {
		writes[p] = nil
		return nil
	}
	return s.renameTestStore.DeleteEntry(ctx, p)
}

func (s *txnRenameStore) FindEntry(ctx context.Context, p util.FullPath) (*filer.Entry, error) {
	if entry, found := txnWrites(ctx)[p]; found {
		if entry == nil {
			return nil, filer_pb.ErrNotFound
		}
		return entry.ShallowClone(), nil
	}
	return s.renameTestStore.FindEntry(ctx, p)
}

func (s *txnRenameStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (string, error) {
	return s.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

// ListDirectoryPrefixedEntries lists what the transaction sees: the store as
// committed, with the transaction's own writes over it.
func (s *txnRenameStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (string, error) {
	visible := map[util.FullPath]*filer.Entry{}
	s.mu.Lock()
	for p, entry := range s.entries {
		visible[util.FullPath(p)] = entry
	}
	s.mu.Unlock()
	for p, entry := range txnWrites(ctx) {
		if entry == nil {
			delete(visible, p)
		} else {
			visible[p] = entry
		}
	}
	var names []string
	children := map[string]*filer.Entry{}
	for p, entry := range visible {
		if parent, name := p.DirAndName(); parent == string(dirPath) && p != dirPath && strings.HasPrefix(name, prefix) {
			names = append(names, name)
			children[name] = entry
		}
	}
	sort.Strings(names)
	lastFileName, count := "", int64(0)
	for _, name := range names {
		if startFileName != "" && (name < startFileName || name == startFileName && !includeStartFile) {
			continue
		}
		lastFileName = name
		more, err := eachEntryFunc(children[name].ShallowClone())
		if err != nil || !more {
			return lastFileName, err
		}
		if count++; limit > 0 && count >= limit {
			break
		}
	}
	return lastFileName, nil
}

// commitStampedStream records each response with the number of commits the
// store had made when it was sent.
type commitStampedStream struct {
	grpc.ServerStream
	store *txnRenameStore
	sent  []*filer_pb.StreamRenameEntryResponse
	at    []int
}

func (s *commitStampedStream) Context() context.Context { return context.Background() }
func (s *commitStampedStream) Send(resp *filer_pb.StreamRenameEntryResponse) error {
	s.sent = append(s.sent, resp)
	s.at = append(s.at, s.store.commitCount())
	return nil
}

func writeConflict(commit int) error {
	return fmt.Errorf("%w: write conflict { start_ts:%d conflict_ts:%d reason:Optimistic }", filer.ErrTransactionConflict, commit, commit+27)
}

// flushedWhileRenaming refuses the first commit the way TiKV does when a
// writer commits the file being renamed after the rename's transaction began,
// and stores what that writer wrote: weed mount publishing the file the
// application is renaming over its target (safetensors, torch.save).
func flushedWhileRenaming(store *txnRenameStore, p string, flushed *filer.Entry) {
	store.refuse = func(commit int) error {
		if commit > 1 {
			return nil
		}
		store.mu.Lock()
		store.entries[p] = flushed
		store.mu.Unlock()
		return writeConflict(commit)
	}
}

func chunkIds(entry *filer.Entry) []string {
	var ids []string
	for _, chunk := range entry.GetChunks() {
		ids = append(ids, chunk.GetFileIdString())
	}
	return ids
}

func checkRenamedFile(t *testing.T, store *txnRenameStore, src, dst string, wantChunk string) {
	t.Helper()
	if _, err := store.renameTestStore.FindEntry(context.Background(), util.FullPath(src)); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("%s after the rename: %v, want gone", src, err)
	}
	moved, err := store.renameTestStore.FindEntry(context.Background(), util.FullPath(dst))
	if err != nil {
		t.Fatalf("%s after the rename: %v", dst, err)
	}
	if ids := chunkIds(moved); len(ids) != 1 || ids[0] != wantChunk {
		t.Fatalf("%s chunks = %v, want [%s]: the rename must move what the concurrent writer stored", dst, ids, wantChunk)
	}
}

// A writer that commits the file while the rename's transaction runs makes
// TiKV refuse the rename's commit. The rename runs again, moves what the
// writer stored, and its client hears of it once, after the commit.
func TestStreamRenameRunsAgainAfterConcurrentWrite(t *testing.T) {
	store := newTxnRenameStore()
	store.entries["/ckpt/.tmpTdjXVI"] = fileEntryWithChunk("/ckpt/.tmpTdjXVI", 101, "1,0a01")
	flushedWhileRenaming(store, "/ckpt/.tmpTdjXVI", fileEntryWithChunk("/ckpt/.tmpTdjXVI", 101, "2,0b02"))
	queue := &captureQueue{}
	swapNotificationQueue(t, queue)
	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
	stream := &commitStampedStream{store: store}

	if err := server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
		OldDirectory: "/ckpt", OldName: ".tmpTdjXVI", NewDirectory: "/ckpt", NewName: "ema.safetensors.tmp",
	}, stream); err != nil {
		t.Fatalf("StreamRenameEntry: %v, want the rename to run again after the conflict", err)
	}

	if got := store.commitCount(); got != 2 {
		t.Fatalf("commits = %d, want 2", got)
	}
	checkRenamedFile(t, store, "/ckpt/.tmpTdjXVI", "/ckpt/ema.safetensors.tmp", "2,0b02")
	if len(stream.sent) != 2 {
		t.Fatalf("client got %d responses, want the committed run's 2", len(stream.sent))
	}
	for i, commits := range stream.at {
		if commits != 2 {
			t.Fatalf("response %d went out after %d commits, want only after the rename committed", i, commits)
		}
	}
	if ids := stream.sent[0].EventNotification.GetNewEntry().GetChunks(); len(ids) != 1 || ids[0].GetFileIdString() != "2,0b02" {
		t.Fatalf("client's new entry chunks = %v, want the concurrent writer's", ids)
	}
	if events := queue.snapshot(); len(events) != 1 || events[0].notification.GetNewEntry().GetName() != "ema.safetensors.tmp" {
		t.Fatalf("metadata events = %+v, want the one committed rename", events)
	}
}

func TestAtomicRenameRunsAgainAfterConcurrentWrite(t *testing.T) {
	store := newTxnRenameStore()
	store.entries["/ckpt/.tmpTdjXVI"] = fileEntryWithChunk("/ckpt/.tmpTdjXVI", 101, "1,0a01")
	flushedWhileRenaming(store, "/ckpt/.tmpTdjXVI", fileEntryWithChunk("/ckpt/.tmpTdjXVI", 101, "2,0b02"))
	queue := &captureQueue{}
	swapNotificationQueue(t, queue)
	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}

	if _, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/ckpt", OldName: ".tmpTdjXVI", NewDirectory: "/ckpt", NewName: "ema.safetensors.tmp",
	}); err != nil {
		t.Fatalf("AtomicRenameEntry: %v, want the rename to run again after the conflict", err)
	}
	if got := store.commitCount(); got != 2 {
		t.Fatalf("commits = %d, want 2", got)
	}
	checkRenamedFile(t, store, "/ckpt/.tmpTdjXVI", "/ckpt/ema.safetensors.tmp", "2,0b02")
	if events := queue.snapshot(); len(events) != 1 {
		t.Fatalf("metadata events = %d, want the one committed rename", len(events))
	}
}

// A rename the store refuses for good must not reach the client: weed mount
// applies each response as it arrives, and would hold the file under a name
// the filer never gave it.
func TestStreamRenameRefusedCommitSendsNothing(t *testing.T) {
	for _, tc := range []struct {
		name        string
		refuse      func(commit int) error
		wantCommits int
	}{
		{"other error", func(int) error { return errors.New("region is unavailable") }, 1},
		{"conflicts every time", writeConflict, renameAttempts},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTxnRenameStore()
			store.entries["/ckpt/.tmpA"] = fileEntryWithChunk("/ckpt/.tmpA", 101, "1,0a01")
			store.refuse = tc.refuse
			queue := &captureQueue{}
			swapNotificationQueue(t, queue)
			server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
			stream := &commitStampedStream{store: store}

			err := server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
				OldDirectory: "/ckpt", OldName: ".tmpA", NewDirectory: "/ckpt", NewName: "ema.safetensors",
			}, stream)
			if err == nil || !strings.Contains(err.Error(), "move commit error") {
				t.Fatalf("StreamRenameEntry = %v, want the commit error", err)
			}
			if got := store.commitCount(); got != tc.wantCommits {
				t.Fatalf("commits = %d, want %d", got, tc.wantCommits)
			}
			if len(stream.sent) != 0 {
				t.Fatalf("client got %d responses for a rename that did not happen", len(stream.sent))
			}
			if events := queue.snapshot(); len(events) != 0 {
				t.Fatalf("metadata events = %d for a rename that did not happen", len(events))
			}
			if _, err := store.renameTestStore.FindEntry(context.Background(), "/ckpt/.tmpA"); err != nil {
				t.Fatalf("source after the refused rename: %v", err)
			}
			if _, err := store.renameTestStore.FindEntry(context.Background(), "/ckpt/ema.safetensors"); !errors.Is(err, filer_pb.ErrNotFound) {
				t.Fatalf("target after the refused rename: %v, want absent", err)
			}
		})
	}
}

// A directory's rename sends its children's events as it moves them, which
// keeps its client from timing out on a long one; having sent them, it is
// not run again.
func TestStreamRenameOfDirectoryIsNotRunAgain(t *testing.T) {
	store := newTxnRenameStore()
	store.entries["/a"] = newDirectoryEntry("/a", 1)
	store.entries["/a/f"] = fileEntryWithChunk("/a/f", 2, "1,0a01")
	store.refuse = writeConflict
	swapNotificationQueue(t, &captureQueue{})
	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
	stream := &commitStampedStream{store: store}

	err := server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
		OldDirectory: "/", OldName: "a", NewDirectory: "/", NewName: "b",
	}, stream)
	if err == nil || !strings.Contains(err.Error(), "move commit error") {
		t.Fatalf("StreamRenameEntry = %v, want the commit error", err)
	}
	if got := store.commitCount(); got != 1 {
		t.Fatalf("commits = %d, want 1: a directory rename is not run again", got)
	}
	if len(stream.sent) == 0 {
		t.Fatal("a directory rename streams its events as it moves the entries")
	}
}

// A file with a remote object is not run again: the move may have copied or
// deleted that object, outside the store's transaction.
func TestStreamRenameOfRemoteFileIsNotRunAgain(t *testing.T) {
	store := newTxnRenameStore()
	remote := fileEntryWithChunk("/ckpt/r.bin", 101, "1,0a01")
	remote.Remote = &filer_pb.RemoteEntry{StorageName: "s3", RemoteSize: 10, RemoteMtime: 1}
	store.entries["/ckpt/r.bin"] = remote
	store.refuse = writeConflict
	swapNotificationQueue(t, &captureQueue{})
	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
	stream := &commitStampedStream{store: store}

	err := server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
		OldDirectory: "/ckpt", OldName: "r.bin", NewDirectory: "/ckpt", NewName: "s.bin",
	}, stream)
	if err == nil || !strings.Contains(err.Error(), "move commit error") {
		t.Fatalf("StreamRenameEntry = %v, want the commit error", err)
	}
	if got := store.commitCount(); got != 1 {
		t.Fatalf("commits = %d, want 1: a remote file's rename is not run again", got)
	}
	if len(stream.sent) != 0 {
		t.Fatalf("client got %d responses for a rename that did not happen", len(stream.sent))
	}
}

// A store that does not declare isolating transactions keeps the responses
// going out as the entry moves, as before: one without transactions, where
// each write shows at once, and one that hands back a transaction context but
// does not say that its writes go through it.
func TestStreamRenameWithoutIsolatingTransactionsStreamsLive(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strictTx bool
	}{
		{"no transactions", false},
		{"transactions not declared isolating", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newRenameTestStore()
			store.strictTx = tc.strictTx
			store.entries["/ckpt"] = newDirectoryEntry("/ckpt", 100)
			store.entries["/ckpt/.tmpL"] = fileEntryWithChunk("/ckpt/.tmpL", 101, "1,0a01")
			swapNotificationQueue(t, &captureQueue{})
			server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
			stream := &renameLiveStream{store: store}

			if err := server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
				OldDirectory: "/ckpt", OldName: ".tmpL", NewDirectory: "/ckpt", NewName: "l.bin",
			}, stream); err != nil {
				t.Fatalf("StreamRenameEntry: %v", err)
			}
			if len(stream.sourceAt) != 2 || !stream.sourceAt[0] {
				t.Fatalf("responses saw the source as %v, want the first sent while the move was still under way", stream.sourceAt)
			}
		})
	}
}

// renameLiveStream records, for each response, whether the source entry was
// still in the store when it was sent.
type renameLiveStream struct {
	grpc.ServerStream
	store    *renameTestStore
	sourceAt []bool
}

func (s *renameLiveStream) Context() context.Context { return context.Background() }
func (s *renameLiveStream) Send(resp *filer_pb.StreamRenameEntryResponse) error {
	_, err := s.store.FindEntry(context.Background(), "/ckpt/.tmpL")
	s.sourceAt = append(s.sourceAt, err == nil)
	return nil
}

// A path-specific store's writes bypass the default store's transaction. A
// rename that writes there is not run again; one that stays in the default
// store is, as without the path-specific store.
func TestRenameWithPathSpecificStore(t *testing.T) {
	for _, tc := range []struct {
		name, newDir string
		wantCommits  int
	}{
		{"into it", "/ckpt/archive", 1},
		{"elsewhere", "/ckpt", 2},
	} {
		for _, stream := range []bool{true, false} {
			t.Run(tc.name+map[bool]string{true: "/stream", false: "/atomic"}[stream], func(t *testing.T) {
				store := newTxnRenameStore()
				store.entries["/ckpt/.tmpP"] = fileEntryWithChunk("/ckpt/.tmpP", 101, "1,0a01")
				if tc.wantCommits == 1 {
					store.refuse = writeConflict
				} else {
					flushedWhileRenaming(store, "/ckpt/.tmpP", fileEntryWithChunk("/ckpt/.tmpP", 101, "2,0b02"))
				}
				swapNotificationQueue(t, &captureQueue{})
				f := newRenameTestFiler(t, store)
				f.Store.(*filer.FilerStoreWrapper).AddPathSpecificStore("/ckpt/archive/", "archive", newRenameTestStore())
				server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}

				responses := &commitStampedStream{store: store}
				var err error
				if stream {
					err = server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
						OldDirectory: "/ckpt", OldName: ".tmpP", NewDirectory: tc.newDir, NewName: "p.bin",
					}, responses)
				} else {
					_, err = server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
						OldDirectory: "/ckpt", OldName: ".tmpP", NewDirectory: tc.newDir, NewName: "p.bin",
					})
				}
				if tc.wantCommits == 1 && (err == nil || !strings.Contains(err.Error(), "move commit error")) {
					t.Fatalf("rename = %v, want the commit error", err)
				} else if tc.wantCommits == 2 && err != nil {
					t.Fatalf("rename = %v, want it run again after the conflict", err)
				}
				if got := store.commitCount(); got != tc.wantCommits {
					t.Fatalf("commits = %d, want %d", got, tc.wantCommits)
				}
				if tc.wantCommits == 2 && stream {
					for i, commits := range responses.at {
						if commits != 2 {
							t.Fatalf("response %d went out after %d commits, want only after the rename committed", i, commits)
						}
					}
				}
			})
		}
	}
}

// canceledRenameStream stands for a client that went away before the rename
// sent anything.
type canceledRenameStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *canceledRenameStream) Context() context.Context { return s.ctx }
func (s *canceledRenameStream) Send(*filer_pb.StreamRenameEntryResponse) error {
	return s.ctx.Err()
}

// A rename whose client canceled before its first response is rolled back,
// as it is when the responses go out at once, not committed unseen.
func TestStreamRenameWhoseClientCanceledIsRolledBack(t *testing.T) {
	store := newTxnRenameStore()
	store.entries["/ckpt/.tmpC"] = fileEntryWithChunk("/ckpt/.tmpC", 101, "1,0a01")
	swapNotificationQueue(t, &captureQueue{})
	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
		OldDirectory: "/ckpt", OldName: ".tmpC", NewDirectory: "/ckpt", NewName: "c.bin",
	}, &canceledRenameStream{ctx: ctx})
	if err == nil {
		t.Fatal("StreamRenameEntry = nil, want the cancellation")
	}
	if got := store.commitCount(); got != 0 {
		t.Fatalf("commits = %d, want the canceled rename rolled back", got)
	}
	if _, err := store.renameTestStore.FindEntry(context.Background(), "/ckpt/.tmpC"); err != nil {
		t.Fatalf("source after the canceled rename: %v", err)
	}
}

// unreadRenameStream stands for a client that stopped reading: Send waits
// until the test lets it go.
type unreadRenameStream struct {
	grpc.ServerStream
	release chan struct{}
}

func (s *unreadRenameStream) Context() context.Context { return context.Background() }
func (s *unreadRenameStream) Send(*filer_pb.StreamRenameEntryResponse) error {
	<-s.release
	return nil
}

// A committed rename finishes (events, chunk deletions) before its held
// responses go out, so a client that stops reading cannot hold that up.
func TestStreamRenameFinishesBeforeTheClientReads(t *testing.T) {
	store := newTxnRenameStore()
	store.entries["/ckpt/.tmpB"] = fileEntryWithChunk("/ckpt/.tmpB", 101, "1,0a01")
	store.entries["/ckpt/b.bin"] = fileEntryWithChunk("/ckpt/b.bin", 102, "2,0b02")
	queue := &captureQueue{}
	swapNotificationQueue(t, queue)
	f := newRenameTestFiler(t, store)
	server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}
	stream := &unreadRenameStream{release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- server.StreamRenameEntry(&filer_pb.StreamRenameEntryRequest{
			OldDirectory: "/ckpt", OldName: ".tmpB", NewDirectory: "/ckpt", NewName: "b.bin",
		}, stream)
	}()
	defer func() {
		close(stream.release)
		if err := <-done; err != nil {
			t.Errorf("StreamRenameEntry: %v", err)
		}
	}()

	for deadline := time.Now().Add(10 * time.Second); len(queue.snapshot()) < 2 && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	if events := queue.snapshot(); len(events) != 2 {
		t.Fatalf("metadata events = %d while the client does not read, want the target's delete and the rename", len(events))
	}
	if got := queuedDeletions(f); len(got) != 1 || got[0] != "2,0b02" {
		t.Fatalf("queued chunk deletions = %v while the client does not read, want the replaced file's 2,0b02", got)
	}
}
