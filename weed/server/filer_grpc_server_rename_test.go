package weed_server

import (
	"context"
	"errors"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type renameTestStore struct {
	mu        sync.Mutex
	entries   map[string]*filer.Entry
	findCalls map[string]int
	commitErr error
	deleteErr error
	listErr   error         // simulates a transient store/RPC failure from a directory listing
	findDelay time.Duration // optional: widen check-then-act windows in tests
	findErr   error         // simulates a failing entry lookup
	strictTx  bool          // like a SQL store: calls through a committed transaction fail
	openTx    int           // strictTx transactions begun and not yet committed or rolled back

	kv        map[string][]byte // optional KV (hard links); nil: every key is absent
	kvGetErrs int               // fail this many KvGet calls before answering

	afterOp   func(op string, p util.FullPath) // optional: runs after each insert, update or delete
	afterFind func(p util.FullPath)            // optional: runs after each lookup
	listing   map[string]bool                  // optional: directory listing kept the way Redis keeps it

	overwritesAtOnce bool // declares filer.OverwritingStore, as the Redis stores do
}

// OverwritesAtOnce is what the store declares; its InsertEntry does replace an
// entry at once and whole either way.
func (s *renameTestStore) OverwritesAtOnce() bool { return s.overwritesAtOnce }

// noteOp runs the afterOp hook after a write, outside the store's lock.
func (s *renameTestStore) noteOp(op string, p util.FullPath) {
	if s.afterOp != nil {
		s.afterOp(op, p)
	}
}

type renameTestTxKey struct{}

type renameTestTx struct{ done bool }

// txDone reports a call through a transaction that has already committed or
// rolled back, which a SQL store refuses.
func (s *renameTestStore) txDone(ctx context.Context) error {
	if tx, ok := ctx.Value(renameTestTxKey{}).(*renameTestTx); ok && tx.done {
		return errors.New("sql: transaction has already been committed or rolled back")
	}
	return nil
}

func newRenameTestStore() *renameTestStore {
	return &renameTestStore{
		entries:   make(map[string]*filer.Entry),
		findCalls: make(map[string]int),
	}
}

func (s *renameTestStore) GetName() string                             { return "rename_test" }
func (s *renameTestStore) Initialize(util.Configuration, string) error { return nil }
func (s *renameTestStore) Shutdown()                                   {}
func (s *renameTestStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	if s.strictTx {
		s.mu.Lock()
		s.openTx++
		s.mu.Unlock()
		return context.WithValue(ctx, renameTestTxKey{}, &renameTestTx{}), nil
	}
	return ctx, nil
}

// endTx marks the transaction in ctx committed or rolled back.
func (s *renameTestStore) endTx(ctx context.Context) {
	if tx, ok := ctx.Value(renameTestTxKey{}).(*renameTestTx); ok {
		s.mu.Lock()
		if !tx.done {
			s.openTx--
			tx.done = true
		}
		s.mu.Unlock()
	}
}

func (s *renameTestStore) openTransactions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.openTx
}

func (s *renameTestStore) CommitTransaction(ctx context.Context) error {
	s.endTx(ctx)
	return s.commitErr
}
func (s *renameTestStore) RollbackTransaction(ctx context.Context) error {
	s.endTx(ctx)
	return nil
}
func (s *renameTestStore) KvPut(_ context.Context, key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kv != nil {
		s.kv[string(key)] = append([]byte(nil), value...)
	}
	return nil
}
func (s *renameTestStore) KvGet(_ context.Context, key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kvGetErrs > 0 {
		s.kvGetErrs--
		return nil, errors.New("kv get: transient failure")
	}
	if value, found := s.kv[string(key)]; found {
		return value, nil
	}
	return nil, filer.ErrKvNotFound
}
func (s *renameTestStore) KvDelete(_ context.Context, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.kv, string(key))
	return nil
}

func (s *renameTestStore) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	if err := s.txDone(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.entries[string(entry.FullPath)] = entry.ShallowClone()
	if s.listing != nil {
		s.listing[string(entry.FullPath)] = true
	}
	s.mu.Unlock()
	s.noteOp("insert", entry.FullPath)
	return nil
}

// UpdateEntry rewrites the entry and leaves the listing alone, as Redis does.
func (s *renameTestStore) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	if err := s.txDone(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.entries[string(entry.FullPath)] = entry.ShallowClone()
	s.mu.Unlock()
	s.noteOp("update", entry.FullPath)
	return nil
}

func (s *renameTestStore) FindEntry(ctx context.Context, p util.FullPath) (*filer.Entry, error) {
	if err := s.txDone(ctx); err != nil {
		return nil, err
	}
	if s.findDelay > 0 {
		time.Sleep(s.findDelay)
	}
	if s.afterFind != nil {
		defer s.afterFind(p)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.findCalls[string(p)]++
	if s.findErr != nil {
		return nil, s.findErr
	}
	entry, found := s.entries[string(p)]
	if !found {
		return nil, filer_pb.ErrNotFound
	}
	return entry.ShallowClone(), nil
}

func (s *renameTestStore) DeleteEntry(ctx context.Context, p util.FullPath) error {
	if err := s.txDone(ctx); err != nil {
		return err
	}
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.mu.Lock()
	delete(s.entries, string(p))
	if s.listing != nil {
		delete(s.listing, string(p))
	}
	s.mu.Unlock()
	s.noteOp("delete", p)
	return nil
}

func (s *renameTestStore) DeleteFolderChildren(_ context.Context, p util.FullPath) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := string(p) + "/"
	for path := range s.entries {
		if len(path) > len(prefix) && path[:len(prefix)] == prefix {
			delete(s.entries, path)
		}
	}
	return nil
}

func (s *renameTestStore) listDirectoryEntries(dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (string, error) {
	s.mu.Lock()
	if s.listErr != nil {
		err := s.listErr
		s.mu.Unlock()
		return "", err
	}
	var entries []*filer.Entry
	for path, entry := range s.entries {
		if path == string(dirPath) {
			continue
		}
		parent, _ := util.FullPath(path).DirAndName()
		if parent != string(dirPath) {
			continue
		}
		if prefix != "" && !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		entries = append(entries, entry.ShallowClone())
	}
	s.mu.Unlock()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	count := int64(0)
	lastFileName := ""
	for _, entry := range entries {
		name := entry.Name()
		if startFileName != "" {
			if includeStartFile {
				if name < startFileName {
					continue
				}
			} else if name <= startFileName {
				continue
			}
		}

		lastFileName = name
		if eachEntryFunc != nil {
			includeMore, err := eachEntryFunc(entry)
			if err != nil {
				return lastFileName, err
			}
			if !includeMore {
				return lastFileName, nil
			}
		}

		count++
		if limit > 0 && count >= limit {
			break
		}
	}

	return lastFileName, nil
}

func (s *renameTestStore) ListDirectoryEntries(_ context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (string, error) {
	return s.listDirectoryEntries(dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (s *renameTestStore) ListDirectoryPrefixedEntries(_ context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (string, error) {
	return s.listDirectoryEntries(dirPath, startFileName, includeStartFile, limit, prefix, eachEntryFunc)
}

func (s *renameTestStore) findEntryCallCount(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.findCalls[path]
}

type capturedEvent struct {
	key          string
	notification *filer_pb.EventNotification
}

type captureQueue struct {
	mu     sync.Mutex
	events []capturedEvent
}

var notificationQueueSwapMu sync.Mutex

func (q *captureQueue) GetName() string                             { return "capture" }
func (q *captureQueue) Initialize(util.Configuration, string) error { return nil }
func (q *captureQueue) SendMessage(key string, message proto.Message) error {
	notification, ok := message.(*filer_pb.EventNotification)
	if !ok {
		return nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.events = append(q.events, capturedEvent{
		key:          key,
		notification: proto.Clone(notification).(*filer_pb.EventNotification),
	})
	return nil
}

func (q *captureQueue) snapshot() []capturedEvent {
	q.mu.Lock()
	defer q.mu.Unlock()
	events := make([]capturedEvent, len(q.events))
	copy(events, q.events)
	return events
}

func swapNotificationQueue(t *testing.T, q notification.MessageQueue) {
	t.Helper()
	notificationQueueSwapMu.Lock()
	prevQueue := notification.Queue
	notification.Queue = q
	t.Cleanup(func() {
		notification.Queue = prevQueue
		notificationQueueSwapMu.Unlock()
	})
}

// newRenameTestFiler builds a filer whose meta log buffer is released when the
// test ends. A live LogBuffer holds PreviousBufferCount+1 buffers of BufferSize
// each, and its loop goroutines keep the whole filer reachable, so the tens of
// filers this package builds would otherwise pin gigabytes for the rest of the
// run - fatal on 32-bit, where that exhausts the address space.
func newRenameTestFiler(t *testing.T, store filer.FilerStore) *filer.Filer {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	masterClient := wdclient.NewMasterClient(
		dialOption,
		"test",
		cluster.FilerType,
		pb.ServerAddress("localhost:0"),
		"",
		"",
		*pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{}),
	)

	logBuffer := log_buffer.NewLogBuffer(
		"test",
		time.Minute,
		func(*log_buffer.LogBuffer, time.Time, time.Time, []byte, int64, int64) {},
		nil,
		func() {},
	)
	t.Cleanup(logBuffer.ShutdownLogBuffer)

	return &filer.Filer{
		// Use an explicit bucket root so ordinary fixture paths retain the
		// default collection. Collection tests configure their own bucket root.
		DirBucketsPath:      "/test-buckets",
		Store:               filer.NewFilerStoreWrapper(store),
		MasterClient:        masterClient,
		FilerConf:           filer.NewFilerConf(),
		RemoteStorage:       filer.NewFilerRemoteStorage(),
		MaxFilenameLength:   255,
		LocalMetaLogBuffer:  logBuffer,
		FileIdDeletionQueue: util.NewUnboundedQueue(),
	}
}

func newFileEntry(path string, inode uint64) *filer.Entry {
	now := time.Unix(1700000000, 0)
	return &filer.Entry{
		FullPath: util.FullPath(path),
		Attr: filer.Attr{
			Mtime:  now,
			Crtime: now,
			Mode:   0644,
			Inode:  inode,
		},
	}
}

func newDirectoryEntry(path string, inode uint64) *filer.Entry {
	now := time.Unix(1700000000, 0)
	return &filer.Entry{
		FullPath: util.FullPath(path),
		Attr: filer.Attr{
			Mtime:  now,
			Crtime: now,
			Mode:   os.ModeDir | 0755,
			Inode:  inode,
		},
	}
}

func TestAtomicRenameEntryEmitsLogicalRenameEvent(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/src.txt"] = newFileEntry("/src.txt", 101)

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
	_, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/",
		OldName:      "src.txt",
		NewDirectory: "/",
		NewName:      "dst.txt",
	})
	if err != nil {
		t.Fatalf("AtomicRenameEntry: %v", err)
	}

	events := queue.snapshot()
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}

	event := events[0]
	if event.key != "/src.txt" {
		t.Fatalf("event key = %q, want /src.txt", event.key)
	}
	if event.notification.OldEntry == nil || event.notification.OldEntry.Name != "src.txt" {
		t.Fatalf("old entry = %+v, want src.txt", event.notification.OldEntry)
	}
	if event.notification.NewEntry == nil || event.notification.NewEntry.Name != "dst.txt" {
		t.Fatalf("new entry = %+v, want dst.txt", event.notification.NewEntry)
	}
	if event.notification.NewParentPath != "/" {
		t.Fatalf("new parent path = %q, want /", event.notification.NewParentPath)
	}

	if _, err := store.FindEntry(context.Background(), "/src.txt"); err != filer_pb.ErrNotFound {
		t.Fatalf("source entry error = %v, want %v", err, filer_pb.ErrNotFound)
	}
	dst, err := store.FindEntry(context.Background(), "/dst.txt")
	if err != nil {
		t.Fatalf("find destination: %v", err)
	}
	if dst.Attr.Inode != 101 {
		t.Fatalf("destination inode = %d, want 101", dst.Attr.Inode)
	}
}

func TestAtomicRenameEntryOverwriteEmitsDeleteThenRename(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/src.txt"] = newFileEntry("/src.txt", 101)
	store.entries["/dst.txt"] = newFileEntry("/dst.txt", 202)

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
	_, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/",
		OldName:      "src.txt",
		NewDirectory: "/",
		NewName:      "dst.txt",
	})
	if err != nil {
		t.Fatalf("AtomicRenameEntry: %v", err)
	}

	events := queue.snapshot()
	if len(events) != 2 {
		t.Fatalf("event count = %d, want 2", len(events))
	}

	deleteEvent := events[0]
	if deleteEvent.key != "/dst.txt" {
		t.Fatalf("delete event key = %q, want /dst.txt", deleteEvent.key)
	}
	if deleteEvent.notification.OldEntry == nil || deleteEvent.notification.OldEntry.Name != "dst.txt" {
		t.Fatalf("delete old entry = %+v, want dst.txt", deleteEvent.notification.OldEntry)
	}
	if deleteEvent.notification.NewEntry != nil {
		t.Fatalf("delete new entry = %+v, want nil", deleteEvent.notification.NewEntry)
	}
	if !deleteEvent.notification.DeleteChunks {
		t.Fatal("delete event should delete chunks")
	}

	renameEvent := events[1]
	if renameEvent.key != "/src.txt" {
		t.Fatalf("rename event key = %q, want /src.txt", renameEvent.key)
	}
	if renameEvent.notification.OldEntry == nil || renameEvent.notification.OldEntry.Name != "src.txt" {
		t.Fatalf("rename old entry = %+v, want src.txt", renameEvent.notification.OldEntry)
	}
	if renameEvent.notification.NewEntry == nil || renameEvent.notification.NewEntry.Name != "dst.txt" {
		t.Fatalf("rename new entry = %+v, want dst.txt", renameEvent.notification.NewEntry)
	}
	if renameEvent.notification.NewParentPath != "/" {
		t.Fatalf("rename new parent path = %q, want /", renameEvent.notification.NewParentPath)
	}

	if _, err := store.FindEntry(context.Background(), "/src.txt"); err != filer_pb.ErrNotFound {
		t.Fatalf("source entry error = %v, want %v", err, filer_pb.ErrNotFound)
	}
	dst, err := store.FindEntry(context.Background(), "/dst.txt")
	if err != nil {
		t.Fatalf("find destination: %v", err)
	}
	if dst.Attr.Inode != 101 {
		t.Fatalf("destination inode = %d, want 101", dst.Attr.Inode)
	}
}

func TestAtomicRenameEntryDoesNotEmitEventOnDeleteFailure(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/src.txt"] = newFileEntry("/src.txt", 101)
	store.deleteErr = errors.New("delete failed")

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
	_, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/",
		OldName:      "src.txt",
		NewDirectory: "/",
		NewName:      "dst.txt",
	})
	if err == nil {
		t.Fatal("expected delete failure")
	}

	if events := queue.snapshot(); len(events) != 0 {
		t.Fatalf("event count = %d, want 0", len(events))
	}
}

func TestAtomicRenameEntryDoesNotEmitEventOnCommitFailure(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/src.txt"] = newFileEntry("/src.txt", 101)
	store.commitErr = errors.New("commit failed")

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
	_, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/",
		OldName:      "src.txt",
		NewDirectory: "/",
		NewName:      "dst.txt",
	})
	if err == nil {
		t.Fatal("expected commit failure")
	}

	if events := queue.snapshot(); len(events) != 0 {
		t.Fatalf("event count = %d, want 0", len(events))
	}
}

func TestAtomicRenameEntrySkipsDescendantTargetLookups(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/srcdir"] = newDirectoryEntry("/srcdir", 100)
	store.entries["/srcdir/subdir"] = newDirectoryEntry("/srcdir/subdir", 101)
	store.entries["/srcdir/subdir/file.txt"] = newFileEntry("/srcdir/subdir/file.txt", 102)

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	server := &FilerServer{filer: newRenameTestFiler(t, store), entryLockTable: util.NewLockTable[util.FullPath]()}
	_, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/",
		OldName:      "srcdir",
		NewDirectory: "/",
		NewName:      "dstdir",
	})
	if err != nil {
		t.Fatalf("AtomicRenameEntry: %v", err)
	}

	for _, target := range []string{"/dstdir/subdir", "/dstdir/subdir/file.txt"} {
		if calls := store.findEntryCallCount(target); calls != 0 {
			t.Fatalf("FindEntry(%q) called %d times, want 0", target, calls)
		}
	}

	for _, target := range []string{"/dstdir", "/dstdir/subdir", "/dstdir/subdir/file.txt"} {
		if _, err := store.FindEntry(context.Background(), util.FullPath(target)); err != nil {
			t.Fatalf("find renamed target %q: %v", target, err)
		}
	}

	if got := len(queue.snapshot()); got != 3 {
		t.Fatalf("event count = %d, want 3", got)
	}
}

// The stamps queued rename events carry are released when the rename fails:
// left in flight they would pin the delivered watermark below the aborted
// rename forever.
func TestAtomicRenameReleasesQueuedStampsOnCommitFailure(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/dir"] = newDirectoryEntry("/dir", 100)
	store.entries["/dir/a.txt"] = newFileEntry("/dir/a.txt", 101)
	store.entries["/dir/b.txt"] = newFileEntry("/dir/b.txt", 102)
	store.commitErr = errors.New("commit failed")

	f := newRenameTestFiler(t, store)
	server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}
	_, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/",
		OldName:      "dir",
		NewDirectory: "/",
		NewName:      "dstdir",
	})
	if err == nil {
		t.Fatal("expected commit failure")
	}
	now := time.Now().UnixNano()
	if got := f.LocalDeliveredThroughTsNs(now); got != now {
		t.Fatalf("LocalDeliveredThroughTsNs = %d, want %d: a leaked stamp still caps the claim", got, now)
	}
}

func TestAtomicRenameEntryRejectsDotSegmentTarget(t *testing.T) {
	for _, tc := range []struct{ dir, name string }{
		{"/../escape", "dst.txt"},
		{"/", ".."},
	} {
		store := newRenameTestStore()
		store.entries["/src.txt"] = newFileEntry("/src.txt", 101)
		f := newRenameTestFiler(t, store)
		f.DirBucketsPath = "/buckets"
		server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}

		_, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: "/",
			OldName:      "src.txt",
			NewDirectory: tc.dir,
			NewName:      tc.name,
		})
		if err == nil {
			t.Fatalf("rename to %s/%s must be rejected", tc.dir, tc.name)
		}
		if _, found := store.entries["/src.txt"]; !found {
			t.Fatalf("rename to %s/%s removed the source", tc.dir, tc.name)
		}
		if len(store.entries) != 1 {
			t.Fatalf("rename to %s/%s left entries %v", tc.dir, tc.name, store.entries)
		}
	}
}

// A peer filer with its own store applies only the events it is sent, so the
// directories a rename creates at its destination need events of their own.
func TestAtomicRenameEntryAnnouncesCreatedParents(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/buckets"] = newDirectoryEntry("/buckets", 100)
	store.entries["/buckets/arc"] = newDirectoryEntry("/buckets/arc", 101)
	store.entries["/buckets/arc/xxx"] = newDirectoryEntry("/buckets/arc/xxx", 102)
	store.entries["/buckets/arc/xxx/rate_data"] = newFileEntry("/buckets/arc/xxx/rate_data", 103)

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	f := newRenameTestFiler(t, store)
	server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}
	_, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/buckets/arc/xxx",
		OldName:      "rate_data",
		NewDirectory: "/buckets/arc/keep/2026/xxx",
		NewName:      "rate_data",
	})
	if err != nil {
		t.Fatalf("AtomicRenameEntry: %v", err)
	}

	peer := newRenameTestFiler(t, newRenameTestStore())
	keys := replayCapturedEvents(t, peer, queue)
	wantKeys := []string{"/buckets/arc/keep", "/buckets/arc/keep/2026", "/buckets/arc/keep/2026/xxx", "/buckets/arc/xxx/rate_data"}
	if !slices.Equal(keys, wantKeys) {
		t.Fatalf("event keys = %v, want %v", keys, wantKeys)
	}

	for dir, name := range map[string]string{
		"/buckets/arc":               "keep",
		"/buckets/arc/keep":          "2026",
		"/buckets/arc/keep/2026":     "xxx",
		"/buckets/arc/keep/2026/xxx": "rate_data",
	} {
		var listed []string
		if _, err := peer.Store.ListDirectoryEntries(context.Background(), util.FullPath(dir), "", false, 100, func(entry *filer.Entry) (bool, error) {
			listed = append(listed, entry.Name())
			return true, nil
		}); err != nil {
			t.Fatalf("list %s on the peer: %v", dir, err)
		}
		if !slices.Contains(listed, name) {
			t.Fatalf("peer lists %v under %s, want %s", listed, dir, name)
		}
	}

	now := time.Now().UnixNano()
	if got := f.LocalDeliveredThroughTsNs(now); got != now {
		t.Fatalf("LocalDeliveredThroughTsNs = %d, want %d: a parent's stamp was never adopted", got, now)
	}
}

func replayCapturedEvents(t *testing.T, peer *filer.Filer, queue *captureQueue) (keys []string) {
	t.Helper()
	for _, event := range queue.snapshot() {
		keys = append(keys, event.key)
		dir, _ := util.FullPath(event.key).DirAndName()
		if err := peer.Replay(&filer_pb.SubscribeMetadataResponse{
			Directory:         dir,
			EventNotification: event.notification,
			TsNs:              time.Now().UnixNano(),
		}); err != nil {
			t.Fatalf("replay %s: %v", event.key, err)
		}
	}
	return keys
}

func TestAtomicRenameEntryAnnouncesPromotedParent(t *testing.T) {
	seed := func() *renameTestStore {
		store := newRenameTestStore()
		store.entries["/buckets"] = newDirectoryEntry("/buckets", 100)
		store.entries["/buckets/arc"] = newDirectoryEntry("/buckets/arc", 101)
		store.entries["/buckets/arc/report"] = newFileEntry("/buckets/arc/report", 102)
		return store
	}
	store := seed()
	store.entries["/buckets/arc/rate_data"] = newFileEntry("/buckets/arc/rate_data", 103)

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	f := newRenameTestFiler(t, store)
	server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}
	if _, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/buckets/arc",
		OldName:      "rate_data",
		NewDirectory: "/buckets/arc/report",
		NewName:      "rate_data",
	}); err != nil {
		t.Fatalf("AtomicRenameEntry: %v", err)
	}

	peer := newRenameTestFiler(t, seed())
	keys := replayCapturedEvents(t, peer, queue)
	if wantKeys := []string{"/buckets/arc/report", "/buckets/arc/rate_data"}; !slices.Equal(keys, wantKeys) {
		t.Fatalf("event keys = %v, want %v", keys, wantKeys)
	}
	report, err := peer.Store.FindEntry(context.Background(), "/buckets/arc/report")
	if err != nil {
		t.Fatalf("find the promoted parent on the peer: %v", err)
	}
	if !report.IsDirectory() {
		t.Fatalf("peer keeps %s as a file, mode %v", report.FullPath, report.Mode)
	}
	if _, err := peer.Store.FindEntry(context.Background(), "/buckets/arc/report/rate_data"); err != nil {
		t.Fatalf("find the moved entry on the peer: %v", err)
	}

	now := time.Now().UnixNano()
	if got := f.LocalDeliveredThroughTsNs(now); got != now {
		t.Fatalf("LocalDeliveredThroughTsNs = %d, want %d: the promotion stamp was never adopted", got, now)
	}
}

func TestAtomicRenameReleasesCreatedParentStampsOnCommitFailure(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/src.txt"] = newFileEntry("/src.txt", 101)
	store.commitErr = errors.New("commit failed")

	queue := &captureQueue{}
	swapNotificationQueue(t, queue)

	f := newRenameTestFiler(t, store)
	server := &FilerServer{filer: f, entryLockTable: util.NewLockTable[util.FullPath]()}
	if _, err := server.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
		OldDirectory: "/",
		OldName:      "src.txt",
		NewDirectory: "/a/b",
		NewName:      "dst.txt",
	}); err == nil {
		t.Fatal("expected commit failure")
	}
	if events := queue.snapshot(); len(events) != 0 {
		t.Fatalf("a failed rename published %d events", len(events))
	}
	now := time.Now().UnixNano()
	if got := f.LocalDeliveredThroughTsNs(now); got != now {
		t.Fatalf("LocalDeliveredThroughTsNs = %d, want %d: a leaked parent stamp still caps the claim", got, now)
	}
}
