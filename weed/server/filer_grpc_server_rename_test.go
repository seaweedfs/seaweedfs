package weed_server

import (
	"context"
	"errors"
	"os"
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
	return ctx, nil
}
func (s *renameTestStore) CommitTransaction(context.Context) error   { return s.commitErr }
func (s *renameTestStore) RollbackTransaction(context.Context) error { return nil }
func (s *renameTestStore) KvPut(context.Context, []byte, []byte) error {
	return nil
}
func (s *renameTestStore) KvGet(context.Context, []byte) ([]byte, error) {
	return nil, filer.ErrKvNotFound
}
func (s *renameTestStore) KvDelete(context.Context, []byte) error { return nil }

func (s *renameTestStore) InsertEntry(_ context.Context, entry *filer.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[string(entry.FullPath)] = entry.ShallowClone()
	return nil
}

func (s *renameTestStore) UpdateEntry(_ context.Context, entry *filer.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[string(entry.FullPath)] = entry.ShallowClone()
	return nil
}

func (s *renameTestStore) FindEntry(_ context.Context, p util.FullPath) (*filer.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.findCalls[string(p)]++
	entry, found := s.entries[string(p)]
	if !found {
		return nil, filer_pb.ErrNotFound
	}
	return entry.ShallowClone(), nil
}

func (s *renameTestStore) DeleteEntry(_ context.Context, p util.FullPath) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, string(p))
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

func newRenameTestFiler(store *renameTestStore) *filer.Filer {
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

	return &filer.Filer{
		Store:             filer.NewFilerStoreWrapper(store),
		MasterClient:      masterClient,
		FilerConf:         filer.NewFilerConf(),
		RemoteStorage:     filer.NewFilerRemoteStorage(),
		MaxFilenameLength: 255,
		LocalMetaLogBuffer: log_buffer.NewLogBuffer(
			"test",
			time.Minute,
			func(*log_buffer.LogBuffer, time.Time, time.Time, []byte, int64, int64) {},
			nil,
			func() {},
		),
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

	server := &FilerServer{filer: newRenameTestFiler(store)}
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

	server := &FilerServer{filer: newRenameTestFiler(store)}
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

	server := &FilerServer{filer: newRenameTestFiler(store)}
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

	server := &FilerServer{filer: newRenameTestFiler(store)}
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

	server := &FilerServer{filer: newRenameTestFiler(store)}
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
