package filer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// fakeReplayStore fails InsertEntry for its first failUntil calls, then
// succeeds. The other methods can be no-ops because Replay of a create-only
// event only reaches InsertEntry.
type fakeReplayStore struct {
	mu        sync.Mutex
	failUntil int
	err       error
	attempts  int
	inserted  *Entry
}

func (s *fakeReplayStore) GetName() string                             { return "fake-replay-store" }
func (s *fakeReplayStore) Initialize(util.Configuration, string) error { return nil }
func (s *fakeReplayStore) UpdateEntry(context.Context, *Entry) error   { return nil }
func (s *fakeReplayStore) FindEntry(context.Context, util.FullPath) (*Entry, error) {
	return nil, filer_pb.ErrNotFound
}
func (s *fakeReplayStore) DeleteEntry(context.Context, util.FullPath) error          { return nil }
func (s *fakeReplayStore) DeleteFolderChildren(context.Context, util.FullPath) error { return nil }
func (s *fakeReplayStore) ListDirectoryEntries(context.Context, util.FullPath, string, bool, int64, ListEachEntryFunc) (string, error) {
	return "", nil
}
func (s *fakeReplayStore) ListDirectoryPrefixedEntries(context.Context, util.FullPath, string, bool, int64, string, ListEachEntryFunc) (string, error) {
	return "", nil
}
func (s *fakeReplayStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (s *fakeReplayStore) CommitTransaction(context.Context) error     { return nil }
func (s *fakeReplayStore) RollbackTransaction(context.Context) error   { return nil }
func (s *fakeReplayStore) KvPut(context.Context, []byte, []byte) error { return nil }
func (s *fakeReplayStore) KvGet(context.Context, []byte) ([]byte, error) {
	return nil, ErrKvNotFound
}
func (s *fakeReplayStore) KvDelete(context.Context, []byte) error { return nil }
func (s *fakeReplayStore) Shutdown()                              {}

func (s *fakeReplayStore) InsertEntry(_ context.Context, entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts++
	if s.attempts <= s.failUntil {
		return s.err
	}
	s.inserted = entry
	return nil
}

func (s *fakeReplayStore) insertedEntry() *Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inserted
}

func (s *fakeReplayStore) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func replayFailureCount(t *testing.T, peer pb.ServerAddress) float64 {
	t.Helper()
	var m dto.Metric
	if err := stats.FilerMetaAggregatorReplayFailures.WithLabelValues(string(peer)).Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

func quotaChangeEvent(bucket string, quota int64) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets",
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: bucket, IsDirectory: true, Quota: quota},
		},
		TsNs: time.Now().UnixNano(),
	}
}

// maybeReplicateMetadataChange used to log a Replay error and return while the
// offset advanced past the event anyway. On a filer with its own store rather
// than a shared backend, that left the entry diverged from the peer for good.
// Against that body this test fails: a single failure and the store is never
// updated.
func TestReplicateMetadataChangeRetriesTransientFailure(t *testing.T) {
	// Typed so the value stays int64 in t.Fatalf's ...any; untyped it defaults to
	// int and overflows a 32-bit build.
	const wantQuota int64 = 131072 << 20

	store := &fakeReplayStore{failUntil: 1, err: errors.New("i/o timeout talking to store")}
	peer := pb.ServerAddress("peer-transient:1")
	event := quotaChangeEvent("my-bucket", wantQuota)

	replicateMetadataChange(store, peer, event)

	inserted := store.insertedEntry()
	if inserted == nil {
		t.Fatal("expected the entry to be inserted once the transient failure is retried past")
	}
	if inserted.Quota != wantQuota {
		t.Fatalf("quota = %d, want %d", inserted.Quota, wantQuota)
	}
	if got := store.attemptCount(); got < 2 {
		t.Fatalf("attempts = %d, want at least 2: a one-shot Replay would have swallowed the failure and never retried", got)
	}
}

// The other side of the tradeoff: an event that can never replay must not be
// retried forever, since retrying inside the subscribe stream would block every
// later event from that peer. It must still be counted, or a permanent failure
// is indistinguishable from a transient one from the outside.
func TestReplicateMetadataChangeGivesUpLoudlyOnPermanentFailure(t *testing.T) {
	store := &fakeReplayStore{failUntil: 1 << 30, err: errors.New("entry checksum mismatch")}
	peer := pb.ServerAddress("peer-permanent:1")
	event := quotaChangeEvent("poison-bucket", 65536<<20)

	before := replayFailureCount(t, peer)

	done := make(chan struct{})
	go func() {
		replicateMetadataChange(store, peer, event)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("replicateMetadataChange blocked instead of giving up on a permanently failing event")
	}

	if got := store.attemptCount(); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1: a non-transient error must fail fast, not retry", got)
	}
	if got := replayFailureCount(t, peer); got != before+1 {
		t.Fatalf("FilerMetaAggregatorReplayFailures[%s] = %v, want %v: a permanently failing replay must be counted, not silent", peer, got, before+1)
	}
}

// fakeMultiStepReplayStore models a store whose DeleteEntry, like the redis
// families, removes the primary key and the parent-directory membership as
// two separate steps, and whose InsertEntry likewise sets the primary key
// before adding the membership. Each can be made to fail after mutating but
// before the membership step, for a configured number of attempts.
type fakeMultiStepReplayStore struct {
	mu          sync.Mutex
	entries     map[string]*Entry
	dirChildren map[string]map[string]bool

	deleteAttempts, deleteFailUntil int
	deleteErr                       error

	insertAttempts, insertFailUntil int
	insertErr                       error
}

func newFakeMultiStepReplayStore() *fakeMultiStepReplayStore {
	return &fakeMultiStepReplayStore{
		entries:     make(map[string]*Entry),
		dirChildren: make(map[string]map[string]bool),
	}
}

func (s *fakeMultiStepReplayStore) seed(entry *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[string(entry.FullPath)] = entry
	dir, name := entry.FullPath.DirAndName()
	if s.dirChildren[dir] == nil {
		s.dirChildren[dir] = make(map[string]bool)
	}
	s.dirChildren[dir][name] = true
}

func (s *fakeMultiStepReplayStore) hasChild(dir, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dirChildren[dir][name]
}

func (s *fakeMultiStepReplayStore) entry(path util.FullPath) *Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[string(path)]
}

func (s *fakeMultiStepReplayStore) GetName() string                             { return "fake-multi-step-store" }
func (s *fakeMultiStepReplayStore) Initialize(util.Configuration, string) error { return nil }
func (s *fakeMultiStepReplayStore) UpdateEntry(context.Context, *Entry) error   { return nil }

func (s *fakeMultiStepReplayStore) FindEntry(_ context.Context, fp util.FullPath) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[string(fp)]
	if !ok {
		return nil, filer_pb.ErrNotFound
	}
	return entry, nil
}

// DeleteEntry mirrors UniversalRedisStore.DeleteEntry: the primary key is
// removed before the parent-directory membership, so a failure in between
// leaves the membership stale.
func (s *fakeMultiStepReplayStore) DeleteEntry(_ context.Context, fp util.FullPath) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteAttempts++
	delete(s.entries, string(fp))
	if s.deleteAttempts <= s.deleteFailUntil {
		return s.deleteErr
	}
	dir, name := fp.DirAndName()
	if s.dirChildren[dir] != nil {
		delete(s.dirChildren[dir], name)
	}
	return nil
}

// InsertEntry mirrors UniversalRedisStore.InsertEntry: the primary key is set
// before the parent-directory membership is added.
func (s *fakeMultiStepReplayStore) InsertEntry(_ context.Context, entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertAttempts++
	s.entries[string(entry.FullPath)] = entry
	if s.insertAttempts <= s.insertFailUntil {
		return s.insertErr
	}
	dir, name := entry.FullPath.DirAndName()
	if s.dirChildren[dir] == nil {
		s.dirChildren[dir] = make(map[string]bool)
	}
	s.dirChildren[dir][name] = true
	return nil
}

func (s *fakeMultiStepReplayStore) DeleteFolderChildren(context.Context, util.FullPath) error {
	return nil
}
func (s *fakeMultiStepReplayStore) ListDirectoryEntries(context.Context, util.FullPath, string, bool, int64, ListEachEntryFunc) (string, error) {
	return "", nil
}
func (s *fakeMultiStepReplayStore) ListDirectoryPrefixedEntries(context.Context, util.FullPath, string, bool, int64, string, ListEachEntryFunc) (string, error) {
	return "", nil
}
func (s *fakeMultiStepReplayStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (s *fakeMultiStepReplayStore) CommitTransaction(context.Context) error     { return nil }
func (s *fakeMultiStepReplayStore) RollbackTransaction(context.Context) error   { return nil }
func (s *fakeMultiStepReplayStore) KvPut(context.Context, []byte, []byte) error { return nil }
func (s *fakeMultiStepReplayStore) KvGet(context.Context, []byte) ([]byte, error) {
	return nil, ErrKvNotFound
}
func (s *fakeMultiStepReplayStore) KvDelete(context.Context, []byte) error { return nil }
func (s *fakeMultiStepReplayStore) Shutdown()                              {}

func renameEvent(dir, oldName, newName string, quota int64) *filer_pb.SubscribeMetadataResponse {
	return &filer_pb.SubscribeMetadataResponse{
		Directory: dir,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: oldName, IsDirectory: true},
			NewEntry: &filer_pb.Entry{Name: newName, IsDirectory: true, Quota: quota},
		},
		TsNs: time.Now().UnixNano(),
	}
}

// CodeRabbit's review flagged that FilerStoreWrapper.DeleteEntry skips the
// delete once FindEntry reports the path gone, and that the redis store
// families remove the primary key before the parent-directory membership.
// Chained together: a delete that fails between those two steps is retried
// as a no-op (FindEntry now reports "already gone"), the stale membership is
// never revisited, replicateMetadataChange reports overall success, and
// FilerMetaAggregatorReplayFailures never fires.
//
// This wraps the fake store in the real FilerStoreWrapper (as production
// does via Filer.Store) rather than passing the fake directly, so the
// FindEntry short-circuit under test is the real one. The hazard is
// inherited from FilerStoreWrapper.DeleteEntry and the store layer, not
// created by this retry (see Replay's doc comment): a single non-retried
// Replay call already leaves the same stale membership behind, just with an
// error surfaced. What retry changes here is that it also hides it.
func TestReplicateMetadataChangeRetryHidesStaleDirectoryIndexOnDelete(t *testing.T) {
	// Typed so the value stays int64 in t.Fatalf's ...any; untyped it defaults to
	// int and overflows a 32-bit build.
	const wantQuota int64 = 4096 << 20
	const dir = "/buckets"
	fake := newFakeMultiStepReplayStore()
	fake.seed(&Entry{FullPath: util.NewFullPath(dir, "old-name")})
	fake.deleteFailUntil = 1
	fake.deleteErr = errors.New("connection reset by peer")
	fake.insertFailUntil = 1
	fake.insertErr = errors.New("connection reset by peer")

	store := NewFilerStoreWrapper(fake)
	peer := pb.ServerAddress("peer-rename:1")
	event := renameEvent(dir, "old-name", "new-name", wantQuota)

	before := replayFailureCount(t, peer)

	replicateMetadataChange(store, peer, event)

	if got := replayFailureCount(t, peer); got != before {
		t.Fatalf("FilerMetaAggregatorReplayFailures[%s] = %v, want unchanged at %v: replay reported overall success", peer, got, before)
	}
	if entry := fake.entry(util.NewFullPath(dir, "old-name")); entry != nil {
		t.Fatalf("old entry %v should have been deleted", entry.FullPath)
	}
	newEntry := fake.entry(util.NewFullPath(dir, "new-name"))
	if newEntry == nil {
		t.Fatal("expected the new entry to be inserted once its transient failure is retried past")
	}
	if newEntry.Quota != wantQuota {
		t.Fatalf("quota = %d, want %d", newEntry.Quota, wantQuota)
	}
	if !fake.hasChild(dir, "new-name") {
		t.Fatal("new entry should be present in the parent directory's listing")
	}
	if !fake.hasChild(dir, "old-name") {
		t.Fatal("expected the known stale-directory-index hazard: the old name is still listed in the parent directory even though replay reported success. If this now fails, the hazard was fixed at the store layer and this test (and the doc comments referencing it) should be updated")
	}
}
