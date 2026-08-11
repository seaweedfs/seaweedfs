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
