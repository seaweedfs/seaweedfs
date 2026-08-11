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

// fakeReplayStore fails InsertEntry with a fixed error for its first
// failUntil calls, then succeeds. Everything else is a no-op: Replay only
// exercises InsertEntry/DeleteEntry for a create-only event.
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

// TestReplicateMetadataChangeRetriesTransientFailure pins the regression: a
// filer with its own store (leveldb3-per-pod, no shared backend) that hits one
// transient error while replaying a peer's bucket-quota update used to log it
// and move on, per the old body of maybeReplicateMetadataChange in
// doSubscribeToOneFiler ("if err := Replay(...); err != nil { glog.Errorf(...);
// return }"). The offset still advanced past the event afterwards, so the
// bucket's quota on this filer diverged from the peer permanently - exactly
// the mechanism behind three filers enforcing three different quota values for
// the same bucket. With the retry this test would fail: a single failure would
// leave the store never updated. It passes now because the transient failure
// gets a second attempt.
func TestReplicateMetadataChangeRetriesTransientFailure(t *testing.T) {
	// Typed so the value stays int64 when passed to t.Fatalf's ...any; an untyped
	// constant defaults to int and overflows a 32-bit build.
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

// TestReplicateMetadataChangeGivesUpLoudlyOnPermanentFailure covers the other
// side of the tradeoff: an event that can never replay must not be retried
// forever, since doing so inside the subscribe stream would block every later
// event from that peer behind it. It must also stop being silent, which is
// the defect this change exists to fix: today's glog.Errorf-and-move-on has no
// counter and no bounded retry, so a permanent failure and a transient one are
// indistinguishable from the outside.
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
