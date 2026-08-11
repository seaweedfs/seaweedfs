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

// flakyReplayStore fails InsertEntry for its first failUntil calls, then
// delegates to the embedded stubFilerStore.
type flakyReplayStore struct {
	*stubFilerStore
	countMu   sync.Mutex
	failUntil int
	err       error
	attempts  int
}

func (s *flakyReplayStore) InsertEntry(ctx context.Context, entry *Entry) error {
	s.countMu.Lock()
	s.attempts++
	fail := s.attempts <= s.failUntil
	s.countMu.Unlock()
	if fail {
		return s.err
	}
	return s.stubFilerStore.InsertEntry(ctx, entry)
}

func (s *flakyReplayStore) attemptCount() int {
	s.countMu.Lock()
	defer s.countMu.Unlock()
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

// The old body logged a Replay error and returned, so one transient failure
// permanently diverged the entry from the peer.
func TestReplicateMetadataChangeRetriesTransientFailure(t *testing.T) {
	// int64: untyped, the value defaults to int and overflows a 32-bit build
	const wantQuota int64 = 131072 << 20

	store := &flakyReplayStore{
		stubFilerStore: newStubFilerStore(),
		failUntil:      1,
		err:            errors.New("i/o timeout talking to store"),
	}
	peer := pb.ServerAddress("peer-transient:1")

	replicateMetadataChange(store, peer, quotaChangeEvent("my-bucket", wantQuota))

	inserted, err := store.FindEntry(context.Background(), util.NewFullPath("/buckets", "my-bucket"))
	if err != nil {
		t.Fatal("expected the entry to be inserted once the transient failure is retried past")
	}
	if inserted.Quota != wantQuota {
		t.Fatalf("quota = %d, want %d", inserted.Quota, wantQuota)
	}
	if got := store.attemptCount(); got < 2 {
		t.Fatalf("attempts = %d, want at least 2", got)
	}
}

// An event that can never replay must fail fast instead of blocking the
// subscribe stream, and must be counted so the divergence is not silent.
func TestReplicateMetadataChangeGivesUpLoudlyOnPermanentFailure(t *testing.T) {
	store := &flakyReplayStore{
		stubFilerStore: newStubFilerStore(),
		failUntil:      1 << 30,
		err:            errors.New("entry checksum mismatch"),
	}
	peer := pb.ServerAddress("peer-permanent:1")

	before := replayFailureCount(t, peer)

	replicateMetadataChange(store, peer, quotaChangeEvent("poison-bucket", 65536<<20))

	if got := store.attemptCount(); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1: a non-transient error must fail fast, not retry", got)
	}
	if got := replayFailureCount(t, peer); got != before+1 {
		t.Fatalf("FilerMetaAggregatorReplayFailures[%s] = %v, want %v", peer, got, before+1)
	}
}
