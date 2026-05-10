package dispatcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errFilerStore satisfies FilerStore with a forced Read error so the
// Load path's "non-NotFound transport error" branch is reachable.
// Save mirrors the pattern; tests for the nil-Store branch don't need
// a stub at all.
type errFilerStore struct {
	readErr  error
	saveErr  error
	readData []byte
}

func (e *errFilerStore) Read(_ context.Context, _, _ string) ([]byte, error) {
	if e.readErr != nil {
		return nil, e.readErr
	}
	return e.readData, nil
}
func (e *errFilerStore) Save(_ context.Context, _, _ string, _ []byte) error {
	return e.saveErr
}

// ---------- FilerPersister edges ----------

func TestFilerPersisterLoad_NilStoreErrors(t *testing.T) {
	// The persister's Store dependency is required; loading with a nil
	// Store must error rather than panic on the Read call.
	p := &FilerPersister{}
	_, err := p.Load(context.Background(), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil Store")
}

func TestFilerPersisterSave_NilStoreErrors(t *testing.T) {
	p := &FilerPersister{}
	err := p.Save(context.Background(), 0, map[s3lifecycle.ActionKey]int64{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil Store")
}

func TestFilerPersisterLoad_NonNotFoundErrorIsWrapped(t *testing.T) {
	// A transport-level error (anything that isn't ErrNotFound) wraps
	// with the shard ID context so operators can attribute it. Pin
	// both that the error surfaces and that the original is recoverable
	// via errors.Is so a caller can distinguish.
	want := errors.New("filer-side bang")
	p := &FilerPersister{Store: &errFilerStore{readErr: want}}
	_, err := p.Load(context.Background(), 3)
	require.Error(t, err)
	assert.ErrorIs(t, err, want)
	assert.Contains(t, err.Error(), "shard=3")
}

func TestFilerPersisterLoad_EmptyContentReturnsEmptyMap(t *testing.T) {
	// Read returning a successful empty []byte means "file exists but
	// is zero length"; the persister must treat this as "no entries"
	// rather than try to JSON-decode an empty slice and return an
	// error.
	p := &FilerPersister{Store: &errFilerStore{readData: []byte{}}}
	got, err := p.Load(context.Background(), 0)
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

// ---------- Tick edges ----------

func TestTick_InitializesRetriesMapOnFirstCall(t *testing.T) {
	// retries is a lazy map; a Dispatcher constructed without it must
	// not panic on the first Tick. Pin that handleRetryLater can write
	// into the map afterwards (proven by the dispatcher returning an
	// integer rather than crashing).
	d, sched := newDispatcher(&fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}, nil
		},
	})
	require.Nil(t, d.retries, "preconditions: retries map starts nil")
	t0 := time.Now()
	sched.Add(router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindExpirationDays},
		ObjectKey: "k",
		DueTime:   t0,
	})
	processed := d.Tick(context.Background(), t0.Add(time.Hour))
	assert.Equal(t, 1, processed)
	assert.NotNil(t, d.retries, "Tick must initialize the retries map")
}

func TestTick_CtxShutdownMidLoopRequeuesAndReturnsZero(t *testing.T) {
	// If ctx is canceled before Tick starts dispatching, the entire
	// drained batch must be re-queued. Drain pops ALL due matches at
	// once, so a naive "re-add the current Match only" would silently
	// lose every Match past the cancellation point. Three matches
	// here exercises that the loop re-queues the current AND every
	// remaining drained entry.
	calls := 0
	client := &fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			calls++
			return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}, nil
		},
	}
	d, sched := newDispatcher(client)
	t0 := time.Now()
	for _, k := range []string{"k1", "k2", "k3"} {
		sched.Add(router.Match{
			Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindExpirationDays},
			ObjectKey: k,
			DueTime:   t0,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // shutdown before Tick even starts dispatching

	processed := d.Tick(ctx, t0.Add(time.Hour))
	assert.Equal(t, 0, processed, "shutdown must report zero processed")
	assert.Equal(t, 0, calls, "shutdown must skip every dispatch call")
	assert.Equal(t, 3, sched.Len(), "every drained Match must be re-queued — none lost")
}
