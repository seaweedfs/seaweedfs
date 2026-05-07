package dispatcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
)

type fakeClient struct {
	calls    int
	respond  func(call int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error)
}

func (f *fakeClient) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	f.calls++
	return f.respond(f.calls)
}

func mkMatch(eventTs time.Time, due time.Time, key string) router.Match {
	return router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindExpirationDays},
		EventTs:   eventTs,
		DueTime:   due,
		Bucket:    "bk",
		ObjectKey: key,
	}
}

func newDispatcher(client LifecycleClient) (*Dispatcher, *router.Schedule) {
	sched := router.NewSchedule()
	d := &Dispatcher{
		ShardID:      0,
		Client:       client,
		Cursor:       reader.NewCursor(),
		Blockers:     NewInMemoryBlockerStore(),
		Schedule:     sched,
		RetryBudget:  3,
		RetryBackoff: time.Millisecond,
	}
	return d, sched
}

func TestDispatchDoneAdvancesCursor(t *testing.T) {
	client := &fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return &s3_lifecycle_pb.LifecycleDeleteResponse{
				Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
			}, nil
		},
	}
	d, sched := newDispatcher(client)
	t0 := time.Now()
	m := mkMatch(t0, t0, "obj")
	sched.Add(m)

	got := d.Tick(context.Background(), t0)
	if got != 1 {
		t.Fatalf("Tick processed=%d, want 1", got)
	}
	if d.Cursor.Get(m.Key) != t0.UnixNano() {
		t.Fatalf("cursor not advanced: %d", d.Cursor.Get(m.Key))
	}
	if d.Cursor.IsFrozen(m.Key) {
		t.Fatal("cursor should not be frozen")
	}
}

func TestDispatchNoopAdvancesCursor(t *testing.T) {
	client := &fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return &s3_lifecycle_pb.LifecycleDeleteResponse{
				Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
				Reason:  "STALE_IDENTITY",
			}, nil
		},
	}
	d, sched := newDispatcher(client)
	t0 := time.Now()
	m := mkMatch(t0, t0, "obj")
	sched.Add(m)
	d.Tick(context.Background(), t0)
	if d.Cursor.Get(m.Key) != t0.UnixNano() {
		t.Fatal("NOOP_RESOLVED should advance cursor")
	}
}

func TestDispatchRetryLaterReSchedules(t *testing.T) {
	client := &fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return &s3_lifecycle_pb.LifecycleDeleteResponse{
				Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
				Reason:  "TRANSPORT_ERROR",
			}, nil
		},
	}
	d, sched := newDispatcher(client)
	t0 := time.Now()
	m := mkMatch(t0, t0, "obj")
	sched.Add(m)

	// First tick: dispatched, retry-budget = 1, re-scheduled.
	d.Tick(context.Background(), t0)
	if sched.Len() != 1 {
		t.Fatalf("expected re-schedule on RETRY_LATER, sched.Len=%d", sched.Len())
	}
	if d.Cursor.Get(m.Key) != 0 {
		t.Fatal("cursor must not advance on RETRY_LATER")
	}
	if d.Cursor.IsFrozen(m.Key) {
		t.Fatal("cursor must not freeze within budget")
	}
}

func TestDispatchRetryBudgetEscalatesToBlocked(t *testing.T) {
	client := &fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return &s3_lifecycle_pb.LifecycleDeleteResponse{
				Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER,
				Reason:  "stuck",
			}, nil
		},
	}
	d, sched := newDispatcher(client)
	d.RetryBudget = 2
	t0 := time.Now()
	m := mkMatch(t0, t0, "obj")
	sched.Add(m)

	// Tick repeatedly; each pushes the re-scheduled entry forward by backoff,
	// so we advance "now" past each backoff to drain it.
	now := t0
	for i := 0; i < 5 && sched.Len() > 0; i++ {
		now = now.Add(d.RetryBackoff + time.Millisecond)
		d.Tick(context.Background(), now)
	}
	if !d.Cursor.IsFrozen(m.Key) {
		t.Fatal("expected freeze after budget exhausted")
	}
	recs, _ := d.Blockers.List(context.Background(), 0)
	if len(recs) != 1 {
		t.Fatalf("expected 1 blocker record, got %d", len(recs))
	}
	if recs[0].Reason == "" {
		t.Fatal("blocker record missing reason")
	}
}

func TestDispatchBlockedFreezesCursor(t *testing.T) {
	client := &fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return &s3_lifecycle_pb.LifecycleDeleteResponse{
				Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED,
				Reason:  "FATAL_EVENT_ERROR: empty bucket",
			}, nil
		},
	}
	d, sched := newDispatcher(client)
	t0 := time.Now()
	m := mkMatch(t0, t0, "obj")
	sched.Add(m)
	d.Tick(context.Background(), t0)
	if !d.Cursor.IsFrozen(m.Key) {
		t.Fatal("BLOCKED must freeze cursor")
	}
	if d.Cursor.Get(m.Key) != t0.UnixNano() {
		t.Fatal("frozen cursor should be pinned at event ts")
	}
}

func TestDispatchContextCancelDoesNotBurnBudget(t *testing.T) {
	// Worker shutdown causes the in-flight RPC to return context.Canceled.
	// The Match should go back on the schedule untouched; no retry-budget
	// burn means a quick restart can't escalate it to BLOCKED.
	client := &fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return nil, context.Canceled
		},
	}
	d, sched := newDispatcher(client)
	d.RetryBudget = 1
	t0 := time.Now()
	m := mkMatch(t0, t0, "obj")
	sched.Add(m)

	d.Tick(context.Background(), t0)
	if sched.Len() != 1 {
		t.Fatalf("expected re-queue on ctx cancel, sched.Len=%d", sched.Len())
	}
	if d.Cursor.IsFrozen(m.Key) {
		t.Fatal("ctx cancel must not freeze cursor")
	}
	if got := d.retries[keyOf(m)]; got != 0 {
		t.Fatalf("ctx cancel must not burn retry budget, retries=%d", got)
	}
}

func TestDispatchTransportErrorRetries(t *testing.T) {
	// gRPC error: classified as RETRY_LATER. After the budget the cursor freezes.
	client := &fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return nil, errors.New("connection refused")
		},
	}
	d, sched := newDispatcher(client)
	d.RetryBudget = 1
	t0 := time.Now()
	m := mkMatch(t0, t0, "obj")
	sched.Add(m)

	now := t0
	for i := 0; i < 5 && sched.Len() > 0 && !d.Cursor.IsFrozen(m.Key); i++ {
		now = now.Add(d.RetryBackoff + time.Millisecond)
		d.Tick(context.Background(), now)
	}
	if !d.Cursor.IsFrozen(m.Key) {
		t.Fatal("transport-error retries should escalate to BLOCKED past budget")
	}
}

func TestDispatchSkipsFrozenCursor(t *testing.T) {
	client := &fakeClient{
		respond: func(int) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			t.Fatal("frozen cursor should not call RPC")
			return nil, nil
		},
	}
	d, sched := newDispatcher(client)
	t0 := time.Now()
	m := mkMatch(t0, t0, "obj")
	d.Cursor.Freeze(m.Key, t0.UnixNano())
	sched.Add(m)
	d.Tick(context.Background(), t0)
	if client.calls != 0 {
		t.Fatalf("expected 0 RPC calls, got %d", client.calls)
	}
}

func TestReplayBlockersRefreezes(t *testing.T) {
	store := NewInMemoryBlockerStore()
	t0 := time.Now()
	key := s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindExpirationDays}
	store.Put(context.Background(), BlockerRecord{
		ShardID:    0,
		Key:        key,
		FrozenAtNs: t0.UnixNano(),
		Reason:     "prior run",
		CreatedAt:  t0,
	})
	d := &Dispatcher{
		ShardID:  0,
		Cursor:   reader.NewCursor(),
		Blockers: store,
		Schedule: router.NewSchedule(),
	}
	if err := d.ReplayBlockers(context.Background()); err != nil {
		t.Fatalf("ReplayBlockers: %v", err)
	}
	if !d.Cursor.IsFrozen(key) {
		t.Fatal("ReplayBlockers should refreeze cursor")
	}
}
