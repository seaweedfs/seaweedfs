package dispatcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
)

// Layer 2 component tests for the dispatcher pipeline mechanics:
// the per-shard composition that Pipeline.Run wires up at runtime
// (cursors, schedules, dispatchers, persister), exercised directly so
// the tests stay independent of a live filer connection. Pipeline.Run
// itself can't run here because it builds a real reader.Reader; these
// tests instead drive Tick directly while sharing the same fakeClient
// that dispatcher_test.go uses.

// shardKit bundles the per-shard state Pipeline.Run constructs in
// production: a Cursor, a Schedule, and a Dispatcher hooked to both.
type shardKit struct {
	id     int
	cursor *reader.Cursor
	dispatch *Dispatcher
}

func newShardKit(t *testing.T, id int, client LifecycleClient) *shardKit {
	t.Helper()
	c := reader.NewCursor()
	return &shardKit{
		id:     id,
		cursor: c,
		dispatch: &Dispatcher{
			ShardID:      id,
			Client:       client,
			Cursor:       c,
			Schedule:     router.NewSchedule(),
			RetryBudget:  3,
			RetryBackoff: time.Millisecond,
		},
	}
}

// recordingClient lets Layer 2 tests assert exactly which RPCs the
// dispatcher made. Goroutine-safe because Tick is called from test
// goroutines but inspections happen from the test goroutine.
type recordingClient struct {
	mu       sync.Mutex
	requests []*s3_lifecycle_pb.LifecycleDeleteRequest
	respond  func(req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error)
}

func (r *recordingClient) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	r.mu.Lock()
	r.requests = append(r.requests, req)
	r.mu.Unlock()
	return r.respond(req)
}

func (r *recordingClient) snapshot() []*s3_lifecycle_pb.LifecycleDeleteRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*s3_lifecycle_pb.LifecycleDeleteRequest, len(r.requests))
	copy(out, r.requests)
	return out
}

func TestPipelineMultiShardFanOutKeepsCursorsIsolated(t *testing.T) {
	// Two events for two different shards land in different schedules
	// and dispatch independently. Each shard's cursor advances only
	// for the event(s) targeting that shard.
	client := &recordingClient{
		respond: func(*s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}, nil
		},
	}
	shards := map[int]*shardKit{
		0: newShardKit(t, 0, client),
		1: newShardKit(t, 1, client),
	}

	t0 := time.Now()
	mA := router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindExpirationDays},
		EventTs:   t0,
		DueTime:   t0,
		Bucket:    "bk",
		ObjectKey: "alpha",
	}
	// Use a different action kind on shard 1 so the cursor key differs
	// from shard 0 — shared keys would write to the same cursor entry.
	mB := router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindNoncurrentDays},
		EventTs:   t0.Add(time.Second),
		DueTime:   t0.Add(time.Second),
		Bucket:    "bk",
		ObjectKey: "beta",
	}
	shards[0].dispatch.Schedule.Add(mA)
	shards[1].dispatch.Schedule.Add(mB)

	// Tick shard 0 first; shard 1's schedule must still hold its match.
	if got := shards[0].dispatch.Tick(context.Background(), t0.Add(time.Hour)); got != 1 {
		t.Fatalf("shard 0 Tick processed=%d, want 1", got)
	}
	if got := shards[1].dispatch.Schedule.Len(); got != 1 {
		t.Fatalf("shard 1 schedule must not be drained by shard 0 Tick: len=%d", got)
	}

	// Now tick shard 1.
	if got := shards[1].dispatch.Tick(context.Background(), t0.Add(time.Hour)); got != 1 {
		t.Fatalf("shard 1 Tick processed=%d, want 1", got)
	}

	if shards[0].cursor.Get(mA.Key) != mA.EventTs.UnixNano() {
		t.Fatalf("shard 0 cursor not advanced: got %d", shards[0].cursor.Get(mA.Key))
	}
	if shards[1].cursor.Get(mB.Key) != mB.EventTs.UnixNano() {
		t.Fatalf("shard 1 cursor not advanced: got %d", shards[1].cursor.Get(mB.Key))
	}
	// Cursors must NOT cross-contaminate.
	if shards[0].cursor.Get(mB.Key) != 0 {
		t.Fatalf("shard 0 cursor saw shard 1's key: got %d", shards[0].cursor.Get(mB.Key))
	}
}

func TestPipelineCursorFreezeOnOneShardDoesntBlockOthers(t *testing.T) {
	// Shard 0 dispatches a poison event that returns BLOCKED — its
	// cursor freezes at the event TsNs. Shard 1's progress must be
	// independent: it dispatches successfully and advances.
	client := &recordingClient{
		respond: func(req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			if req.Bucket == "bk" && req.ObjectPath == "poison" {
				return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED}, nil
			}
			return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}, nil
		},
	}
	shards := map[int]*shardKit{
		0: newShardKit(t, 0, client),
		1: newShardKit(t, 1, client),
	}

	t0 := time.Now()
	poison := router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindExpirationDays},
		EventTs:   t0,
		DueTime:   t0,
		Bucket:    "bk",
		ObjectKey: "poison",
	}
	good := router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindNoncurrentDays},
		EventTs:   t0.Add(time.Second),
		DueTime:   t0.Add(time.Second),
		Bucket:    "bk",
		ObjectKey: "good",
	}
	shards[0].dispatch.Schedule.Add(poison)
	shards[1].dispatch.Schedule.Add(good)

	shards[0].dispatch.Tick(context.Background(), t0.Add(time.Hour))
	shards[1].dispatch.Tick(context.Background(), t0.Add(time.Hour))

	if !shards[0].cursor.IsFrozen(poison.Key) {
		t.Fatalf("shard 0 cursor must freeze on BLOCKED outcome")
	}
	if shards[1].cursor.IsFrozen(good.Key) {
		t.Fatalf("shard 1 cursor must not freeze when shard 0 fails")
	}
	if shards[1].cursor.Get(good.Key) != good.EventTs.UnixNano() {
		t.Fatalf("shard 1 cursor must advance independently: got %d", shards[1].cursor.Get(good.Key))
	}
}

func TestPipelinePersisterCheckpointRoundTripsEveryShard(t *testing.T) {
	// A cursor checkpoint persists the per-shard map; a fresh dispatcher
	// can Restore from that map and resume at the same TsNs. Pipeline.Run
	// drives this on a ticker; tests exercise the contract directly.
	client := &recordingClient{
		respond: func(*s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}, nil
		},
	}
	shards := map[int]*shardKit{
		0: newShardKit(t, 0, client),
		1: newShardKit(t, 1, client),
	}

	t0 := time.Now()
	addAndTick := func(s *shardKit, key, obj string, kind s3lifecycle.ActionKind, ts time.Time) {
		s.dispatch.Schedule.Add(router.Match{
			Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: kind},
			EventTs:   ts,
			DueTime:   ts,
			Bucket:    "bk",
			ObjectKey: obj,
		})
		s.dispatch.Tick(context.Background(), t0.Add(time.Hour))
	}
	addAndTick(shards[0], "k0", "alpha", s3lifecycle.ActionKindExpirationDays, t0)
	addAndTick(shards[1], "k1", "beta", s3lifecycle.ActionKindNoncurrentDays, t0.Add(time.Second))

	pers := reader.NewInMemoryPersister()
	for id, s := range shards {
		if err := pers.Save(context.Background(), id, s.cursor.Snapshot()); err != nil {
			t.Fatalf("save shard %d: %v", id, err)
		}
	}

	// Restore into fresh cursors, verify every shard's TsNs round-trips.
	for id, s := range shards {
		state, err := pers.Load(context.Background(), id)
		if err != nil {
			t.Fatalf("load shard %d: %v", id, err)
		}
		c := reader.NewCursor()
		c.Restore(state)
		want := s.cursor.Snapshot()
		got := c.Snapshot()
		if len(got) != len(want) {
			t.Fatalf("shard %d snapshot len: got %d, want %d", id, len(got), len(want))
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("shard %d cursor key %v: got %d, want %d", id, k, got[k], v)
			}
		}
	}
}

func TestPipelineRetryLaterRespectsDispatchTickCadence(t *testing.T) {
	// RETRY_LATER schedules the match for a later DueTime. A Tick at
	// the present moment must NOT re-dispatch it; only a Tick past
	// the new DueTime triggers a second RPC. This pins the cadence
	// contract against premature retries from later refresh ticks.
	calls := 0
	client := &recordingClient{
		respond: func(*s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
			calls++
			if calls == 1 {
				return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_RETRY_LATER}, nil
			}
			return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}, nil
		},
	}
	s := newShardKit(t, 0, client)
	s.dispatch.RetryBackoff = time.Hour // long backoff so the second Tick at t0+1m doesn't fire

	t0 := time.Now()
	m := router.Match{
		Key:       s3lifecycle.ActionKey{Bucket: "bk", ActionKind: s3lifecycle.ActionKindExpirationDays},
		EventTs:   t0,
		DueTime:   t0,
		Bucket:    "bk",
		ObjectKey: "obj",
	}
	s.dispatch.Schedule.Add(m)

	if got := s.dispatch.Tick(context.Background(), t0); got != 1 {
		t.Fatalf("first Tick processed=%d, want 1 (RETRY_LATER counts as processed)", got)
	}

	// Within the backoff window: nothing fires.
	if got := s.dispatch.Tick(context.Background(), t0.Add(time.Minute)); got != 0 {
		t.Fatalf("Tick within backoff must skip: processed=%d", got)
	}
	if calls != 1 {
		t.Fatalf("RPC must not retry within backoff: calls=%d", calls)
	}

	// Past backoff: re-dispatches.
	if got := s.dispatch.Tick(context.Background(), t0.Add(2*time.Hour)); got != 1 {
		t.Fatalf("Tick past backoff must re-dispatch: processed=%d", got)
	}
	if calls != 2 {
		t.Fatalf("expected exactly 2 calls (initial + one retry), got %d", calls)
	}
}
