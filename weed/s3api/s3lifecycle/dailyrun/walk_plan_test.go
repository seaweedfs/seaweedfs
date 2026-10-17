package dailyrun

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Issue #11791: a pass used to invoke the walker once per shard, so the
// whole bucket tree was listed ShardCount times. The walker now gets
// the full shard set in at most two calls per pass — one for the
// recovery group, one for the steady group.
func TestRun_SharesBucketWalkAcrossShards(t *testing.T) {
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled,
				ExpirationDate: time.Now().Add(24 * time.Hour)},
		}},
	}, engine.CompileOptions{})

	shards := make([]int, 0, s3lifecycle.ShardCount)
	for i := 0; i < s3lifecycle.ShardCount; i++ {
		shards = append(shards, i)
	}

	var mu sync.Mutex
	var calls [][]int
	p := newMemPersister()
	cfg := Config{
		Shards:      shards,
		BucketsPath: "/buckets",
		Engine:      e,
		FilerClient: stubFilerClient{},
		Client:      stubLifecycleClient{},
		Persister:   p,
		Lister:      stubSiblingLister{},
		Walker: func(_ context.Context, _ *engine.Snapshot, shards []int) error {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, append([]int{}, shards...))
			return nil
		},
	}
	runNow := time.Unix(1_700_000_000, 0).UTC()
	cfg.Now = func() time.Time { return runNow }
	require.NoError(t, Run(context.Background(), cfg))

	require.LessOrEqual(t, len(calls), 2, "a pass must not list the bucket once per shard")
	covered := map[int]bool{}
	for _, c := range calls {
		for _, sh := range c {
			covered[sh] = true
		}
	}
	for _, sh := range shards {
		assert.True(t, covered[sh], "shard %d missed the shared walk", sh)
		c, found, err := p.Load(context.Background(), sh)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, runNow.UnixNano(), c.LastWalkedNs,
			"shard %d must credit the shared walk to its own cursor", sh)
	}
}

// A recovery group and a steady group in the same pass share one walk
// each: two walker calls total, each covering its own shard set and
// its own view.
func TestRun_RecoveryAndSteadyGroupsWalkSeparately(t *testing.T) {
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled,
				ExpirationDays: 30,
				ExpirationDate: time.Now().Add(24 * time.Hour)},
		}},
	}, engine.CompileOptions{})
	snap := e.Snapshot()
	for _, a := range snap.AllActions() {
		snap.MarkActive(a.Key)
	}

	p := newMemPersister()
	var stale [32]byte
	for i := range stale {
		stale[i] = 0xAA
	}
	// Shard 0's stale rule hash sends it through the recovery walk.
	// Shards 1 and 2 have matching hashes and a zero walk anchor, so
	// the steady-state branch takes them.
	require.NoError(t, p.Save(context.Background(), 0, Cursor{TsNs: 42, RuleSetHash: stale}))
	matching := Cursor{
		TsNs:         1234,
		RuleSetHash:  engine.ReplayContentHash(snap),
		PromotedHash: engine.PromotedHash(snap, engine.MaxEffectiveTTL(snap)),
	}
	for _, sh := range []int{1, 2} {
		require.NoError(t, p.Save(context.Background(), sh, matching))
	}

	var mu sync.Mutex
	var calls [][]int
	var actions []int
	cfg := Config{
		Shards:      []int{0, 1, 2},
		BucketsPath: "/buckets",
		Engine:      e,
		FilerClient: &idleSubscribeClient{},
		Client:      stubLifecycleClient{},
		Persister:   p,
		Lister:      stubSiblingLister{},
		Walker: func(_ context.Context, view *engine.Snapshot, shards []int) error {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, append([]int{}, shards...))
			actions = append(actions, len(view.AllActions()))
			return nil
		},
	}
	require.NoError(t, Run(context.Background(), cfg))

	require.Len(t, calls, 2, "one shared walk per group: recovery + steady")
	for i, c := range calls {
		sort.Ints(c)
		switch {
		case len(c) == 1 && c[0] == 0:
			assert.Equal(t, 2, actions[i], "recovery view carries every non-disabled action")
		case len(c) == 2 && c[0] == 1 && c[1] == 2:
			assert.Equal(t, 1, actions[i], "walk view carries only the walker-bound action")
		default:
			t.Fatalf("unexpected walk group %v", c)
		}
	}
}

// A shared walk error must surface on every shard it covered, not just
// the one that happened to run the listing.
func TestRun_SharedWalkErrorFailsCoveredShards(t *testing.T) {
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled,
				ExpirationDate: time.Now().Add(24 * time.Hour)},
		}},
	}, engine.CompileOptions{})

	cfg := Config{
		Shards:      []int{0, 1},
		BucketsPath: "/buckets",
		Engine:      e,
		FilerClient: stubFilerClient{},
		Client:      stubLifecycleClient{},
		Persister:   newMemPersister(),
		Lister:      stubSiblingLister{},
		Walker: func(_ context.Context, _ *engine.Snapshot, _ []int) error {
			return errors.New("walk boom")
		},
	}
	err := Run(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "walk boom")
}

// countingPersister fails Load after the first failAfter calls: call 1
// feeds planWalks, the failing call 2 is runShard's authoritative read.
type countingPersister struct {
	inner     CursorPersister
	calls     atomic.Int32
	failAfter int32
}

func (p *countingPersister) Load(ctx context.Context, shardID int) (Cursor, bool, error) {
	if p.calls.Add(1) > p.failAfter {
		return Cursor{}, false, errors.New("transient load failure")
	}
	return p.inner.Load(ctx, shardID)
}

func (p *countingPersister) Save(ctx context.Context, shardID int, c Cursor) error {
	return p.inner.Save(ctx, shardID, c)
}

// A shard whose second cursor load fails exits runShard without
// awaiting the planned walk it was grouped into. The walk goroutine is
// already running — Run must still join it, or the handler tears down
// the filer connection while the walk is mid-listing.
func TestRun_JoinsPlannedWalkWhenShardExitsEarly(t *testing.T) {
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled,
				ExpirationDate: time.Now().Add(24 * time.Hour)},
		}},
	}, engine.CompileOptions{})

	walkStarted := make(chan struct{})
	walkRelease := make(chan struct{})
	cfg := Config{
		Shards:      []int{0},
		BucketsPath: "/buckets",
		Engine:      e,
		FilerClient: stubFilerClient{},
		Client:      stubLifecycleClient{},
		Persister:   &countingPersister{inner: newMemPersister(), failAfter: 1},
		Lister:      stubSiblingLister{},
		Walker: func(context.Context, *engine.Snapshot, []int) error {
			close(walkStarted)
			<-walkRelease
			return nil
		},
	}
	runDone := make(chan error, 1)
	go func() { runDone <- Run(context.Background(), cfg) }()

	<-walkStarted
	select {
	case <-runDone:
		t.Fatal("Run returned while the planned walk was still running")
	case <-time.After(200 * time.Millisecond):
	}
	close(walkRelease)
	require.Error(t, <-runDone)
}
