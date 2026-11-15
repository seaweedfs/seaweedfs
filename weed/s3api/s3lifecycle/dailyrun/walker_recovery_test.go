package dailyrun

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memPersister is a minimal in-memory CursorPersister. Phase 4b tests
// drive runShard directly; the recovery-branch path returns before
// drainShardEvents so the heavier filer/client/lister fakes aren't
// needed here. Locked because Run fans out a goroutine per shard and
// they all load and save through the same persister.
type memPersister struct {
	mu    sync.Mutex
	store map[int]Cursor
}

func newMemPersister() *memPersister { return &memPersister{store: map[int]Cursor{}} }

func (p *memPersister) Load(_ context.Context, shardID int) (Cursor, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.store[shardID]
	return c, ok, nil
}

func (p *memPersister) Save(_ context.Context, shardID int, c Cursor) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store[shardID] = c
	return nil
}

func snapshotWithRule(t *testing.T, days int) *engine.Snapshot {
	t.Helper()
	e := engine.New()
	e.Compile([]engine.CompileInput{
		{Bucket: "b1", Rules: []*s3lifecycle.Rule{
			{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: days},
		}},
	}, engine.CompileOptions{})
	snap := e.Snapshot()
	for _, a := range snap.AllActions() {
		snap.MarkActive(a.Key)
	}
	return snap
}

func TestRunShard_WalkerInvokedOnRecoveryBranch(t *testing.T) {
	snap := snapshotWithRule(t, 30)
	p := newMemPersister()
	// Seed a persisted cursor whose RuleSetHash differs from snap's, so
	// the rule-change branch fires.
	var stale [32]byte
	for i := range stale {
		stale[i] = 0xAA
	}
	require.NoError(t, p.Save(context.Background(), 3, Cursor{TsNs: 1234, RuleSetHash: stale}))

	var gotView *engine.Snapshot
	var gotShards []int
	calls := 0
	cfg := Config{
		Persister: p,
		Walker: func(_ context.Context, view *engine.Snapshot, shards []int) (time.Time, error) {
			calls++
			gotView = view
			gotShards = shards
			return time.Time{}, nil
		},
	}
	runNow := time.Unix(1_700_000_000, 0).UTC()
	require.NoError(t, runShard(context.Background(), cfg, snap, runNow, 3, nil, nil))

	assert.Equal(t, 1, calls, "walker must fire exactly once on recovery")
	require.NotNil(t, gotView, "walker received the RecoveryView")
	assert.Equal(t, []int{3}, gotShards)

	// Cursor rewound to runNow - maxTTL with the new hashes persisted.
	got, ok, err := p.Load(context.Background(), 3)
	require.NoError(t, err)
	require.True(t, ok)
	expectedFloor := runNow.Add(-engine.MaxEffectiveTTL(snap)).UnixNano()
	assert.Equal(t, expectedFloor, got.TsNs, "cursor must rewind to runNow - maxTTL after recovery walk")
	assert.NotEqual(t, stale, got.RuleSetHash, "stale hash must be replaced")
}

func TestRunShard_RecoveryReplaysFromResumedWalkStart(t *testing.T) {
	snap := snapshotWithRule(t, 30)
	p := newMemPersister()
	var stale [32]byte
	for i := range stale {
		stale[i] = 0xAD
	}
	require.NoError(t, p.Save(context.Background(), 3, Cursor{TsNs: 1234, RuleSetHash: stale}))

	runNow := time.Unix(1_700_000_000, 0).UTC()
	walkedFrom := runNow.Add(-48 * time.Hour)
	cfg := Config{
		Persister: p,
		Walker: func(context.Context, *engine.Snapshot, []int) (time.Time, error) {
			return walkedFrom, nil
		},
	}
	require.NoError(t, runShard(context.Background(), cfg, snap, runNow, 3, nil, nil))

	got, ok, err := p.Load(context.Background(), 3)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, walkedFrom.Add(-engine.MaxEffectiveTTL(snap)).UnixNano(), got.TsNs,
		"objects in buckets a resumed walk listed earlier must stay in the replay window")
	assert.Equal(t, runNow.UnixNano(), got.LastWalkedNs)
}

func TestRunShard_ColdStartWithResumedWalkReplaysNextPass(t *testing.T) {
	snap := snapshotWithRule(t, 30)
	p := newMemPersister()
	runNow := time.Unix(1_700_000_000, 0).UTC()
	walkedFrom := runNow.Add(-48 * time.Hour)
	cfg := Config{
		Persister: p,
		Walker: func(context.Context, *engine.Snapshot, []int) (time.Time, error) {
			return walkedFrom, nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, runShard(ctx, cfg, snap, runNow, 3, nil, nil))

	got, ok, err := p.Load(context.Background(), 3)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, walkedFrom.Add(-engine.MaxEffectiveTTL(snap)).UnixNano(), got.TsNs,
		"this pass's subscription began at runNow - maxTTL, so the next pass replays from the walk start")
	assert.Equal(t, engine.ReplayContentHash(snap), got.RuleSetHash)
}

func TestComputeGlobalStartTsNs_SavedCursorBelowTheFloor(t *testing.T) {
	runNow := time.Unix(1_700_000_000, 0).UTC()
	maxTTL := 30 * 24 * time.Hour
	floor := runNow.Add(-maxTTL).UnixNano()
	saved := runNow.Add(-maxTTL - 48*time.Hour).UnixNano()
	cfg := Config{Shards: []int{1, 3}}
	cursors := map[int]cursorLoad{3: {cursor: Cursor{TsNs: saved}, found: true}}
	assert.Equal(t, saved, computeGlobalStartTsNs(cursors, cfg, runNow, maxTTL),
		"a cursor a resumed cold start saved below runNow - maxTTL opens the next subscription")
	assert.Equal(t, floor, computeGlobalStartTsNs(map[int]cursorLoad{}, cfg, runNow, maxTTL))
}

func TestRunShard_NilWalkerOnRecoveryIsNoop(t *testing.T) {
	// Phase 4a behavior: a nil Walker must not crash and must still
	// rewind the cursor.
	snap := snapshotWithRule(t, 30)
	p := newMemPersister()
	var stale [32]byte
	for i := range stale {
		stale[i] = 0xBB
	}
	require.NoError(t, p.Save(context.Background(), 0, Cursor{TsNs: 9999, RuleSetHash: stale}))

	cfg := Config{Persister: p} // Walker nil
	runNow := time.Unix(1_700_000_000, 0).UTC()
	require.NoError(t, runShard(context.Background(), cfg, snap, runNow, 0, nil, nil))

	got, ok, err := p.Load(context.Background(), 0)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, runNow.Add(-engine.MaxEffectiveTTL(snap)).UnixNano(), got.TsNs)
}

func TestRunShard_WalkerErrorPropagates(t *testing.T) {
	// A walker failure must surface as the runShard error so the daily
	// run treats the shard as interrupted, and must NOT advance the
	// cursor (the seeded cursor stays).
	snap := snapshotWithRule(t, 30)
	p := newMemPersister()
	var stale [32]byte
	for i := range stale {
		stale[i] = 0xCC
	}
	require.NoError(t, p.Save(context.Background(), 7, Cursor{TsNs: 42, RuleSetHash: stale}))

	cfg := Config{
		Persister: p,
		Walker: func(_ context.Context, _ *engine.Snapshot, _ []int) (time.Time, error) {
			return time.Time{}, errors.New("walker boom")
		},
	}
	runNow := time.Unix(1_700_000_000, 0).UTC()
	err := runShard(context.Background(), cfg, snap, runNow, 7, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "walker boom")

	// Cursor untouched.
	got, _, _ := p.Load(context.Background(), 7)
	assert.Equal(t, int64(42), got.TsNs, "walker failure must leave cursor unchanged")
	assert.Equal(t, stale, got.RuleSetHash)
}

// The "matching cursor doesn't invoke walker" case is implicit: the
// walker call is inside the rule-change / partition-flip `if` and
// can't be reached otherwise. Exercising it end-to-end requires the
// full filer + lister + client harness; covered by the integration
// tests once Phase 4b is wired into the handler.
