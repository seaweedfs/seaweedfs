package dailyrun

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readerEventAlias keeps the test free of the reader import collision
// in the recovery test file when we need a typed nil channel.
type readerEventAlias = reader.Event

// TestWalkerDue covers the throttle decision matrix in isolation.
func TestWalkerDue(t *testing.T) {
	runNow := time.Unix(1_700_000_000, 0).UTC()
	cases := []struct {
		name         string
		lastWalkedNs int64
		interval     time.Duration
		want         bool
	}{
		// interval=0 keeps the prior "fire every pass" semantics — the
		// in-repo integration tests and the s3tests fast driver rely on
		// this so the rule-change-then-walk behavior surfaces within a
		// single test runtime. Within-pass double-fire suppression
		// lives in runShard's walkedThisPass local; walkerDue answers
		// the persisted-state question only.
		{"interval zero always due", runNow.UnixNano(), 0, true},
		// LastWalkedNs=0 marks "never walked steady-state" — the post-
		// upgrade cold-start case for cursor files that predate the
		// LastWalkedNs field. Fire so the throttle anchor gets seeded.
		{"never walked is due", 0, time.Hour, true},
		// Throttle on: interval elapsed → due.
		{"interval elapsed", runNow.Add(-2 * time.Hour).UnixNano(), time.Hour, true},
		{"interval exactly elapsed", runNow.Add(-time.Hour).UnixNano(), time.Hour, true},
		// Throttle on: not yet → skip.
		{"interval not yet elapsed", runNow.Add(-30 * time.Minute).UnixNano(), time.Hour, false},
		// Future LastWalkedNs (clock skew or replay of an older runNow
		// against a newer cursor) must not panic and must not fire —
		// treat as "throttled" because the cursor claims a future walk.
		{"future lastWalked", runNow.Add(time.Hour).UnixNano(), time.Hour, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, walkerDue(tc.lastWalkedNs, runNow, tc.interval))
		})
	}
}

// TestRunShard_WalkerThrottle confirms two back-to-back runShard
// invocations only fire the steady-state walker once when the second
// pass starts inside the WalkerInterval window. The interval-zero
// control case keeps firing on every pass — same shape, different
// expectation — pinning the prior behavior so a default-zero config
// (tests, in-repo integration) doesn't regress.
func TestRunShard_WalkerThrottle(t *testing.T) {
	cases := []struct {
		name             string
		interval         time.Duration
		secondPassAfter  time.Duration
		wantTotalCalls   int
		wantSecondAdvNs  bool // did LastWalkedNs change between pass 1 and pass 2?
	}{
		{"interval=0 fires every pass", 0, 30 * time.Second, 2, true},
		{"throttled: second pass within interval", time.Hour, 30 * time.Second, 1, false},
		{"throttled: second pass past interval", time.Hour, 2 * time.Hour, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := snapshotWithRule(t, 30) // replay-eligible only; steady-state walker would normally skip
			// Force a walker-only partition so the steady-state branch
			// has something to walk: set retentionWindow=0 in cfg, which
			// makes promoted non-empty and walkView non-empty.
			p := newMemPersister()
			calls := 0
			cfg := Config{
				Persister: p,
				Walker: func(_ context.Context, _ *engine.Snapshot, _ int) error {
					calls++
					return nil
				},
				RetentionWindow: -1, // negative sentinel: falls back to maxTTL — leaves walkView empty
				WalkerInterval:  tc.interval,
			}
			// Pre-seed cursor matching snap's hashes so cold-start /
			// recovery branches don't fire and we measure ONLY the
			// steady-state walker.
			rsh := engine.ReplayContentHash(snap)
			promoted := engine.PromotedHash(snap, engine.MaxEffectiveTTL(snap))
			runNow := time.Unix(1_700_000_000, 0).UTC()
			require.NoError(t, p.Save(context.Background(), 0, Cursor{
				TsNs:         runNow.UnixNano(),
				RuleSetHash:  rsh,
				PromotedHash: promoted,
			}))

			// With the snapshot's only rule being replay-eligible, the
			// steady-state walkView is empty and the walker won't fire
			// regardless of throttle. Use a snapshot that has BOTH a
			// replay rule AND a scan-only walker rule by forcing
			// retentionWindow to a very small value via cfg.
			cfg.RetentionWindow = time.Nanosecond
			// Recompute promoted with the test retention so persisted
			// hashes match what runShard sees on each pass.
			snapPromoted := engine.PromotedHash(snap, time.Nanosecond)
			require.NoError(t, p.Save(context.Background(), 0, Cursor{
				TsNs:         runNow.UnixNano(),
				RuleSetHash:  rsh,
				PromotedHash: snapPromoted,
			}))

			// runShard reaches drainShardEvents after the steady-state
			// walker fires (replay-eligible rules present). A nil events
			// channel would block forever; a closed one returns
			// immediately so drain exits cleanly and the cursor save
			// still records LastWalkedNs.
			closedEvents := make(chan *readerEventAlias)
			close(closedEvents)

			// Pass 1.
			require.NoError(t, runShard(context.Background(), cfg, snap, runNow, 0, closedEvents))
			afterPass1, _, _ := p.Load(context.Background(), 0)
			require.Equal(t, 1, calls, "pass 1 must fire the walker (cold-start anchor)")
			require.Equal(t, runNow.UnixNano(), afterPass1.LastWalkedNs)

			// Pass 2.
			runNow2 := runNow.Add(tc.secondPassAfter)
			closedEvents2 := make(chan *readerEventAlias)
			close(closedEvents2)
			require.NoError(t, runShard(context.Background(), cfg, snap, runNow2, 0, closedEvents2))
			afterPass2, _, _ := p.Load(context.Background(), 0)
			assert.Equal(t, tc.wantTotalCalls, calls, "walker calls after 2 passes")
			if tc.wantSecondAdvNs {
				assert.Equal(t, runNow2.UnixNano(), afterPass2.LastWalkedNs, "throttle allowed the second walk")
			} else {
				assert.Equal(t, runNow.UnixNano(), afterPass2.LastWalkedNs, "throttle suppressed the second walk; anchor unchanged")
			}
		})
	}
}

// TestValidate_RejectsNegativeWalkerInterval pins the loud-failure
// contract for an embedder or test that constructs dailyrun.Config
// directly with a negative WalkerInterval. The admin-config parser
// clamps negative input to zero (worker/tasks/s3_lifecycle/config.go),
// but a caller bypassing that parser would otherwise get the silent
// fall-through-to-walk-every-pass behavior the throttle is trying to
// prevent.
func TestValidate_RejectsNegativeWalkerInterval(t *testing.T) {
	// validate only checks for nil fields, not behavior — type-assert
	// each interface to its zero value via a stub. The stubs never run.
	cfg := validatableConfig()
	cfg.WalkerInterval = -time.Hour
	err := validate(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WalkerInterval")
	// And the zero sentinel still passes.
	cfg.WalkerInterval = 0
	require.NoError(t, validate(cfg))
}

// validatableConfig builds a minimal dailyrun.Config that passes the
// nil checks in validate() so individual fields can be poked. The
// stubs intentionally don't implement any meaningful behavior — they
// only need to be non-nil interface values.
func validatableConfig() Config {
	return Config{
		Engine:      engine.New(),
		FilerClient: stubFilerClient{},
		Client:      stubLifecycleClient{},
		Persister:   newMemPersister(),
		Lister:      stubSiblingLister{},
		BucketsPath: "/buckets",
	}
}

type stubFilerClient struct{ filer_pb.SeaweedFilerClient }
type stubLifecycleClient struct{}

func (stubLifecycleClient) LifecycleDelete(_ context.Context, _ *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	return nil, nil
}

type stubSiblingLister struct{}

func (stubSiblingLister) Survivors(_ context.Context, _, _ string) (router.Survivors, error) {
	return router.Survivors{}, nil
}
func (stubSiblingLister) LookupVersion(_ context.Context, _, _, _ string) (*filer_pb.Entry, error) {
	return nil, nil
}
func (stubSiblingLister) ListVersions(_ context.Context, _, _ string) ([]*filer_pb.Entry, error) {
	return nil, nil
}
func (stubSiblingLister) LookupNullVersion(_ context.Context, _, _ string) (*filer_pb.Entry, bool, error) {
	return nil, false, nil
}

// TestRunShard_ColdStartDoesNotDoubleWalk pins the within-pass guard
// in runShard's walkedThisPass local. Before the guard, a cold-start
// pass with WalkerInterval=0 fired the walker twice in one pass:
// once via the mustWalkColdStart branch (with RecoveryView) and again
// immediately via the steady-state branch (with the per-shard walk
// view, which is a subset of RecoveryView). The second walk added no
// coverage and burned a full bucket scan. Guard against regression.
func TestRunShard_ColdStartDoesNotDoubleWalk(t *testing.T) {
	snap := snapshotWithRule(t, 30)
	p := newMemPersister()
	// No persisted cursor → mustWalkColdStart=true. WalkerInterval=0
	// would, before the fix, allow the steady-state branch to fire
	// again with no time elapsed.
	calls := 0
	cfg := Config{
		Persister: p,
		Walker: func(_ context.Context, _ *engine.Snapshot, _ int) error {
			calls++
			return nil
		},
		WalkerInterval: 0,
	}
	runNow := time.Unix(1_700_000_000, 0).UTC()

	closedEvents := make(chan *readerEventAlias)
	close(closedEvents)
	require.NoError(t, runShard(context.Background(), cfg, snap, runNow, 0, closedEvents))

	assert.Equal(t, 1, calls, "cold-start walker must fire exactly once per pass even with interval=0")
}

// TestRunShard_RecoveryWalkerSetsLastWalkedAnchor pins that the
// unconditional recovery-branch walker fire still updates the
// LastWalkedNs anchor so the NEXT pass's throttle starts counting
// from this walk rather than re-firing immediately.
func TestRunShard_RecoveryWalkerSetsLastWalkedAnchor(t *testing.T) {
	snap := snapshotWithRule(t, 30)
	p := newMemPersister()
	var stale [32]byte
	for i := range stale {
		stale[i] = 0xAA
	}
	require.NoError(t, p.Save(context.Background(), 5, Cursor{
		TsNs:         1234,
		RuleSetHash:  stale,
		LastWalkedNs: 0, // never walked anchor
	}))

	calls := 0
	cfg := Config{
		Persister: p,
		Walker: func(_ context.Context, _ *engine.Snapshot, _ int) error {
			calls++
			return nil
		},
		WalkerInterval: time.Hour,
	}
	runNow := time.Unix(1_700_000_000, 0).UTC()
	require.NoError(t, runShard(context.Background(), cfg, snap, runNow, 5, nil))

	require.Equal(t, 1, calls, "recovery branch fires the walker unconditionally")
	got, ok, _ := p.Load(context.Background(), 5)
	require.True(t, ok)
	assert.Equal(t, runNow.UnixNano(), got.LastWalkedNs,
		"recovery walk must update LastWalkedNs so the steady-state pass after it can throttle")
}

// Compile-time check that the documented sentinel passes the type system.
var _ = s3lifecycle.ShardCount
