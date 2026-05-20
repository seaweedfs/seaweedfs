package engine

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildSnapshotForViews compiles a snapshot with a single bucket containing
// one rule per supplied factory, marking every action bootstrap_complete so
// the base actions are active. Tests can then call RulesForShard and assert
// the per-view clones come out independent.
func buildSnapshotForViews(t *testing.T, bucket string, rules ...*s3lifecycle.Rule) *Snapshot {
	t.Helper()
	prior := map[s3lifecycle.ActionKey]PriorState{}
	for _, r := range rules {
		rh := s3lifecycle.RuleHash(r)
		for _, k := range s3lifecycle.RuleActionKinds(r) {
			prior[s3lifecycle.ActionKey{Bucket: bucket, RuleHash: rh, ActionKind: k}] = PriorState{
				BootstrapComplete: true,
			}
		}
	}
	return New().Compile(
		[]CompileInput{{Bucket: bucket, Rules: rules}},
		CompileOptions{PriorStates: prior},
	)
}

func TestCurrentSnapshot_NilWhenNoEngineRegistered(t *testing.T) {
	// Save and restore the package-level pointer so tests stay isolated.
	prev := currentEngine.Load()
	t.Cleanup(func() { currentEngine.Store(prev) })

	SetCurrentEngine(nil)
	assert.Nil(t, CurrentSnapshot())
}

func TestCurrentSnapshot_ReturnsRegisteredEngineSnapshot(t *testing.T) {
	prev := currentEngine.Load()
	t.Cleanup(func() { currentEngine.Store(prev) })

	e := New()
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7}
	snap := e.Compile([]CompileInput{{Bucket: "b", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{})
	SetCurrentEngine(e)
	assert.Same(t, snap, CurrentSnapshot())
}

func TestRulesForShard_NilSnapshot(t *testing.T) {
	var s *Snapshot
	replay, walk := s.RulesForShard(0, 30*24*time.Hour)
	assert.Nil(t, replay)
	assert.Nil(t, walk)
}

func TestRulesForShard_ReplayPartitionMembership(t *testing.T) {
	// ExpirationDays / NoncurrentDays / AbortMPU below retentionWindow all
	// land in replay; the replay map carries clones with active=true and
	// Mode=ModeEventDriven.
	rule := &s3lifecycle.Rule{
		ID:                              "all-replay",
		Status:                          s3lifecycle.StatusEnabled,
		ExpirationDays:                  3,
		NoncurrentVersionExpirationDays: 5,
		AbortMPUDaysAfterInitiation:     2,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	replay, walk := snap.RulesForShard(0, s3lifecycle.DaysToDuration(30))
	require.NotNil(t, replay, "all three replay-eligible kinds should land in replay")
	assert.Nil(t, walk, "no walker kinds, no scan_only promotion, walk should be empty")
	assert.Len(t, replay.actions, 3)
	for k, a := range replay.actions {
		assert.True(t, a.IsActive(), "replay clone must be active for kind %v", k.ActionKind)
		assert.Equal(t, ModeEventDriven, a.Mode, "replay Mode must be ModeEventDriven for kind %v", k.ActionKind)
	}
}

func TestRulesForShard_WalkOnlyKindsRouteToWalk(t *testing.T) {
	// ExpirationDate, ExpiredObjectDeleteMarker, NewerNoncurrent are walker-
	// only and must land in walk regardless of retentionWindow.
	when := time.Now().Add(24 * time.Hour)
	rule := &s3lifecycle.Rule{
		ID:                        "walker-mix",
		Status:                    s3lifecycle.StatusEnabled,
		ExpirationDate:            when,
		ExpiredObjectDeleteMarker: true,
		NewerNoncurrentVersions:   2,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	replay, walk := snap.RulesForShard(0, s3lifecycle.DaysToDuration(30))
	assert.Nil(t, replay, "walker-only rule has no replay partition")
	require.NotNil(t, walk)
	assert.Len(t, walk.actions, 3, "all three walker kinds should land in walk")
	for _, a := range walk.actions {
		assert.True(t, a.IsActive())
	}
}

func TestRulesForShard_MultiActionXMLRuleSplits(t *testing.T) {
	// A single XML rule that compiles to ExpirationDays + NewerNoncurrent
	// must split: ExpirationDays → replay, NewerNoncurrent → walk.
	rule := &s3lifecycle.Rule{
		ID:                      "split",
		Status:                  s3lifecycle.StatusEnabled,
		ExpirationDays:          3,
		NewerNoncurrentVersions: 5,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	replay, walk := snap.RulesForShard(0, s3lifecycle.DaysToDuration(30))
	require.NotNil(t, replay)
	require.NotNil(t, walk)
	assert.Len(t, replay.actions, 1)
	assert.Len(t, walk.actions, 1)
	for k := range replay.actions {
		assert.Equal(t, s3lifecycle.ActionKindExpirationDays, k.ActionKind)
	}
	for k := range walk.actions {
		assert.Equal(t, s3lifecycle.ActionKindNewerNoncurrent, k.ActionKind)
	}
}

func TestRulesForShard_ScanOnlyPromotionRoutesToWalk(t *testing.T) {
	// A replay-eligible kind whose TTL exceeds retentionWindow promotes to
	// walk. The clone's active=true is set; Mode is preserved as-is from
	// the base (the walker only rejects ModeDisabled).
	rule := &s3lifecycle.Rule{
		ID:                          "long",
		Status:                      s3lifecycle.StatusEnabled,
		ExpirationDays:              90,
		AbortMPUDaysAfterInitiation: 1,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	// Retention window of 30 days promotes ExpirationDays(90d) to walk;
	// AbortMPU(1d) stays in replay.
	replay, walk := snap.RulesForShard(0, s3lifecycle.DaysToDuration(30))
	require.NotNil(t, replay)
	require.NotNil(t, walk)
	assert.Len(t, replay.actions, 1)
	assert.Len(t, walk.actions, 1)
	for k, a := range replay.actions {
		assert.Equal(t, s3lifecycle.ActionKindAbortMPU, k.ActionKind)
		assert.Equal(t, ModeEventDriven, a.Mode)
	}
	for k := range walk.actions {
		assert.Equal(t, s3lifecycle.ActionKindExpirationDays, k.ActionKind)
	}
}

func TestRulesForShard_ReplayRewritesScanOnlyMode(t *testing.T) {
	// A base action carrying Mode=ModeScanOnly via PriorState must still
	// land in replay as ModeEventDriven once retention rehabilitates its
	// TTL. router.Route gates on ModeEventDriven, so this is the
	// rehabilitation contract.
	rule := &s3lifecycle.Rule{
		ID:             "rehab",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 5,
	}
	rh := s3lifecycle.RuleHash(rule)
	key := s3lifecycle.ActionKey{Bucket: "b1", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}
	snap := New().Compile(
		[]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{
			PriorStates: map[s3lifecycle.ActionKey]PriorState{
				// Persistent ModeScanOnly carried over from a prior compile.
				key: {BootstrapComplete: true, Mode: ModeScanOnly},
			},
		},
	)
	// Verify the base really did keep ModeScanOnly (Compile preserves it).
	require.Equal(t, ModeScanOnly, snap.actions[key].Mode)

	replay, _ := snap.RulesForShard(0, s3lifecycle.DaysToDuration(30))
	require.NotNil(t, replay)
	clone := replay.actions[key]
	require.NotNil(t, clone)
	assert.Equal(t, ModeEventDriven, clone.Mode, "replay must force ModeEventDriven on the clone")
	assert.True(t, clone.IsActive())
}

func TestRulesForShard_DisabledRuleExcludedFromBothViews(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "off", Status: s3lifecycle.StatusDisabled, ExpirationDays: 7}
	snap := buildSnapshotForViews(t, "b1", rule)
	replay, walk := snap.RulesForShard(0, s3lifecycle.DaysToDuration(30))
	assert.Nil(t, replay)
	assert.Nil(t, walk)
}

func TestRulesForShard_ClonesAreIndependentOfBase(t *testing.T) {
	// Toggling active on a view clone must never propagate to the base
	// action — that's the whole point of cloning per view.
	rule := &s3lifecycle.Rule{ID: "iso", Status: s3lifecycle.StatusEnabled, ExpirationDays: 3}
	snap := buildSnapshotForViews(t, "b1", rule)
	key := s3lifecycle.ActionKey{
		Bucket:     "b1",
		RuleHash:   s3lifecycle.RuleHash(rule),
		ActionKind: s3lifecycle.ActionKindExpirationDays,
	}
	base := snap.Action(key)
	require.NotNil(t, base)
	baseActiveBefore := base.IsActive()

	replay, _ := snap.RulesForShard(0, s3lifecycle.DaysToDuration(30))
	require.NotNil(t, replay)
	clone := replay.actions[key]
	require.NotNil(t, clone)
	assert.NotSame(t, base, clone, "clone must be a distinct *CompiledAction")

	// Force the clone inactive (write directly to its atomic so we exercise
	// the active bit boundary, not just markActive's setter).
	clone.engineState.Store(engineStateInactive)
	assert.False(t, clone.IsActive())
	assert.Equal(t, baseActiveBefore, base.IsActive(),
		"toggling the clone must not perturb the base's active bit")
}

func TestRulesForShard_ZeroRetentionRoutesAllReplayKindsToWalk(t *testing.T) {
	// retentionWindow == 0 (no proven retention) must route every replay-
	// eligible action into walk — the partition is "ttl > 0 && ttl <=
	// retentionWindow", and 0 fails the upper bound. This is the safe
	// default before the meta-log horizon is known.
	rule := &s3lifecycle.Rule{
		ID:             "any",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 1,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	replay, walk := snap.RulesForShard(0, 0)
	assert.Nil(t, replay)
	require.NotNil(t, walk)
	assert.Len(t, walk.actions, 1)
}

func TestRecoveryView_ActivatesEveryNonDisabledAction(t *testing.T) {
	// Base snapshot has no PriorState → event-driven actions land inactive
	// (pre-BootstrapComplete). Recovery view must clone them with active=
	// true so the recovery walker emits matches for them.
	rule := &s3lifecycle.Rule{
		ID:             "pending",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 7,
	}
	snap := New().Compile(
		[]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{}, // no PriorStates → action lands inactive
	)
	key := s3lifecycle.ActionKey{
		Bucket:     "b1",
		RuleHash:   s3lifecycle.RuleHash(rule),
		ActionKind: s3lifecycle.ActionKindExpirationDays,
	}
	require.False(t, snap.actions[key].IsActive(), "precondition: base action must be inactive")

	rv := RecoveryView(snap)
	require.NotNil(t, rv)
	clone := rv.actions[key]
	require.NotNil(t, clone)
	assert.True(t, clone.IsActive(), "recovery view must force-activate even pre-BootstrapComplete actions")
	assert.Equal(t, ModeEventDriven, clone.Mode, "recovery preserves base Mode")
}

func TestRecoveryView_PreservesBaseModeForScanAtDate(t *testing.T) {
	// ScanAtDate actions are active at compile time (no BootstrapComplete
	// requirement). Recovery view must keep their Mode as ModeScanAtDate so
	// the walker's date evaluator runs as expected — a Mode rewrite here
	// would re-route them through the event-driven gate by mistake.
	when := time.Now().Add(48 * time.Hour)
	rule := &s3lifecycle.Rule{
		ID:             "d",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDate: when,
	}
	snap := New().Compile(
		[]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{},
	)
	rv := RecoveryView(snap)
	require.NotNil(t, rv)
	require.Len(t, rv.actions, 1)
	for _, a := range rv.actions {
		assert.Equal(t, ModeScanAtDate, a.Mode)
		assert.True(t, a.IsActive())
	}
}

func TestRecoveryView_ExcludesDisabledRules(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "off",
		Status:         s3lifecycle.StatusDisabled,
		ExpirationDays: 7,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	rv := RecoveryView(snap)
	assert.Nil(t, rv, "disabled rules must not surface in recovery view")
}

func TestRecoveryView_ClonesAreIndependentOfBase(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "iso", Status: s3lifecycle.StatusEnabled, ExpirationDays: 3}
	snap := buildSnapshotForViews(t, "b1", rule)
	key := s3lifecycle.ActionKey{
		Bucket:     "b1",
		RuleHash:   s3lifecycle.RuleHash(rule),
		ActionKind: s3lifecycle.ActionKindExpirationDays,
	}
	base := snap.Action(key)
	baseActiveBefore := base.IsActive()

	rv := RecoveryView(snap)
	require.NotNil(t, rv)
	clone := rv.actions[key]
	require.NotNil(t, clone)
	assert.NotSame(t, base, clone)

	clone.engineState.Store(engineStateInactive)
	assert.Equal(t, baseActiveBefore, base.IsActive(),
		"flipping the recovery clone must not perturb the base")
}

func TestRecoveryView_NilSnapshot(t *testing.T) {
	assert.Nil(t, RecoveryView(nil))
}
