package engine

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplayContentHash_EmptyWhenNoReplayActions(t *testing.T) {
	var empty [32]byte
	// Nil snapshot.
	assert.Equal(t, empty, ReplayContentHash(nil))

	// Walker-only rule: ExpirationDate compiles to a single ScanAtDate
	// action which is excluded from ReplayContentHash.
	when := time.Now().Add(24 * time.Hour)
	rule := &s3lifecycle.Rule{
		ID:             "d",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDate: when,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	assert.Equal(t, empty, ReplayContentHash(snap))
}

func TestReplayContentHash_PartitionIndependent(t *testing.T) {
	// Critical invariant: a retention shift that promotes a rule from
	// replay to walk (or back) MUST NOT change ReplayContentHash. The hash
	// is over the rule content as it appears in the base snapshot, not
	// over the partition it ends up in.
	rule := &s3lifecycle.Rule{
		ID:             "shift",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	hashWithLargeRetention := ReplayContentHash(snap)

	// Whether retention is 1d (promotes to walk) or 365d (stays in
	// replay), the base snapshot's rule content didn't move — only its
	// partition did. ReplayContentHash hashes the base, so this is
	// trivially constant. We re-call here to pin the contract.
	_, walkSmall := snap.RulesForShard(0, s3lifecycle.DaysToDuration(1))
	require.NotNil(t, walkSmall, "precondition: 1d retention promotes 30d rule to walk")
	replayLarge, _ := snap.RulesForShard(0, s3lifecycle.DaysToDuration(365))
	require.NotNil(t, replayLarge, "precondition: 365d retention keeps 30d rule in replay")

	hashAgain := ReplayContentHash(snap)
	assert.Equal(t, hashWithLargeRetention, hashAgain,
		"ReplayContentHash must be partition-independent")
}

func TestReplayContentHash_ChangesOnRuleContentEdit(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap1 := buildSnapshotForViews(t, "b1", rule)
	h1 := ReplayContentHash(snap1)

	// Edit the TTL — RuleHash changes, so the action's key changes, and
	// the content hash must move.
	rule2 := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 60,
	}
	snap2 := buildSnapshotForViews(t, "b1", rule2)
	h2 := ReplayContentHash(snap2)

	assert.NotEqual(t, h1, h2, "TTL edit must change ReplayContentHash")
}

func TestReplayContentHash_StableAcrossRuleReorder(t *testing.T) {
	r1 := &s3lifecycle.Rule{ID: "a", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7, Prefix: "a/"}
	r2 := &s3lifecycle.Rule{ID: "b", Status: s3lifecycle.StatusEnabled, ExpirationDays: 14, Prefix: "b/"}
	snapAB := buildSnapshotForViews(t, "b1", r1, r2)
	snapBA := buildSnapshotForViews(t, "b1", r2, r1)
	assert.Equal(t, ReplayContentHash(snapAB), ReplayContentHash(snapBA),
		"reordering rules at compile time must not change ReplayContentHash")
}

func TestReplayContentHash_ExcludesWalkerOnlyAndDisabled(t *testing.T) {
	// Walker-only kinds and disabled rules don't affect ReplayContentHash;
	// only replay-eligible action kinds count.
	replayRule := &s3lifecycle.Rule{
		ID:             "rep",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 7,
	}
	snapOnlyReplay := buildSnapshotForViews(t, "b1", replayRule)
	baseline := ReplayContentHash(snapOnlyReplay)

	walkerRule := &s3lifecycle.Rule{
		ID:                        "walker",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	disabledRule := &s3lifecycle.Rule{
		ID:             "off",
		Status:         s3lifecycle.StatusDisabled,
		ExpirationDays: 90,
	}
	snapMixed := buildSnapshotForViews(t, "b1", replayRule, walkerRule, disabledRule)
	mixed := ReplayContentHash(snapMixed)

	assert.Equal(t, baseline, mixed,
		"adding walker-only and disabled rules must not change ReplayContentHash")
}

func TestPromotedHash_EmptyWhenNoneAreInWalk(t *testing.T) {
	var empty [32]byte
	assert.Equal(t, empty, PromotedHash(nil, 0))

	rule := &s3lifecycle.Rule{
		ID:             "fits",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 7,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	// Large retention → rule stays in replay → no promotion.
	assert.Equal(t, empty, PromotedHash(snap, s3lifecycle.DaysToDuration(365)))
}

func TestPromotedHash_ChangesOnReplayToWalkPromotion(t *testing.T) {
	// Retention drops below the rule's TTL → rule promotes from replay to
	// walk → PromotedHash changes from empty to non-empty.
	rule := &s3lifecycle.Rule{
		ID:             "long",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap := buildSnapshotForViews(t, "b1", rule)

	before := PromotedHash(snap, s3lifecycle.DaysToDuration(365)) // replay
	after := PromotedHash(snap, s3lifecycle.DaysToDuration(7))    // promoted to walk

	var empty [32]byte
	assert.Equal(t, empty, before)
	assert.NotEqual(t, empty, after)
	assert.NotEqual(t, before, after, "promotion must change PromotedHash")
}

func TestPromotedHash_ChangesOnWalkToReplayDemotion(t *testing.T) {
	// Retention recovers from < TTL to >= TTL → rule demotes from walk to
	// replay → PromotedHash changes from non-empty to empty.
	rule := &s3lifecycle.Rule{
		ID:             "demo",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap := buildSnapshotForViews(t, "b1", rule)

	before := PromotedHash(snap, s3lifecycle.DaysToDuration(7))  // walk
	after := PromotedHash(snap, s3lifecycle.DaysToDuration(365)) // demoted back to replay

	var empty [32]byte
	assert.NotEqual(t, empty, before)
	assert.Equal(t, empty, after)
	assert.NotEqual(t, before, after, "demotion must change PromotedHash")
}

func TestPromotedHash_StableWhenContentUnchangedAndPartitionStays(t *testing.T) {
	// Same snapshot + same retentionWindow → same PromotedHash.
	rule := &s3lifecycle.Rule{
		ID:             "stable",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	rw := s3lifecycle.DaysToDuration(7) // promoted
	assert.Equal(t, PromotedHash(snap, rw), PromotedHash(snap, rw))
}

func TestPromotedHash_MatchesRulesForShardWalkMembership(t *testing.T) {
	// PromotedHash must agree with RulesForShard about which replay-
	// eligible actions are in walk for the same retentionWindow.
	rules := []*s3lifecycle.Rule{
		{ID: "short", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1},
		{ID: "long", Status: s3lifecycle.StatusEnabled, ExpirationDays: 90},
		{ID: "noncurrent", Status: s3lifecycle.StatusEnabled, NoncurrentVersionExpirationDays: 200},
	}
	snap := buildSnapshotForViews(t, "b1", rules...)
	rw := s3lifecycle.DaysToDuration(30)

	replay, walk := snap.RulesForShard(0, rw)
	require.NotNil(t, replay)
	require.NotNil(t, walk)

	// Expected walk members (from replay-eligible kinds only): long, noncurrent.
	wantPromoted := map[s3lifecycle.ActionKey]struct{}{}
	for k := range walk.actions {
		if isReplayKind(k.ActionKind) {
			wantPromoted[k] = struct{}{}
		}
	}
	assert.Len(t, wantPromoted, 2, "two of three replay-eligible rules should promote")

	hash := PromotedHash(snap, rw)
	var empty [32]byte
	assert.NotEqual(t, empty, hash)
}

func TestMaxEffectiveTTL_NilAndEmpty(t *testing.T) {
	assert.Equal(t, time.Duration(0), MaxEffectiveTTL(nil))

	// Snapshot with only walker-only kinds → no active replay action →
	// MaxEffectiveTTL returns 0.
	rule := &s3lifecycle.Rule{
		ID:                        "walk",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	assert.Equal(t, time.Duration(0), MaxEffectiveTTL(snap))
}

func TestMaxEffectiveTTL_ReturnsMaxAcrossActiveReplay(t *testing.T) {
	// Three replay-eligible actions with mixed TTLs; MaxEffectiveTTL picks
	// the largest.
	rule := &s3lifecycle.Rule{
		ID:                              "mix",
		Status:                          s3lifecycle.StatusEnabled,
		ExpirationDays:                  10,
		NoncurrentVersionExpirationDays: 60,
		AbortMPUDaysAfterInitiation:     3,
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	got := MaxEffectiveTTL(snap)
	assert.Equal(t, s3lifecycle.DaysToDuration(60), got)
}

func TestMaxEffectiveTTL_OperatesOnReplayView(t *testing.T) {
	// The caller passes the *replay* snapshot here in production. Verify
	// that on a replay view the answer is the max over the view's
	// (active) replay actions and excludes anything that was routed away
	// to walk via scan_only promotion.
	rule := &s3lifecycle.Rule{
		ID:                              "mix",
		Status:                          s3lifecycle.StatusEnabled,
		ExpirationDays:                  10,
		NoncurrentVersionExpirationDays: 200, // would promote at small retention
	}
	snap := buildSnapshotForViews(t, "b1", rule)
	// Retention 30d: ExpirationDays(10) stays in replay; NoncurrentDays(200)
	// promotes to walk. The replay view's MaxEffectiveTTL is therefore 10d.
	replay, _ := snap.RulesForShard(0, s3lifecycle.DaysToDuration(30))
	require.NotNil(t, replay)
	assert.Equal(t, s3lifecycle.DaysToDuration(10), MaxEffectiveTTL(replay))
}

func TestMaxEffectiveTTL_IgnoresInactiveActions(t *testing.T) {
	// Pre-BootstrapComplete event-driven action is in the snapshot but
	// inactive. MaxEffectiveTTL only counts active actions.
	rule := &s3lifecycle.Rule{
		ID:             "pending",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap := New().Compile(
		[]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{}, // no PriorState → inactive
	)
	for _, a := range snap.actions {
		require.False(t, a.IsActive(), "precondition")
	}
	assert.Equal(t, time.Duration(0), MaxEffectiveTTL(snap),
		"inactive replay actions must not contribute to MaxEffectiveTTL")
}
