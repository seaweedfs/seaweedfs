package engine

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prefixMatches and filterAllows are pure helpers exercised indirectly
// through MatchOriginalWrite / MatchPath; pinning them directly catches
// a regression at the helper level so a Match-test failure isn't the
// first signal of a broken filter.

func TestPrefixMatches_EmptyPrefixAllowsEverything(t *testing.T) {
	// An empty prefix on a rule means "match every key in the bucket"
	// — the no-op fast path that skips the HasPrefix call.
	assert.True(t, prefixMatches("", "logs/2026/01/01"))
	assert.True(t, prefixMatches("", ""))
	assert.True(t, prefixMatches("", "/"))
}

func TestPrefixMatches_HonorsExactPrefix(t *testing.T) {
	assert.True(t, prefixMatches("logs/", "logs/2026/01/01"))
	assert.True(t, prefixMatches("logs/", "logs/"))
}

func TestPrefixMatches_RejectsNonMatchingPath(t *testing.T) {
	assert.False(t, prefixMatches("logs/", "metrics/2026/01/01"))
	assert.False(t, prefixMatches("logs/", "log/2026/01/01")) // no trailing slash
}

func TestPrefixMatches_PathShorterThanPrefixIsReject(t *testing.T) {
	// Path shorter than the rule prefix can't satisfy HasPrefix; the
	// helper must say no rather than treat it as a partial match.
	assert.False(t, prefixMatches("logs/2026/", "logs/"))
}

func TestFilterAllows_NoFiltersAcceptsAnything(t *testing.T) {
	// A rule without any filter knobs allows every event regardless of
	// size or tag map. Pins the documented "no filter" fast path.
	rule := &s3lifecycle.Rule{}
	assert.True(t, filterAllows(rule, &Event{Size: 0}))
	assert.True(t, filterAllows(rule, &Event{Size: 1024}))
	assert.True(t, filterAllows(rule, &Event{Tags: map[string]string{"a": "b"}}))
}

func TestFilterAllows_SizeGreaterThanGate(t *testing.T) {
	// FilterSizeGreaterThan is "strictly greater"; events at or below
	// the threshold are rejected.
	rule := &s3lifecycle.Rule{FilterSizeGreaterThan: 1024}
	assert.False(t, filterAllows(rule, &Event{Size: 1024}), "boundary value rejected (gate is >, not >=)")
	assert.False(t, filterAllows(rule, &Event{Size: 100}))
	assert.True(t, filterAllows(rule, &Event{Size: 1025}))
}

func TestFilterAllows_SizeLessThanGate(t *testing.T) {
	// FilterSizeLessThan is "strictly less"; events at or above the
	// threshold are rejected.
	rule := &s3lifecycle.Rule{FilterSizeLessThan: 1024}
	assert.False(t, filterAllows(rule, &Event{Size: 1024}), "boundary value rejected (gate is <, not <=)")
	assert.False(t, filterAllows(rule, &Event{Size: 4096}))
	assert.True(t, filterAllows(rule, &Event{Size: 1023}))
}

func TestFilterAllows_ZeroSizeFilterIsDisabled(t *testing.T) {
	// FilterSizeGreaterThan/LessThan = 0 means "not set"; the filter
	// must let any size through. A regression treating 0 as a real
	// threshold would reject every event at or above 0 (i.e., all of
	// them).
	greater := &s3lifecycle.Rule{FilterSizeGreaterThan: 0}
	less := &s3lifecycle.Rule{FilterSizeLessThan: 0}
	for _, size := range []int64{0, 1, 1024, 1 << 30} {
		assert.True(t, filterAllows(greater, &Event{Size: size}), "Gt 0 must allow size %d", size)
		assert.True(t, filterAllows(less, &Event{Size: size}), "Lt 0 must allow size %d", size)
	}
}

func TestFilterAllows_RequiredTagPresent(t *testing.T) {
	// A FilterTags entry is "must equal"; the event's Tags map must
	// have the same key with the same value.
	rule := &s3lifecycle.Rule{FilterTags: map[string]string{"env": "prod"}}
	assert.True(t, filterAllows(rule, &Event{Tags: map[string]string{"env": "prod"}}))
	assert.True(t, filterAllows(rule, &Event{Tags: map[string]string{"env": "prod", "team": "a"}}))
}

func TestFilterAllows_RequiredTagMissingOrWrong(t *testing.T) {
	rule := &s3lifecycle.Rule{FilterTags: map[string]string{"env": "prod"}}
	assert.False(t, filterAllows(rule, &Event{}), "nil tags map rejected")
	assert.False(t, filterAllows(rule, &Event{Tags: map[string]string{}}), "empty tags rejected")
	assert.False(t, filterAllows(rule, &Event{Tags: map[string]string{"team": "a"}}), "different key rejected")
	assert.False(t, filterAllows(rule, &Event{Tags: map[string]string{"env": "stage"}}), "wrong value rejected")
}

func TestFilterAllows_AllRequiredTagsMustMatch(t *testing.T) {
	// Multiple FilterTags are AND'd; missing any one rejects.
	rule := &s3lifecycle.Rule{FilterTags: map[string]string{"env": "prod", "team": "data"}}
	assert.True(t, filterAllows(rule, &Event{Tags: map[string]string{"env": "prod", "team": "data"}}))
	assert.False(t, filterAllows(rule, &Event{Tags: map[string]string{"env": "prod"}}), "missing one tag rejected")
	assert.False(t, filterAllows(rule, &Event{Tags: map[string]string{"team": "data"}}), "missing other tag rejected")
	assert.False(t, filterAllows(rule, &Event{Tags: map[string]string{"env": "prod", "team": "ops"}}), "wrong value on second tag rejected")
}

func TestFilterAllows_SizeAndTagAreANDed(t *testing.T) {
	// A rule with both size and tag filters requires BOTH to pass;
	// either one failing rejects the event.
	rule := &s3lifecycle.Rule{
		FilterSizeGreaterThan: 100,
		FilterTags:            map[string]string{"env": "prod"},
	}
	// Both pass.
	assert.True(t, filterAllows(rule, &Event{Size: 200, Tags: map[string]string{"env": "prod"}}))
	// Size fails.
	assert.False(t, filterAllows(rule, &Event{Size: 50, Tags: map[string]string{"env": "prod"}}))
	// Tag fails.
	assert.False(t, filterAllows(rule, &Event{Size: 200, Tags: map[string]string{"env": "stage"}}))
}

// ---------- MatchOriginalWrite / MatchPredicateChange / MatchPath edge cases ----------

// helperSnapshot constructs a Snapshot via the engine's Compile path so
// the action map and bucket index are populated correctly. Tests below
// poke at the public Match functions rather than the internal maps.
func helperSnapshot(t *testing.T) (*Snapshot, *s3lifecycle.Rule, s3lifecycle.ActionKey) {
	t.Helper()
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		Prefix:         "logs/",
		ExpirationDays: 7,
	}
	hash := s3lifecycle.RuleHash(rule)
	prior := map[s3lifecycle.ActionKey]PriorState{}
	for _, k := range s3lifecycle.RuleActionKinds(rule) {
		prior[s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: k}] = PriorState{
			BootstrapComplete: true,
			Mode:              ModeEventDriven,
		}
	}
	snap := New().Compile(
		[]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{PriorStates: prior},
	)
	key := s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindExpirationDays}
	return snap, rule, key
}

func TestMatchOriginalWrite_NilEventReturnsNil(t *testing.T) {
	// A nil event is treated as "no match"; the routing path should
	// not panic on a defensive caller.
	snap, _, _ := helperSnapshot(t)
	assert.Nil(t, snap.MatchOriginalWrite(nil, 7*24*time.Hour))
}

func TestMatchOriginalWrite_WrongShapeReturnsNil(t *testing.T) {
	// Only EventShapeOriginalWrite goes through this path; a
	// PredicateChange event must not produce delay-group matches.
	snap, _, _ := helperSnapshot(t)
	got := snap.MatchOriginalWrite(&Event{Shape: EventShapePredicateChange, Bucket: "bk"}, 7*24*time.Hour)
	assert.Nil(t, got)
}

func TestMatchOriginalWrite_DelayMismatchReturnsNil(t *testing.T) {
	// The action is in the 7-day delay group; asking for a different
	// delay must produce no matches.
	snap, _, _ := helperSnapshot(t)
	got := snap.MatchOriginalWrite(&Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "logs/x"}, 30*24*time.Hour)
	assert.Empty(t, got)
}

func TestMatchPredicateChange_NilEventReturnsNil(t *testing.T) {
	snap, _, _ := helperSnapshot(t)
	assert.Nil(t, snap.MatchPredicateChange(nil))
}

func TestMatchPredicateChange_WrongShapeReturnsNil(t *testing.T) {
	// EventShapeOriginalWrite events go through MatchOriginalWrite; a
	// caller passing one to MatchPredicateChange gets nothing back.
	snap, _, _ := helperSnapshot(t)
	got := snap.MatchPredicateChange(&Event{Shape: EventShapeOriginalWrite, Bucket: "bk"})
	assert.Nil(t, got)
}

func TestMatchPredicateChange_NoTagSensitiveActionsReturnsNil(t *testing.T) {
	// The default helperSnapshot rule has no FilterTags, so it's not
	// tag-sensitive — predicate changes must yield no matches.
	snap, _, _ := helperSnapshot(t)
	got := snap.MatchPredicateChange(&Event{Shape: EventShapePredicateChange, Bucket: "bk", Path: "logs/x"})
	assert.Nil(t, got, "non-tag-sensitive snapshot returns nil from predicate change")
}

func TestMatchPath_UnknownBucketReturnsNil(t *testing.T) {
	snap, _, _ := helperSnapshot(t)
	got := snap.MatchPath("other-bucket", "logs/x", nil)
	assert.Nil(t, got)
}

func TestMatchPath_NilEventStillAppliesPrefix(t *testing.T) {
	// MatchPath with ev=nil applies prefix matching only; tags/size
	// don't gate. Pinned because the bootstrap walker calls it that
	// way.
	snap, _, key := helperSnapshot(t)
	matches := snap.MatchPath("bk", "logs/2026/01/01", nil)
	assert.Contains(t, matches, key)
	assert.Empty(t, snap.MatchPath("bk", "metrics/x", nil), "prefix mismatch must reject even with nil event")
}

func TestMatchPath_FilterRespectedWhenEventProvided(t *testing.T) {
	// When the caller supplies an Event, both prefix and filter gates
	// fire. A size-filter rule rejects an event below the threshold.
	rule := &s3lifecycle.Rule{
		ID:                    "r",
		Status:                s3lifecycle.StatusEnabled,
		ExpirationDays:        7,
		FilterSizeGreaterThan: 1000,
	}
	hash := s3lifecycle.RuleHash(rule)
	prior := map[s3lifecycle.ActionKey]PriorState{}
	for _, k := range s3lifecycle.RuleActionKinds(rule) {
		prior[s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: k}] = PriorState{
			BootstrapComplete: true,
			Mode:              ModeEventDriven,
		}
	}
	snap := New().Compile(
		[]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{PriorStates: prior},
	)
	belowThreshold := &Event{Size: 500}
	aboveThreshold := &Event{Size: 2000}
	assert.Empty(t, snap.MatchPath("bk", "x", belowThreshold), "size below threshold must reject")
	assert.NotEmpty(t, snap.MatchPath("bk", "x", aboveThreshold), "size above threshold must allow")
}

// ---------- filterMatching direct coverage ----------

func TestFilterMatching_BucketScoping(t *testing.T) {
	// Two buckets sharing identical rules: filterMatching must reject
	// keys whose action.Bucket doesn't match ev.Bucket so a match in
	// "alpha" doesn't bleed into "beta".
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7}
	hash := s3lifecycle.RuleHash(rule)
	prior := map[s3lifecycle.ActionKey]PriorState{}
	for _, b := range []string{"alpha", "beta"} {
		for _, k := range s3lifecycle.RuleActionKinds(rule) {
			prior[s3lifecycle.ActionKey{Bucket: b, RuleHash: hash, ActionKind: k}] = PriorState{
				BootstrapComplete: true,
				Mode:              ModeEventDriven,
			}
		}
	}
	snap := New().Compile(
		[]CompileInput{
			{Bucket: "alpha", Rules: []*s3lifecycle.Rule{rule}},
			{Bucket: "beta", Rules: []*s3lifecycle.Rule{rule}},
		},
		CompileOptions{PriorStates: prior},
	)
	got := snap.MatchOriginalWrite(&Event{
		Shape: EventShapeOriginalWrite, Bucket: "alpha", Path: "x", Size: 1,
	}, 7*24*time.Hour)
	require.NotEmpty(t, got)
	for _, k := range got {
		assert.Equal(t, "alpha", k.Bucket, "match leaked across bucket")
	}
}

func TestFilterMatching_AbortMPURequiresMPUInit(t *testing.T) {
	// AbortMPU actions only fire on MPU-init events; a normal write to
	// the same prefix must not trigger them.
	rule := &s3lifecycle.Rule{
		ID:                          "r",
		Status:                      s3lifecycle.StatusEnabled,
		AbortMPUDaysAfterInitiation: 7,
	}
	hash := s3lifecycle.RuleHash(rule)
	abortKey := s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindAbortMPU}
	prior := map[s3lifecycle.ActionKey]PriorState{
		abortKey: {BootstrapComplete: true, Mode: ModeEventDriven},
	}
	snap := New().Compile(
		[]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{PriorStates: prior},
	)
	regular := &Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "x", IsMPUInit: false}
	mpu := &Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "x", IsMPUInit: true}
	assert.Empty(t, snap.MatchOriginalWrite(regular, 7*24*time.Hour), "regular write must not trigger AbortMPU")
	assert.Contains(t, snap.MatchOriginalWrite(mpu, 7*24*time.Hour), abortKey, "MPU init must trigger AbortMPU")
}

func TestFilterMatching_ExpiredDeleteMarkerRequiresMarkerLatest(t *testing.T) {
	// EXPIRED_DELETE_MARKER actions only fire when the event is both a
	// delete marker AND the current latest version. A non-delete event
	// or a noncurrent delete marker must not trigger.
	rule := &s3lifecycle.Rule{
		ID:                        "r",
		Status:                    s3lifecycle.StatusEnabled,
		ExpiredObjectDeleteMarker: true,
	}
	hash := s3lifecycle.RuleHash(rule)
	markerKey := s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindExpiredDeleteMarker}
	prior := map[s3lifecycle.ActionKey]PriorState{
		markerKey: {BootstrapComplete: true, Mode: ModeEventDriven},
	}
	snap := New().Compile(
		[]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{PriorStates: prior},
	)
	// ExpiredDeleteMarker has MinTriggerAge=0; the action lives in the
	// zero-duration delay group, so the dispatcher polls it on every
	// tick.
	delay := time.Duration(0)
	notMarker := &Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "x", IsDeleteMarker: false, IsLatest: true}
	notLatest := &Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "x", IsDeleteMarker: true, IsLatest: false}
	bothTrue := &Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "x", IsDeleteMarker: true, IsLatest: true}
	assert.Empty(t, snap.MatchOriginalWrite(notMarker, delay), "non-marker must not trigger")
	assert.Empty(t, snap.MatchOriginalWrite(notLatest, delay), "noncurrent marker must not trigger")
	assert.Contains(t, snap.MatchOriginalWrite(bothTrue, delay), markerKey, "marker that is also latest must trigger")
}
