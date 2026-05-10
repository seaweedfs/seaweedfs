package engine

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Direct tests for Snapshot.OriginalDelayGroups / PredicateActions /
// DateActions accessors and MarkActive/IsActive transitions. The Compile
// tests exercise the construction path; these tests pin the read-side
// surface the router and dispatcher reach for at runtime.

func TestSnapshot_OriginalDelayGroupsExposesCompiledGroups(t *testing.T) {
	// Two action kinds with different delays must land in distinct
	// delay groups so the dispatcher polls each at the right cadence.
	rule := &s3lifecycle.Rule{
		ID:                          "r",
		Status:                      s3lifecycle.StatusEnabled,
		ExpirationDays:              7,
		AbortMPUDaysAfterInitiation: 3,
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
	groups := snap.OriginalDelayGroups()
	require.NotNil(t, groups)

	// 7d expiration delay group must contain the specific expiration key.
	expirationDelay := s3lifecycle.MinTriggerAge(rule, s3lifecycle.ActionKindExpirationDays)
	expirationKey := s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindExpirationDays}
	assert.Contains(t, groups[expirationDelay], expirationKey, "expiration delay group must carry its specific key")

	// 3d abort-mpu delay group is distinct and contains the abort key.
	abortDelay := s3lifecycle.MinTriggerAge(rule, s3lifecycle.ActionKindAbortMPU)
	assert.NotEqual(t, expirationDelay, abortDelay)
	abortKey := s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindAbortMPU}
	assert.Contains(t, groups[abortDelay], abortKey, "abort-mpu delay group must carry its specific key")
}

func TestSnapshot_OriginalDelayGroupsScanOnlyExcluded(t *testing.T) {
	// Scan-only actions don't go through originalDelayGroups (they
	// fire from the bootstrap walk only), so the dispatcher's
	// MatchOriginalWrite path won't see them.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7}
	hash := s3lifecycle.RuleHash(rule)
	scanOnlyKey := s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindExpirationDays}
	prior := map[s3lifecycle.ActionKey]PriorState{
		scanOnlyKey: {BootstrapComplete: true, Mode: ModeScanOnly},
	}
	snap := New().Compile(
		[]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{PriorStates: prior},
	)
	groups := snap.OriginalDelayGroups()
	for delay, keys := range groups {
		for _, k := range keys {
			assert.NotEqual(t, scanOnlyKey, k, "scan-only action leaked into delay group %v", delay)
		}
	}
}

func TestSnapshot_PredicateActionsContainsTagSensitive(t *testing.T) {
	// A tag-sensitive rule's actions must surface in PredicateActions
	// so MatchPredicateChange routes them.
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 7,
		FilterTags:     map[string]string{"env": "prod"},
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
	predicates := snap.PredicateActions()
	require.NotEmpty(t, predicates, "tag-sensitive rule must populate predicateActions")
	// Verify the specific key landed (not just non-empty) so a routing
	// regression that emits a wrong ActionKey is caught.
	expectedKey := s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindExpirationDays}
	assert.Contains(t, predicates, expectedKey, "predicateActions must carry the rule's specific ActionKey")
}

func TestSnapshot_PredicateActionsEmptyForNonTagSensitiveRule(t *testing.T) {
	// A rule without FilterTags is not predicate-sensitive; the
	// predicate-action list must stay empty so MatchPredicateChange
	// never routes irrelevant kinds.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7}
	snap := New().Compile(
		[]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{},
	)
	assert.Empty(t, snap.PredicateActions())
}

func TestSnapshot_DateActionsContainsExpirationDate(t *testing.T) {
	// EXPIRATION_DATE rules go into dateActions (not delay groups);
	// the scan-at-date scheduler reads from this map.
	when := time.Now().Add(24 * time.Hour)
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDate: when,
	}
	snap := New().Compile(
		[]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{},
	)
	dateActions := snap.DateActions()
	require.NotEmpty(t, dateActions)
	for _, dt := range dateActions {
		assert.Equal(t, when, dt, "DateActions must surface the rule's ExpirationDate verbatim")
	}
}

func TestSnapshot_DateActionsEmptyForNonDateRule(t *testing.T) {
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7}
	snap := New().Compile(
		[]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{},
	)
	assert.Empty(t, snap.DateActions())
}

func TestSnapshot_MarkActiveUnknownKeyIsNoOp(t *testing.T) {
	// MarkActive must silently skip keys that aren't in the snapshot;
	// the durable bootstrap-complete write can race with a recompile
	// that drops the rule, and a panic here would crash the worker.
	snap := New().Compile(nil, CompileOptions{})
	snap.MarkActive(s3lifecycle.ActionKey{Bucket: "ghost", ActionKind: s3lifecycle.ActionKindExpirationDays})
}

func TestSnapshot_MarkActiveFlipsCompiledActionToActive(t *testing.T) {
	// Bootstrap-pending actions land inactive; MarkActive transitions
	// them to active so the routing filter starts emitting matches.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 7}
	hash := s3lifecycle.RuleHash(rule)
	key := s3lifecycle.ActionKey{Bucket: "bk", RuleHash: hash, ActionKind: s3lifecycle.ActionKindExpirationDays}
	// No prior state -> the action lands inactive (BootstrapComplete=false).
	snap := New().Compile(
		[]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{},
	)
	a := snap.Action(key)
	require.NotNil(t, a)
	assert.False(t, a.IsActive(), "fresh action without prior state must be inactive")
	snap.MarkActive(key)
	assert.True(t, a.IsActive(), "MarkActive must transition the action to active")
}

func TestSnapshot_BucketIndexedActionKeysCoverAllKinds(t *testing.T) {
	// Cross-check that BucketActionKeys lists every kind compiled from
	// the rule, regardless of mode. The router iterates these for
	// MatchPath, so a missed kind silently disables prefix-only routing
	// for that action.
	rule := &s3lifecycle.Rule{
		ID:                          "r",
		Status:                      s3lifecycle.StatusEnabled,
		ExpirationDays:              7,
		AbortMPUDaysAfterInitiation: 3,
		ExpiredObjectDeleteMarker:   true,
	}
	snap := New().Compile(
		[]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}},
		CompileOptions{},
	)
	keys := snap.BucketActionKeys("bk")
	wantKinds := s3lifecycle.RuleActionKinds(rule)
	require.Len(t, keys, len(wantKinds))

	// Verify the contents, not just the count: every key must carry
	// the right bucket scope, and the kinds must match RuleActionKinds
	// element-for-element (in any order). Catches an indexing
	// regression that produces N keys but with wrong kinds.
	gotKinds := make([]s3lifecycle.ActionKind, 0, len(keys))
	for _, k := range keys {
		assert.Equal(t, "bk", k.Bucket, "key leaked across bucket")
		gotKinds = append(gotKinds, k.ActionKind)
	}
	assert.ElementsMatch(t, wantKinds, gotKinds, "kinds must match RuleActionKinds")
}
