package engine

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

func ruleExpDays(id, prefix string, days int) *s3lifecycle.Rule {
	return &s3lifecycle.Rule{
		ID:             id,
		Status:         s3lifecycle.StatusEnabled,
		Prefix:         prefix,
		ExpirationDays: days,
	}
}

func TestCompile_SingleRuleSingleAction(t *testing.T) {
	e := New()
	rule := ruleExpDays("r1", "logs/", 30)
	snap := e.Compile([]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{
		PriorStates: map[s3lifecycle.ActionKey]PriorState{
			{Bucket: "b1", RuleHash: s3lifecycle.RuleHash(rule), ActionKind: s3lifecycle.ActionKindExpirationDays}: {
				BootstrapComplete: true,
			},
		},
	})
	if got := len(snap.actions); got != 1 {
		t.Fatalf("want 1 action, got %d", got)
	}
	for _, a := range snap.actions {
		if !a.IsActive() {
			t.Fatalf("bootstrap_complete + EVENT_DRIVEN should activate, got mode=%v", a.Mode)
		}
		if a.Mode != ModeEventDriven {
			t.Fatalf("want EVENT_DRIVEN, got %v", a.Mode)
		}
		if a.Delay != 30*24*time.Hour {
			t.Fatalf("want 30d delay, got %v", a.Delay)
		}
	}
}

func TestCompile_MultiAction_SiblingsHaveOwnEntries(t *testing.T) {
	// One XML rule with three actions -> three CompiledActions, three
	// distinct ActionKeys, three independent delay group memberships.
	rule := &s3lifecycle.Rule{
		ID:                              "multi",
		Status:                          s3lifecycle.StatusEnabled,
		Prefix:                          "data/",
		ExpirationDays:                  90,
		NoncurrentVersionExpirationDays: 30,
		AbortMPUDaysAfterInitiation:     7,
	}
	rh := s3lifecycle.RuleHash(rule)
	prior := map[s3lifecycle.ActionKey]PriorState{}
	for _, k := range s3lifecycle.RuleActionKinds(rule) {
		prior[s3lifecycle.ActionKey{Bucket: "b1", RuleHash: rh, ActionKind: k}] = PriorState{BootstrapComplete: true}
	}
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{PriorStates: prior})

	if got := len(snap.actions); got != 3 {
		t.Fatalf("want 3 actions, got %d", got)
	}
	wantDelays := map[time.Duration]bool{
		90 * 24 * time.Hour: true, // ExpirationDays
		30 * 24 * time.Hour: true, // NoncurrentDays
		7 * 24 * time.Hour:  true, // AbortMPU
	}
	for delay, keys := range snap.originalDelayGroups {
		if !wantDelays[delay] {
			t.Fatalf("unexpected delay group %v", delay)
		}
		if len(keys) != 1 {
			t.Fatalf("delay %v should hold exactly its own action, got %d", delay, len(keys))
		}
	}
	if len(snap.originalDelayGroups) != 3 {
		t.Fatalf("want 3 delay groups, got %d", len(snap.originalDelayGroups))
	}
}

func TestCompile_BootstrapPendingIndexedButInactive(t *testing.T) {
	// Without prior bootstrap_complete=true the action is pending_bootstrap.
	// It IS indexed in originalDelayGroups (so a later MarkActive flip is
	// routable without recompile), but IsActive() reads false so the
	// reader's IsActive-filter skips dispatch.
	rule := ruleExpDays("r1", "logs/", 30)
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{})

	for _, a := range snap.actions {
		if a.IsActive() {
			t.Fatalf("pending_bootstrap action should not be active")
		}
	}
	if len(snap.originalDelayGroups) != 1 {
		t.Fatalf("EVENT_DRIVEN action should be indexed even when inactive, got %v", snap.originalDelayGroups)
	}
}

func TestCompile_RetentionGate(t *testing.T) {
	// A 90d ExpirationDays rule with 30d retention should land in scan_only.
	// A 7d rule (under retention - lookback) stays event_driven.
	long := ruleExpDays("long", "x/", 90)
	short := ruleExpDays("short", "y/", 1)
	rules := []*s3lifecycle.Rule{long, short}
	prior := map[s3lifecycle.ActionKey]PriorState{}
	for _, r := range rules {
		k := s3lifecycle.ActionKey{Bucket: "b1", RuleHash: s3lifecycle.RuleHash(r), ActionKind: s3lifecycle.ActionKindExpirationDays}
		prior[k] = PriorState{BootstrapComplete: true}
	}

	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "b1", Rules: rules}}, CompileOptions{
		MetaLogRetention:     30 * 24 * time.Hour,
		BootstrapLookbackMin: 5 * time.Minute,
		PriorStates:          prior,
	})

	for _, a := range snap.actions {
		if a.Rule.ID == "long" && a.Mode != ModeScanOnly {
			t.Fatalf("90d rule under 30d retention should be scan_only, got %v", a.Mode)
		}
		if a.Rule.ID == "short" && a.Mode != ModeEventDriven {
			t.Fatalf("1d rule should be event_driven, got %v", a.Mode)
		}
	}
}

func TestCompile_RetentionUnboundedNeverGates(t *testing.T) {
	// MetaLogRetention=0 means unbounded (default deployment); even a 100y
	// rule should stay event_driven.
	rule := ruleExpDays("decade", "x/", 36500)
	prior := map[s3lifecycle.ActionKey]PriorState{
		{RuleHash: s3lifecycle.RuleHash(rule), ActionKind: s3lifecycle.ActionKindExpirationDays}: {BootstrapComplete: true},
	}
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{PriorStates: prior})
	for _, a := range snap.actions {
		if a.Mode != ModeEventDriven {
			t.Fatalf("unbounded retention should keep event_driven, got %v", a.Mode)
		}
	}
}

func TestCompile_SiblingsDegradeIndependently(t *testing.T) {
	// 90d ExpirationDays + 7d AbortMPU under 30d retention: ExpirationDays
	// degrades to scan_only, AbortMPU stays event_driven. The whole point
	// of per-action keying.
	rule := &s3lifecycle.Rule{
		ID:                          "mixed",
		Status:                      s3lifecycle.StatusEnabled,
		Prefix:                      "x/",
		ExpirationDays:              90,
		AbortMPUDaysAfterInitiation: 7,
	}
	rh := s3lifecycle.RuleHash(rule)
	prior := map[s3lifecycle.ActionKey]PriorState{
		{RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}: {BootstrapComplete: true},
		{RuleHash: rh, ActionKind: s3lifecycle.ActionKindAbortMPU}:       {BootstrapComplete: true},
	}
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{
		MetaLogRetention:     30 * 24 * time.Hour,
		BootstrapLookbackMin: 5 * time.Minute,
		PriorStates:          prior,
	})
	expDaysKey := s3lifecycle.ActionKey{Bucket: "b1", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}
	mpuKey := s3lifecycle.ActionKey{Bucket: "b1", RuleHash: rh, ActionKind: s3lifecycle.ActionKindAbortMPU}
	if snap.actions[expDaysKey].Mode != ModeScanOnly {
		t.Fatalf("ExpirationDays should be scan_only under 30d retention")
	}
	if snap.actions[mpuKey].Mode != ModeEventDriven {
		t.Fatalf("AbortMPU should stay event_driven (sibling degrades independently)")
	}
}

func TestCompile_PriorModePreservedOverDecideMode(t *testing.T) {
	// A durably-persisted SCAN_ONLY (or DISABLED, or any degraded mode)
	// must not be re-promoted to EVENT_DRIVEN by decideMode on every
	// Compile. Otherwise lag-fallback / operator pause / manual scan-only
	// don't survive an engine rebuild.
	rule := ruleExpDays("r", "x/", 30)
	rh := s3lifecycle.RuleHash(rule)
	key := s3lifecycle.ActionKey{Bucket: "b1", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}

	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{
		PriorStates: map[s3lifecycle.ActionKey]PriorState{
			key: {BootstrapComplete: true, Mode: ModeScanOnly},
		},
	})
	if snap.actions[key].Mode != ModeScanOnly {
		t.Fatalf("durable Mode=ScanOnly should win over decideMode, got %v", snap.actions[key].Mode)
	}
	if snap.actions[key].IsActive() {
		t.Fatalf("ScanOnly action must not be active")
	}
	// And: a missing PriorState falls through to decideMode as before.
	rule2 := ruleExpDays("fresh", "x/", 30)
	key2 := s3lifecycle.ActionKey{Bucket: "b1", RuleHash: s3lifecycle.RuleHash(rule2), ActionKind: s3lifecycle.ActionKindExpirationDays}
	snap2 := e.Compile([]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule2}}}, CompileOptions{})
	if snap2.actions[key2].Mode != ModeEventDriven {
		t.Fatalf("missing prior should fall through to decideMode (EventDriven), got %v", snap2.actions[key2].Mode)
	}
}

func TestCompile_ExpirationDateScansAtDate(t *testing.T) {
	date := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	rule := &s3lifecycle.Rule{
		ID:             "d",
		Status:         s3lifecycle.StatusEnabled,
		Prefix:         "x/",
		ExpirationDate: date,
	}
	prior := map[s3lifecycle.ActionKey]PriorState{
		{RuleHash: s3lifecycle.RuleHash(rule), ActionKind: s3lifecycle.ActionKindExpirationDate}: {BootstrapComplete: true},
	}
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{PriorStates: prior})
	if len(snap.dateActions) != 1 {
		t.Fatalf("want 1 date action, got %d", len(snap.dateActions))
	}
	for _, d := range snap.dateActions {
		if !d.Equal(date) {
			t.Fatalf("date want %v, got %v", date, d)
		}
	}
}

func TestCompile_DisabledRuleNeverActivates(t *testing.T) {
	rule := ruleExpDays("d", "x/", 30)
	rule.Status = s3lifecycle.StatusDisabled
	prior := map[s3lifecycle.ActionKey]PriorState{
		{RuleHash: s3lifecycle.RuleHash(rule), ActionKind: s3lifecycle.ActionKindExpirationDays}: {BootstrapComplete: true},
	}
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{PriorStates: prior})
	for _, a := range snap.actions {
		if a.Mode != ModeDisabled || a.IsActive() {
			t.Fatalf("disabled rule must be ModeDisabled and inactive")
		}
	}
}

func TestSnapshot_MarkActiveFlipsRoutingFilter(t *testing.T) {
	rule := ruleExpDays("r", "x/", 30)
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "b1", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{})
	key := s3lifecycle.ActionKey{Bucket: "b1", RuleHash: s3lifecycle.RuleHash(rule), ActionKind: s3lifecycle.ActionKindExpirationDays}

	if snap.actions[key].IsActive() {
		t.Fatalf("should start inactive")
	}
	snap.MarkActive(key)
	if !snap.actions[key].IsActive() {
		t.Fatalf("MarkActive should flip the bit")
	}
	// MarkActive on a missing key is a no-op (stale callback after rebuild).
	snap.MarkActive(s3lifecycle.ActionKey{})
}

func TestCompile_CrossBucketIdenticalRulesDoNotCollide(t *testing.T) {
	// Two buckets carry rules whose XML — and therefore RuleHash — is
	// identical. ActionKey must be bucket-scoped so the second bucket's
	// CompiledAction does not overwrite the first's in snap.actions.
	rule := ruleExpDays("shared", "x/", 30)
	rh := s3lifecycle.RuleHash(rule)
	prior := map[s3lifecycle.ActionKey]PriorState{
		{Bucket: "alpha", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}: {BootstrapComplete: true},
		{Bucket: "beta", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}:  {BootstrapComplete: true},
	}
	e := New()
	snap := e.Compile([]CompileInput{
		{Bucket: "alpha", Rules: []*s3lifecycle.Rule{rule}},
		{Bucket: "beta", Rules: []*s3lifecycle.Rule{rule}},
	}, CompileOptions{PriorStates: prior})

	if got := len(snap.actions); got != 2 {
		t.Fatalf("want 2 actions (one per bucket), got %d", got)
	}
	alphaKey := s3lifecycle.ActionKey{Bucket: "alpha", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}
	betaKey := s3lifecycle.ActionKey{Bucket: "beta", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}
	if snap.actions[alphaKey] == nil || snap.actions[alphaKey].Bucket != "alpha" {
		t.Fatalf("alpha bucket action missing or wrong bucket")
	}
	if snap.actions[betaKey] == nil || snap.actions[betaKey].Bucket != "beta" {
		t.Fatalf("beta bucket action missing or wrong bucket")
	}
}

func TestEngine_SnapshotAtomicSwap(t *testing.T) {
	e := New()
	r1 := ruleExpDays("r1", "a/", 1)
	snap1 := e.Compile([]CompileInput{{Bucket: "b", Rules: []*s3lifecycle.Rule{r1}}}, CompileOptions{})
	if snap1.SnapshotID() == 0 {
		t.Fatalf("snapshot id should be > 0")
	}
	r2 := ruleExpDays("r2", "b/", 2)
	snap2 := e.Compile([]CompileInput{{Bucket: "b", Rules: []*s3lifecycle.Rule{r2}}}, CompileOptions{})
	if snap2.SnapshotID() <= snap1.SnapshotID() {
		t.Fatalf("snapshot id should be monotonic")
	}
	if e.Snapshot() != snap2 {
		t.Fatalf("Engine.Snapshot should return the latest")
	}
}
