package engine

import (
	"reflect"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// activeAll returns a PriorStates map that activates every action of the
// given canonical rules under bucket.
func activeAll(bucket string, rules []*s3lifecycle.Rule) map[s3lifecycle.ActionKey]PriorState {
	out := map[s3lifecycle.ActionKey]PriorState{}
	for _, r := range rules {
		rh := s3lifecycle.RuleHash(r)
		for _, k := range s3lifecycle.RuleActionKinds(r) {
			out[s3lifecycle.ActionKey{Bucket: bucket, RuleHash: rh, ActionKind: k}] = PriorState{BootstrapComplete: true}
		}
	}
	return out
}

func sortedKeys(keys []s3lifecycle.ActionKey) []s3lifecycle.ActionKey {
	out := append([]s3lifecycle.ActionKey(nil), keys...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].ActionKind < out[j].ActionKind
	})
	return out
}

func TestMatchOriginalWrite_DelayGroupRoutes(t *testing.T) {
	r30 := ruleExpDays("a", "x/", 30)
	r60 := ruleExpDays("b", "x/", 60)
	rules := []*s3lifecycle.Rule{r30, r60}
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "bk", Rules: rules}}, CompileOptions{PriorStates: activeAll("bk", rules)})

	ev := &Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "x/o"}
	got30 := snap.MatchOriginalWrite(ev, s3lifecycle.DaysToDuration(30))
	if len(got30) != 1 || got30[0].ActionKind != s3lifecycle.ActionKindExpirationDays {
		t.Fatalf("30d sweep should match r30, got %v", got30)
	}
	got60 := snap.MatchOriginalWrite(ev, s3lifecycle.DaysToDuration(60))
	if len(got60) != 1 {
		t.Fatalf("60d sweep should match r60, got %v", got60)
	}
}

func TestMatchOriginalWrite_PrefixFilter(t *testing.T) {
	r := ruleExpDays("a", "logs/", 30)
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{r}}}, CompileOptions{PriorStates: activeAll("bk", []*s3lifecycle.Rule{r})})

	if got := snap.MatchOriginalWrite(&Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "data/x"}, s3lifecycle.DaysToDuration(30)); len(got) != 0 {
		t.Fatalf("non-matching prefix should reject, got %v", got)
	}
	if got := snap.MatchOriginalWrite(&Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "logs/x"}, s3lifecycle.DaysToDuration(30)); len(got) != 1 {
		t.Fatalf("matching prefix should fire, got %v", got)
	}
}

func TestMatchOriginalWrite_MarkActiveBecomesRoutable(t *testing.T) {
	// pending_bootstrap actions are indexed but inactive: MatchOriginalWrite
	// returns nothing. After MarkActive flips engineState, the same key is
	// routable in subsequent matches without a recompile.
	r := ruleExpDays("a", "x/", 30)
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{r}}}, CompileOptions{})
	ev := &Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "x/o"}
	if got := snap.MatchOriginalWrite(ev, s3lifecycle.DaysToDuration(30)); len(got) != 0 {
		t.Fatalf("inactive action should not match, got %v", got)
	}
	snap.MarkActive(s3lifecycle.ActionKey{Bucket: "bk", RuleHash: s3lifecycle.RuleHash(r), ActionKind: s3lifecycle.ActionKindExpirationDays})
	if got := snap.MatchOriginalWrite(ev, s3lifecycle.DaysToDuration(30)); len(got) != 1 {
		t.Fatalf("post-markActive should be routable, got %v", got)
	}
}

func TestMatchOriginalWrite_AbortMPUOnlyOnMPUInit(t *testing.T) {
	r := &s3lifecycle.Rule{
		ID:                          "mpu",
		Status:                      s3lifecycle.StatusEnabled,
		Prefix:                      ".uploads/",
		AbortMPUDaysAfterInitiation: 7,
	}
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{r}}}, CompileOptions{PriorStates: activeAll("bk", []*s3lifecycle.Rule{r})})

	// Non-MPU event under .uploads/ prefix: filtered out by shape gating.
	got := snap.MatchOriginalWrite(&Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: ".uploads/u1/", IsMPUInit: false}, s3lifecycle.DaysToDuration(7))
	if len(got) != 0 {
		t.Fatalf("non-MPU event should be filtered, got %v", got)
	}
	got = snap.MatchOriginalWrite(&Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: ".uploads/u1/", IsMPUInit: true}, s3lifecycle.DaysToDuration(7))
	if len(got) != 1 {
		t.Fatalf("MPU init should fire, got %v", got)
	}
}

func TestMatchPredicateChange_OnlyTagSensitiveActions(t *testing.T) {
	rTag := &s3lifecycle.Rule{
		ID:             "tag",
		Status:         s3lifecycle.StatusEnabled,
		Prefix:         "x/",
		ExpirationDays: 30,
		FilterTags:     map[string]string{"env": "prod"},
	}
	rPlain := ruleExpDays("plain", "x/", 30)
	rules := []*s3lifecycle.Rule{rTag, rPlain}
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "bk", Rules: rules}}, CompileOptions{PriorStates: activeAll("bk", rules)})

	// Predicate-change event: should match only the predicate-sensitive rule.
	ev := &Event{Shape: EventShapePredicateChange, Bucket: "bk", Path: "x/o", Tags: map[string]string{"env": "prod"}}
	got := snap.MatchPredicateChange(ev)
	if len(got) != 1 {
		t.Fatalf("only the tag-sensitive rule should match, got %v", got)
	}
	rh := s3lifecycle.RuleHash(rTag)
	if got[0].RuleHash != rh {
		t.Fatalf("expected match on tag-rule")
	}

	// Wrong shape: never matches.
	if got := snap.MatchPredicateChange(&Event{Shape: EventShapeOriginalWrite, Bucket: "bk", Path: "x/o"}); len(got) != 0 {
		t.Fatalf("wrong shape should be empty, got %v", got)
	}
}

func TestMatchPath_BootstrapWalkerSeesAllActiveActions(t *testing.T) {
	// One rule with three actions (90d Expiration, 30d NoncurrentDays,
	// 7d AbortMPU). MatchPath returns every active action whose filter
	// matches the path; the bootstrap walker iterates these and calls
	// EvaluateAction per kind for the entry.
	rule := &s3lifecycle.Rule{
		ID:                              "multi",
		Status:                          s3lifecycle.StatusEnabled,
		Prefix:                          "x/",
		ExpirationDays:                  90,
		NoncurrentVersionExpirationDays: 30,
		AbortMPUDaysAfterInitiation:     7,
	}
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{PriorStates: activeAll("bk", []*s3lifecycle.Rule{rule})})

	got := snap.MatchPath("bk", "x/obj", nil)
	if len(got) != 3 {
		t.Fatalf("want all 3 active actions, got %v", got)
	}
	wantKinds := []s3lifecycle.ActionKind{
		s3lifecycle.ActionKindExpirationDays,
		s3lifecycle.ActionKindNoncurrentDays,
		s3lifecycle.ActionKindAbortMPU,
	}
	gotKinds := []s3lifecycle.ActionKind{}
	for _, k := range sortedKeys(got) {
		gotKinds = append(gotKinds, k.ActionKind)
	}
	sort.Slice(wantKinds, func(i, j int) bool { return wantKinds[i] < wantKinds[j] })
	if !reflect.DeepEqual(gotKinds, wantKinds) {
		t.Fatalf("want kinds %v, got %v", wantKinds, gotKinds)
	}
}

func TestMatchPath_PrefixMismatchExcludes(t *testing.T) {
	rule := ruleExpDays("a", "logs/", 30)
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{PriorStates: activeAll("bk", []*s3lifecycle.Rule{rule})})
	if got := snap.MatchPath("bk", "data/x", nil); len(got) != 0 {
		t.Fatalf("prefix mismatch should reject, got %v", got)
	}
}

func TestMatchPath_FilterRejectsByTag(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "tag",
		Status:         s3lifecycle.StatusEnabled,
		Prefix:         "x/",
		ExpirationDays: 30,
		FilterTags:     map[string]string{"env": "prod"},
	}
	e := New()
	snap := e.Compile([]CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}}, CompileOptions{PriorStates: activeAll("bk", []*s3lifecycle.Rule{rule})})
	// With ev passed, tag mismatch rejects.
	ev := &Event{Bucket: "bk", Path: "x/obj", Tags: map[string]string{"env": "dev"}}
	if got := snap.MatchPath("bk", "x/obj", ev); len(got) != 0 {
		t.Fatalf("tag mismatch should reject, got %v", got)
	}
	// With ev nil (caller will fetch live state): prefix-only, returns the action.
	if got := snap.MatchPath("bk", "x/obj", nil); len(got) != 1 {
		t.Fatalf("ev=nil prefix-only path should return action, got %v", got)
	}
}
