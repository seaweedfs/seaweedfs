package s3lifecycle

import (
	"testing"
	"time"
)

// idx is a small helper for tests that need to express NoncurrentIndex as
// *int (production callers either compute it or leave it nil for current
// versions; tests want a one-liner).
func idx(i int) *int { return &i }

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return tm
}

func TestEvaluateAction_DisabledRuleNeverFires(t *testing.T) {
	rule := &Rule{Status: StatusDisabled, ExpirationDays: 1}
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")}
	if got := EvaluateAction(rule, ActionKindExpirationDays, info, mustTime(t, "2030-01-01T00:00:00Z")); got.Action != ActionNone {
		t.Fatalf("disabled rule should be no-op, got %v", got)
	}
}

func TestEvaluateAction_NilInputs(t *testing.T) {
	if EvaluateAction(nil, ActionKindExpirationDays, &ObjectInfo{}, time.Now()).Action != ActionNone {
		t.Fatalf("nil rule should be no-op")
	}
	if EvaluateAction(&Rule{Status: StatusEnabled}, ActionKindExpirationDays, nil, time.Now()).Action != ActionNone {
		t.Fatalf("nil info should be no-op")
	}
}

func TestEvaluateAction_ExpirationDaysBoundary(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ID: "r", ExpirationDays: 30}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mod}

	if got := EvaluateAction(rule, ActionKindExpirationDays, info, mod.AddDate(0, 0, 30).Add(-time.Nanosecond)); got.Action != ActionNone {
		t.Fatalf("not yet due, got %v", got)
	}
	if got := EvaluateAction(rule, ActionKindExpirationDays, info, mod.AddDate(0, 0, 30)); got.Action != ActionDeleteObject || got.RuleID != "r" {
		t.Fatalf("at boundary, got %v", got)
	}
	if got := EvaluateAction(rule, ActionKindExpirationDays, info, mod.AddDate(0, 1, 0)); got.Action != ActionDeleteObject {
		t.Fatalf("past due, got %v", got)
	}
}

func TestEvaluateAction_KindFiltersByDeclaredAction(t *testing.T) {
	// Asking the wrong kind of a rule that doesn't declare that action returns
	// ActionNone — even when the object would otherwise match.
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 30}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mod}
	now := mod.AddDate(0, 1, 0)
	if got := EvaluateAction(rule, ActionKindAbortMPU, info, now); got.Action != ActionNone {
		t.Fatalf("AbortMPU not declared, got %v", got)
	}
	if got := EvaluateAction(rule, ActionKindNoncurrentDays, info, now); got.Action != ActionNone {
		t.Fatalf("NoncurrentDays not declared, got %v", got)
	}
}

func TestEvaluateAction_MultiActionRule_SiblingsIndependent(t *testing.T) {
	// The bug this fixes: a rule with both ExpirationDays=90 and AbortMPU=7
	// is evaluated per-action; the 7d MPU window not being "due" for an
	// object event has no effect on the 90d expiration's eligibility.
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 90, AbortMPUDaysAfterInitiation: 7}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mod}

	// Past the 90d threshold: ExpirationDays fires.
	now := mod.AddDate(0, 0, 91)
	if got := EvaluateAction(rule, ActionKindExpirationDays, info, now); got.Action != ActionDeleteObject {
		t.Fatalf("90d action should fire, got %v", got)
	}
	// AbortMPU asked against a non-MPU entry: no-op.
	if got := EvaluateAction(rule, ActionKindAbortMPU, info, now); got.Action != ActionNone {
		t.Fatalf("AbortMPU on non-MPU entry should be no-op, got %v", got)
	}
}

func TestEvaluateAction_ExpirationDate(t *testing.T) {
	date := mustTime(t, "2025-06-15T00:00:00Z")
	rule := &Rule{Status: StatusEnabled, ExpirationDate: date}
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")}

	if got := EvaluateAction(rule, ActionKindExpirationDate, info, date.Add(-time.Second)); got.Action != ActionNone {
		t.Fatalf("before date, got %v", got)
	}
	if got := EvaluateAction(rule, ActionKindExpirationDate, info, date); got.Action != ActionDeleteObject {
		t.Fatalf("at date, got %v", got)
	}
}

func TestEvaluateAction_ExpiredObjectDeleteMarker(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ExpiredObjectDeleteMarker: true}
	now := mustTime(t, "2025-01-01T00:00:00Z")
	mod := mustTime(t, "2024-12-01T00:00:00Z")

	// Current sole-survivor delete marker: fires.
	info := &ObjectInfo{Key: "a", IsLatest: true, IsDeleteMarker: true, NumVersions: 1, ModTime: mod}
	if got := EvaluateAction(rule, ActionKindExpiredDeleteMarker, info, now); got.Action != ActionExpireDeleteMarker {
		t.Fatalf("sole-survivor marker, got %v", got)
	}
	// Non-sole-survivor: not eligible for EXPIRED_DELETE_MARKER.
	info.NumVersions = 2
	if got := EvaluateAction(rule, ActionKindExpiredDeleteMarker, info, now); got.Action != ActionNone {
		t.Fatalf("non-sole marker, got %v", got)
	}
	// Latest non-marker: not in scope of this kind.
	info = &ObjectInfo{Key: "a", IsLatest: true, IsDeleteMarker: false, NumVersions: 1, ModTime: mod}
	if got := EvaluateAction(rule, ActionKindExpiredDeleteMarker, info, now); got.Action != ActionNone {
		t.Fatalf("non-marker, got %v", got)
	}
}

func TestEvaluateAction_NoncurrentDeleteMarkerExpiresViaNoncurrentDays(t *testing.T) {
	// A non-current delete marker is just a version. NoncurrentVersionExpirationDays
	// must apply to it.
	rule := &Rule{Status: StatusEnabled, NoncurrentVersionExpirationDays: 7}
	successor := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{
		Key:              "a",
		IsLatest:         false,
		IsDeleteMarker:   true,
		NumVersions:      3,
		SuccessorModTime: successor,
		ModTime:          successor.AddDate(0, 0, -1),
	}
	if got := EvaluateAction(rule, ActionKindNoncurrentDays, info, successor.AddDate(0, 0, 7)); got.Action != ActionDeleteVersion {
		t.Fatalf("noncurrent delete marker should be eligible under NoncurrentDays, got %v", got)
	}
}

func TestEvaluateAction_NoncurrentVersionDays(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, NoncurrentVersionExpirationDays: 30}
	successor := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{
		Key:              "a",
		IsLatest:         false,
		ModTime:          mustTime(t, "2023-01-01T00:00:00Z"),
		SuccessorModTime: successor,
		NoncurrentIndex:  idx(0),
	}
	if got := EvaluateAction(rule, ActionKindNoncurrentDays, info, successor.AddDate(0, 0, 29)); got.Action != ActionNone {
		t.Fatalf("not due, got %v", got)
	}
	if got := EvaluateAction(rule, ActionKindNoncurrentDays, info, successor.AddDate(0, 0, 30)); got.Action != ActionDeleteVersion {
		t.Fatalf("due, got %v", got)
	}
}

func TestEvaluateAction_NoncurrentDaysFallsBackToModTime(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, NoncurrentVersionExpirationDays: 30}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: mod, NoncurrentIndex: idx(0)}
	if got := EvaluateAction(rule, ActionKindNoncurrentDays, info, mod.AddDate(0, 0, 30)); got.Action != ActionDeleteVersion {
		t.Fatalf("expected fallback to ModTime, got %v", got)
	}
}

func TestEvaluateAction_NoncurrentDays_WithKeepN_NilIndexIsNoOp(t *testing.T) {
	// Pointer-migration safety: a nil NoncurrentIndex paired with a
	// keep-N retention threshold must short-circuit to ActionNone rather
	// than guess at the version's position in the keep window.
	rule := &Rule{Status: StatusEnabled, NoncurrentVersionExpirationDays: 30, NewerNoncurrentVersions: 2}
	successor := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{
		Key:              "a",
		IsLatest:         false,
		ModTime:          successor,
		SuccessorModTime: successor,
		NoncurrentIndex:  nil,
	}
	now := successor.AddDate(0, 0, 30)
	if got := EvaluateAction(rule, ActionKindNoncurrentDays, info, now); got.Action != ActionNone {
		t.Fatalf("nil index with keep-N must be no-op, got %v", got)
	}
}

func TestEvaluateAction_NewerNoncurrent_NilIndexIsNoOp(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, NewerNoncurrentVersions: 3}
	info := &ObjectInfo{
		Key:             "a",
		IsLatest:        false,
		ModTime:         mustTime(t, "2024-01-01T00:00:00Z"),
		NoncurrentIndex: nil,
	}
	if got := EvaluateAction(rule, ActionKindNewerNoncurrent, info, mustTime(t, "2025-01-01T00:00:00Z")); got.Action != ActionNone {
		t.Fatalf("nil index pure-count must be no-op, got %v", got)
	}
}

func TestEvaluateAction_NewerNoncurrentCountOnly(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, NewerNoncurrentVersions: 3}
	now := mustTime(t, "2025-01-01T00:00:00Z")
	mod := mustTime(t, "2024-01-01T00:00:00Z")

	for i := 0; i < 3; i++ {
		info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: mod, NoncurrentIndex: idx(i)}
		if got := EvaluateAction(rule, ActionKindNewerNoncurrent, info, now); got.Action != ActionNone {
			t.Fatalf("idx=%d should be retained, got %v", i, got)
		}
	}
	info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: mod, NoncurrentIndex: idx(3)}
	if got := EvaluateAction(rule, ActionKindNewerNoncurrent, info, now); got.Action != ActionDeleteVersion {
		t.Fatalf("idx=3 should fire, got %v", got)
	}
}

func TestEvaluateAction_NoncurrentDaysAndCount(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, NoncurrentVersionExpirationDays: 30, NewerNoncurrentVersions: 2}
	successor := mustTime(t, "2024-01-01T00:00:00Z")
	now := successor.AddDate(0, 0, 30)

	for i := 0; i < 2; i++ {
		info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: successor, SuccessorModTime: successor, NoncurrentIndex: idx(i)}
		if got := EvaluateAction(rule, ActionKindNoncurrentDays, info, now); got.Action != ActionNone {
			t.Fatalf("idx=%d kept by NewerNoncurrent retention, got %v", i, got)
		}
	}
	info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: successor, SuccessorModTime: successor, NoncurrentIndex: idx(2)}
	if got := EvaluateAction(rule, ActionKindNoncurrentDays, info, now); got.Action != ActionDeleteVersion {
		t.Fatalf("idx=2 satisfies both, got %v", got)
	}
}

func TestEvaluateAction_AbortMultipartUpload(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, AbortMPUDaysAfterInitiation: 7}
	init := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: ".uploads/u1/", IsMPUInit: true, ModTime: init}
	if got := EvaluateAction(rule, ActionKindAbortMPU, info, init.AddDate(0, 0, 6)); got.Action != ActionNone {
		t.Fatalf("not due, got %v", got)
	}
	if got := EvaluateAction(rule, ActionKindAbortMPU, info, init.AddDate(0, 0, 7)); got.Action != ActionAbortMultipartUpload {
		t.Fatalf("due, got %v", got)
	}
}

func TestEvaluateAction_PrefixFilter(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, Prefix: "logs/", ExpirationDays: 1}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	if got := EvaluateAction(rule, ActionKindExpirationDays, &ObjectInfo{Key: "data/x", IsLatest: true, ModTime: mod}, now); got.Action != ActionNone {
		t.Fatalf("prefix mismatch should reject, got %v", got)
	}
	if got := EvaluateAction(rule, ActionKindExpirationDays, &ObjectInfo{Key: "logs/x", IsLatest: true, ModTime: mod}, now); got.Action != ActionDeleteObject {
		t.Fatalf("prefix match should fire, got %v", got)
	}
}

func TestEvaluateAction_TagFilter(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 1, FilterTags: map[string]string{"env": "prod", "tier": "cold"}}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)

	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mod}
	if got := EvaluateAction(rule, ActionKindExpirationDays, info, now); got.Action != ActionNone {
		t.Fatalf("missing tags should reject, got %v", got)
	}
	info.Tags = map[string]string{"env": "prod"}
	if got := EvaluateAction(rule, ActionKindExpirationDays, info, now); got.Action != ActionNone {
		t.Fatalf("partial match should reject, got %v", got)
	}
	info.Tags = map[string]string{"env": "prod", "tier": "cold", "extra": "ignored"}
	if got := EvaluateAction(rule, ActionKindExpirationDays, info, now); got.Action != ActionDeleteObject {
		t.Fatalf("all tags match should fire, got %v", got)
	}
}

func TestEvaluateAction_SizeFilter(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 1, FilterSizeGreaterThan: 100, FilterSizeLessThan: 1000}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	cases := []struct {
		size int64
		want Action
	}{
		{50, ActionNone},
		{100, ActionNone},
		{500, ActionDeleteObject},
		{1000, ActionNone},
		{2000, ActionNone},
	}
	for _, c := range cases {
		info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mod, Size: c.size}
		if got := EvaluateAction(rule, ActionKindExpirationDays, info, now); got.Action != c.want {
			t.Fatalf("size=%d want=%v got=%v", c.size, c.want, got)
		}
	}
}

func TestEvaluateAction_EmptyPrefixMatchesAll(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 1, Prefix: ""}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: "deeply/nested/path/x", IsLatest: true, ModTime: mod}
	if got := EvaluateAction(rule, ActionKindExpirationDays, info, mod.AddDate(0, 0, 10)); got.Action != ActionDeleteObject {
		t.Fatalf("empty prefix should match, got %v", got)
	}
}

func TestEvaluateAction_MPUInitDoesNotFireNoncurrent(t *testing.T) {
	// IsLatest=false on an MPU init must not let NoncurrentDays /
	// NewerNoncurrent fire. The dispatcher BLOCKs noncurrent kinds with
	// empty version_id, which would freeze the cursor.
	rule := &Rule{
		ID:                              "r",
		Status:                          StatusEnabled,
		NoncurrentVersionExpirationDays: 7,
		NewerNoncurrentVersions:         3,
	}
	init := time.Now().AddDate(0, 0, -30)
	info := &ObjectInfo{Key: "logs/foo.txt", IsMPUInit: true, ModTime: init}

	for _, kind := range []ActionKind{ActionKindNoncurrentDays, ActionKindNewerNoncurrent} {
		if got := EvaluateAction(rule, kind, info, time.Now()); got.Action != ActionNone {
			t.Fatalf("kind=%v on IsMPUInit must return None, got %+v", kind, got)
		}
	}
}
