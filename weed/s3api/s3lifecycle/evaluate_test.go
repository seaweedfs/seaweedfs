package s3lifecycle

import (
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return tm
}

func TestEvaluate_DisabledRuleNeverFires(t *testing.T) {
	rule := &Rule{Status: StatusDisabled, ExpirationDays: 1}
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")}
	got := Evaluate(rule, info, mustTime(t, "2030-01-01T00:00:00Z"))
	if got.Action != ActionNone {
		t.Fatalf("disabled rule should be no-op, got %v", got)
	}
}

func TestEvaluate_NilInputs(t *testing.T) {
	if Evaluate(nil, &ObjectInfo{}, time.Now()).Action != ActionNone {
		t.Fatalf("nil rule should be no-op")
	}
	if Evaluate(&Rule{Status: StatusEnabled}, nil, time.Now()).Action != ActionNone {
		t.Fatalf("nil info should be no-op")
	}
}

func TestEvaluate_ExpirationDays(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ID: "r", ExpirationDays: 30}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mod}

	// One ns before due: not yet eligible.
	if got := Evaluate(rule, info, mod.AddDate(0, 0, 30).Add(-time.Nanosecond)); got.Action != ActionNone {
		t.Fatalf("not yet due, got %v", got)
	}
	// Exactly at due: eligible.
	if got := Evaluate(rule, info, mod.AddDate(0, 0, 30)); got.Action != ActionDeleteObject || got.RuleID != "r" {
		t.Fatalf("at boundary, got %v", got)
	}
	// Past due: eligible.
	if got := Evaluate(rule, info, mod.AddDate(0, 1, 0)); got.Action != ActionDeleteObject {
		t.Fatalf("past due, got %v", got)
	}
}

func TestEvaluate_ExpirationDate(t *testing.T) {
	date := mustTime(t, "2025-06-15T00:00:00Z")
	rule := &Rule{Status: StatusEnabled, ExpirationDate: date}
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")}

	if got := Evaluate(rule, info, date.Add(-time.Second)); got.Action != ActionNone {
		t.Fatalf("before date, got %v", got)
	}
	if got := Evaluate(rule, info, date); got.Action != ActionDeleteObject {
		t.Fatalf("at date, got %v", got)
	}
}

func TestEvaluate_ExpiredObjectDeleteMarker(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ExpiredObjectDeleteMarker: true}
	now := mustTime(t, "2025-01-01T00:00:00Z")
	mod := mustTime(t, "2024-12-01T00:00:00Z")

	// Marker that is sole survivor: fires.
	info := &ObjectInfo{Key: "a", IsLatest: true, IsDeleteMarker: true, NumVersions: 1, ModTime: mod}
	if got := Evaluate(rule, info, now); got.Action != ActionExpireDeleteMarker {
		t.Fatalf("sole-survivor marker, got %v", got)
	}
	// Marker with sibling versions: does not fire.
	info.NumVersions = 2
	if got := Evaluate(rule, info, now); got.Action != ActionNone {
		t.Fatalf("non-sole marker, got %v", got)
	}
	// Latest non-marker entry under same rule: does not fire.
	info = &ObjectInfo{Key: "a", IsLatest: true, IsDeleteMarker: false, NumVersions: 1, ModTime: mod}
	if got := Evaluate(rule, info, now); got.Action != ActionNone {
		t.Fatalf("non-marker, got %v", got)
	}
}

func TestEvaluate_NoncurrentVersionDays(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, NoncurrentVersionExpirationDays: 30}
	successor := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{
		Key:              "a",
		IsLatest:         false,
		ModTime:          mustTime(t, "2023-01-01T00:00:00Z"),
		SuccessorModTime: successor,
		NoncurrentIndex:  0,
	}
	if got := Evaluate(rule, info, successor.AddDate(0, 0, 29)); got.Action != ActionNone {
		t.Fatalf("not due, got %v", got)
	}
	if got := Evaluate(rule, info, successor.AddDate(0, 0, 30)); got.Action != ActionDeleteVersion {
		t.Fatalf("due, got %v", got)
	}
}

func TestEvaluate_NoncurrentVersionDays_FallsBackToModTime(t *testing.T) {
	// Successor mtime missing — fall back to ModTime so we don't misfire early.
	rule := &Rule{Status: StatusEnabled, NoncurrentVersionExpirationDays: 30}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: mod, NoncurrentIndex: 0}
	if got := Evaluate(rule, info, mod.AddDate(0, 0, 30)); got.Action != ActionDeleteVersion {
		t.Fatalf("expected fallback to ModTime, got %v", got)
	}
}

func TestEvaluate_NewerNoncurrentVersions_CountOnly(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, NewerNoncurrentVersions: 3}
	now := mustTime(t, "2025-01-01T00:00:00Z")
	mod := mustTime(t, "2024-01-01T00:00:00Z")

	// Index 2 = third-newest non-current; rule keeps 3 newest, so index < 3 stays.
	for i := 0; i < 3; i++ {
		info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: mod, NoncurrentIndex: i}
		if got := Evaluate(rule, info, now); got.Action != ActionNone {
			t.Fatalf("idx=%d should be retained, got %v", i, got)
		}
	}
	// Index 3 is beyond the keep-N window.
	info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: mod, NoncurrentIndex: 3}
	if got := Evaluate(rule, info, now); got.Action != ActionDeleteVersion {
		t.Fatalf("idx=3 should fire, got %v", got)
	}
}

func TestEvaluate_NoncurrentDaysAndCount(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, NoncurrentVersionExpirationDays: 30, NewerNoncurrentVersions: 2}
	successor := mustTime(t, "2024-01-01T00:00:00Z")
	now := successor.AddDate(0, 0, 30) // exactly at days threshold

	// idx=0,1 protected by NewerNoncurrent retention even though days threshold passed.
	for i := 0; i < 2; i++ {
		info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: successor, SuccessorModTime: successor, NoncurrentIndex: i}
		if got := Evaluate(rule, info, now); got.Action != ActionNone {
			t.Fatalf("idx=%d kept by NewerNoncurrent, got %v", i, got)
		}
	}
	// idx=2 satisfies both: days passed AND beyond keep window.
	info := &ObjectInfo{Key: "a", IsLatest: false, ModTime: successor, SuccessorModTime: successor, NoncurrentIndex: 2}
	if got := Evaluate(rule, info, now); got.Action != ActionDeleteVersion {
		t.Fatalf("idx=2 satisfies both, got %v", got)
	}
}

func TestEvaluate_AbortMultipartUpload(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, AbortMPUDaysAfterInitiation: 7}
	init := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: ".uploads/u1/", IsMPUInit: true, ModTime: init}
	if got := Evaluate(rule, info, init.AddDate(0, 0, 6)); got.Action != ActionNone {
		t.Fatalf("not due, got %v", got)
	}
	if got := Evaluate(rule, info, init.AddDate(0, 0, 7)); got.Action != ActionAbortMultipartUpload {
		t.Fatalf("due, got %v", got)
	}
}

func TestEvaluate_PrefixFilter(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, Prefix: "logs/", ExpirationDays: 1}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	if got := Evaluate(rule, &ObjectInfo{Key: "data/x", IsLatest: true, ModTime: mod}, now); got.Action != ActionNone {
		t.Fatalf("prefix mismatch should reject, got %v", got)
	}
	if got := Evaluate(rule, &ObjectInfo{Key: "logs/x", IsLatest: true, ModTime: mod}, now); got.Action != ActionDeleteObject {
		t.Fatalf("prefix match should fire, got %v", got)
	}
	if got := Evaluate(rule, &ObjectInfo{Key: "logs/", IsLatest: true, ModTime: mod}, now); got.Action != ActionDeleteObject {
		t.Fatalf("prefix-equal-key should fire, got %v", got)
	}
}

func TestEvaluate_TagFilter(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 1, FilterTags: map[string]string{"env": "prod", "tier": "cold"}}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)

	// Missing both.
	info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mod}
	if got := Evaluate(rule, info, now); got.Action != ActionNone {
		t.Fatalf("missing tags should reject, got %v", got)
	}
	// Only one tag matching.
	info.Tags = map[string]string{"env": "prod"}
	if got := Evaluate(rule, info, now); got.Action != ActionNone {
		t.Fatalf("partial match should reject, got %v", got)
	}
	// Both tags matching.
	info.Tags = map[string]string{"env": "prod", "tier": "cold", "extra": "ignored"}
	if got := Evaluate(rule, info, now); got.Action != ActionDeleteObject {
		t.Fatalf("all tags match should fire, got %v", got)
	}
	// Wrong value.
	info.Tags = map[string]string{"env": "prod", "tier": "hot"}
	if got := Evaluate(rule, info, now); got.Action != ActionNone {
		t.Fatalf("wrong value should reject, got %v", got)
	}
}

func TestEvaluate_SizeFilter(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 1, FilterSizeGreaterThan: 100, FilterSizeLessThan: 1000}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	cases := []struct {
		size int64
		want Action
	}{
		{50, ActionNone},     // below GT
		{100, ActionNone},    // equal to GT (excluded by AWS semantics)
		{500, ActionDeleteObject},
		{1000, ActionNone}, // equal to LT (excluded)
		{2000, ActionNone}, // above LT
	}
	for _, c := range cases {
		info := &ObjectInfo{Key: "a", IsLatest: true, ModTime: mod, Size: c.size}
		if got := Evaluate(rule, info, now); got.Action != c.want {
			t.Fatalf("size=%d want=%v got=%v", c.size, c.want, got)
		}
	}
}

func TestEvaluate_EmptyPrefixMatchesAll(t *testing.T) {
	rule := &Rule{Status: StatusEnabled, ExpirationDays: 1, Prefix: ""}
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	info := &ObjectInfo{Key: "deeply/nested/path/x", IsLatest: true, ModTime: mod}
	if got := Evaluate(rule, info, mod.AddDate(0, 0, 10)); got.Action != ActionDeleteObject {
		t.Fatalf("empty prefix should match anything, got %v", got)
	}
}
