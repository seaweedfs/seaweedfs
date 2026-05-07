package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// recorder captures dispatched (action, entry) pairs for assertion.
type recorder struct {
	calls []dispatchCall
	err   error // when set, every Delete returns this error
}

type dispatchCall struct {
	kind s3lifecycle.ActionKind
	path string
}

func (r *recorder) Delete(ctx context.Context, action *engine.CompiledAction, entry *Entry) error {
	if r.err != nil {
		return r.err
	}
	r.calls = append(r.calls, dispatchCall{kind: action.Key.ActionKind, path: entry.Path})
	return nil
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return tm
}

func compileEvDriven(t *testing.T, bucket string, rules ...*s3lifecycle.Rule) *engine.Snapshot {
	t.Helper()
	prior := map[s3lifecycle.ActionKey]engine.PriorState{}
	for _, r := range rules {
		rh := s3lifecycle.RuleHash(r)
		for _, k := range s3lifecycle.RuleActionKinds(r) {
			prior[s3lifecycle.ActionKey{Bucket: bucket, RuleHash: rh, ActionKind: k}] = engine.PriorState{BootstrapComplete: true}
		}
	}
	e := engine.New()
	return e.Compile([]engine.CompileInput{{Bucket: bucket, Rules: rules}}, engine.CompileOptions{PriorStates: prior})
}

func TestWalk_DispatchesDueActions(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
		Prefix:         "logs/",
	}
	snap := compileEvDriven(t, "bk", rule)

	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 60) // past the 30d threshold
	entries := []*Entry{
		{Path: "data/x", IsLatest: true, ModTime: mod}, // wrong prefix
		{Path: "logs/a", IsLatest: true, ModTime: mod}, // due
		{Path: "logs/b", IsLatest: true, ModTime: now}, // not yet due (mod=now)
	}

	rec := &recorder{}
	cp, err := Walk(context.Background(), snap, "bk", EntryCallback(entries), rec, WalkOptions{Now: now})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if !cp.Completed {
		t.Fatalf("walk should complete")
	}
	if cp.LastScannedPath != "logs/b" {
		t.Fatalf("checkpoint last scanned want logs/b, got %q", cp.LastScannedPath)
	}
	if len(rec.calls) != 1 || rec.calls[0].path != "logs/a" {
		t.Fatalf("dispatched calls want [logs/a], got %v", rec.calls)
	}
}

func TestWalk_MultiActionRule_AllDueDispatched(t *testing.T) {
	// One rule with three actions; all currently-due for the entry. The
	// walker must dispatch one Delete per action — this is the
	// regression that the per-action keying fixes.
	rule := &s3lifecycle.Rule{
		ID:                              "multi",
		Status:                          s3lifecycle.StatusEnabled,
		ExpirationDays:                  30,
		NoncurrentVersionExpirationDays: 7,
		AbortMPUDaysAfterInitiation:     5,
	}
	snap := compileEvDriven(t, "bk", rule)

	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 100) // past every threshold
	// Use a single entry that satisfies all three action shapes is
	// unrealistic; in practice each shape is a different entry. Cover
	// each shape independently.
	entries := []*Entry{
		// Current version under ExpirationDays.
		{Path: "obj/a", IsLatest: true, ModTime: mod},
		// Non-current version under NoncurrentDays.
		{Path: "obj/a/.versions/v1", IsLatest: false, ModTime: mod, SuccessorModTime: mod},
		// MPU init under AbortMPU.
		{Path: ".uploads/u1/", IsMPUInit: true, ModTime: mod},
	}

	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback(entries), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}

	gotKinds := map[s3lifecycle.ActionKind]bool{}
	for _, c := range rec.calls {
		gotKinds[c.kind] = true
	}
	for _, want := range []s3lifecycle.ActionKind{
		s3lifecycle.ActionKindExpirationDays,
		s3lifecycle.ActionKindNoncurrentDays,
		s3lifecycle.ActionKindAbortMPU,
	} {
		if !gotKinds[want] {
			t.Fatalf("expected dispatch for %v, got %v", want, rec.calls)
		}
	}
}

func TestWalk_NotYetDueSkipped(t *testing.T) {
	// The reader (Phase 3) is responsible for not-yet-due entries; the
	// walker dispatches only currently-due ones, so the meta-log path
	// stays the steady-state route.
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 30,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10) // before the 30d threshold

	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x/a", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("not-yet-due entry should not dispatch, got %v", rec.calls)
	}
}

func TestWalk_DateActionsSkipped(t *testing.T) {
	// Date kind is handled by its own SCAN_AT_DATE bootstrap, not by the
	// regular bootstrap walker.
	date := mustTime(t, "2025-06-15T00:00:00Z")
	rule := &s3lifecycle.Rule{
		ID:             "d",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDate: date,
	}
	snap := compileEvDriven(t, "bk", rule)

	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x/a", IsLatest: true, ModTime: mustTime(t, "2024-01-01T00:00:00Z")},
	}), rec, WalkOptions{Now: date.AddDate(0, 1, 0)}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("date kind should not dispatch from walker, got %v", rec.calls)
	}
}

func TestWalk_DirectoryEntriesSkipped(t *testing.T) {
	// SeaweedFS directory entries can co-exist in the listing; the walker
	// must not dispatch deletes against them even when their path matches.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x", IsDirectory: true, ModTime: mod}, // directory; must skip
		{Path: "x/file", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 1 || rec.calls[0].path != "x/file" {
		t.Fatalf("only the file should dispatch, got %v", rec.calls)
	}
}

func TestWalk_DisabledModeSkipped(t *testing.T) {
	// An operator-flipped ModeDisabled must short-circuit the walker even
	// when the XML rule status is "Enabled" and EvaluateAction would
	// otherwise fire.
	rule := &s3lifecycle.Rule{ID: "r", Status: s3lifecycle.StatusEnabled, ExpirationDays: 1}
	rh := s3lifecycle.RuleHash(rule)
	prior := map[s3lifecycle.ActionKey]engine.PriorState{
		{Bucket: "bk", RuleHash: rh, ActionKind: s3lifecycle.ActionKindExpirationDays}: {
			BootstrapComplete: true, Mode: engine.ModeDisabled,
		},
	}
	e := engine.New()
	snap := e.Compile([]engine.CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}}, engine.CompileOptions{PriorStates: prior})
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x/a", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("disabled action must not dispatch, got %v", rec.calls)
	}
}

func TestWalk_PendingBootstrapNotDispatched(t *testing.T) {
	// Without bootstrap_complete=true in PriorStates, the engine compiles
	// the action as inactive. MatchPath filters on IsActive, so the
	// walker won't dispatch.
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 1,
	}
	e := engine.New()
	snap := e.Compile([]engine.CompileInput{{Bucket: "bk", Rules: []*s3lifecycle.Rule{rule}}}, engine.CompileOptions{})

	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	rec := &recorder{}
	if _, err := Walk(context.Background(), snap, "bk", EntryCallback([]*Entry{
		{Path: "x/a", IsLatest: true, ModTime: mod},
	}), rec, WalkOptions{Now: now}); err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if len(rec.calls) != 0 {
		t.Fatalf("inactive action should not dispatch, got %v", rec.calls)
	}
}

func TestWalk_DispatchErrorHaltsAtCheckpoint(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 1,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	entries := []*Entry{
		{Path: "a", IsLatest: true, ModTime: mod},
		{Path: "b", IsLatest: true, ModTime: mod},
		{Path: "c", IsLatest: true, ModTime: mod},
	}

	wantErr := errors.New("dispatch boom")
	rec := &recorder{err: wantErr}
	cp, err := Walk(context.Background(), snap, "bk", EntryCallback(entries), rec, WalkOptions{Now: now})
	if !errors.Is(err, wantErr) {
		t.Fatalf("want dispatch error, got %v", err)
	}
	if cp.Completed {
		t.Fatalf("walk should not be Completed on dispatch failure")
	}
	// Walker stops on first failure; checkpoint stays at whatever was
	// recorded BEFORE the failed entry. Path "a" is the failing entry,
	// so LastScannedPath stays at the resume point (empty here).
	if cp.LastScannedPath != "" {
		t.Fatalf("checkpoint should not advance past failing entry, got %q", cp.LastScannedPath)
	}
}

func TestWalk_ResumeFromCheckpoint(t *testing.T) {
	rule := &s3lifecycle.Rule{
		ID:             "r",
		Status:         s3lifecycle.StatusEnabled,
		ExpirationDays: 1,
	}
	snap := compileEvDriven(t, "bk", rule)
	mod := mustTime(t, "2024-01-01T00:00:00Z")
	now := mod.AddDate(0, 0, 10)
	entries := []*Entry{
		{Path: "a", IsLatest: true, ModTime: mod},
		{Path: "b", IsLatest: true, ModTime: mod},
		{Path: "c", IsLatest: true, ModTime: mod},
	}

	rec := &recorder{}
	cp, err := Walk(context.Background(), snap, "bk", EntryCallback(entries), rec, WalkOptions{Now: now, Resume: "b"})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if !cp.Completed {
		t.Fatalf("walk should complete")
	}
	// Only "c" is processed (entries with Path <= "b" are skipped).
	if len(rec.calls) != 1 || rec.calls[0].path != "c" {
		t.Fatalf("Resume should only process c, got %v", rec.calls)
	}
	if cp.LastScannedPath != "c" {
		t.Fatalf("checkpoint want c, got %q", cp.LastScannedPath)
	}
}
