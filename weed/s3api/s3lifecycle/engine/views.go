package engine

import (
	"bytes"
	"sort"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// currentEngine is the package-level pointer the wiring will eventually
// populate so daily_run can fetch the live snapshot through CurrentSnapshot.
// Today the worker/scheduler owns its own *Engine and passes the snapshot
// explicitly to its consumers; nothing calls SetCurrentEngine yet. Phase 4's
// daily-replay caller will register the worker's engine here on startup so
// the package-level helper resolves without threading the *Engine through
// every layer.
//
// TODO(phase4-wiring): hook scheduler/worker startup to call SetCurrentEngine.
var currentEngine atomic.Pointer[Engine]

// SetCurrentEngine installs e as the package-level engine returned by
// CurrentSnapshot. Intended for the worker bootstrap path; tests may also
// call it with a locally-constructed engine. Passing nil clears the pointer.
func SetCurrentEngine(e *Engine) { currentEngine.Store(e) }

// CurrentSnapshot returns the latest *Snapshot from the package-level engine,
// or nil if no engine has been registered yet. The caller is responsible for
// nil-checking the return — a missing engine is treated as "no rules" so the
// daily-run loop short-circuits cleanly instead of panicking during early
// startup.
//
// TODO(phase4-wiring): SetCurrentEngine must be invoked at worker startup
// for production callers; until then this returns nil.
func CurrentSnapshot() *Snapshot {
	e := currentEngine.Load()
	if e == nil {
		return nil
	}
	return e.Snapshot()
}

// cloneAction makes an independent *CompiledAction for a per-view snapshot.
// The clone shares Rule (immutable) and Key (value) with the base, but its
// engineState (the active bit) is its own atomic — so flipping active on the
// clone never reaches the base or any sibling view. Mode is set explicitly by
// the caller to encode the per-view dispatch contract (replay forces
// ModeEventDriven so router.Route's gate passes; walk and recovery preserve
// the base Mode so the walker's "any non-Disabled" gate works as-is).
func cloneAction(src *CompiledAction, mode RuleMode, active bool) *CompiledAction {
	if src == nil {
		return nil
	}
	dst := &CompiledAction{
		Rule:               src.Rule,
		Bucket:             src.Bucket,
		Key:                src.Key,
		Delay:              src.Delay,
		PredicateSensitive: src.PredicateSensitive,
		Mode:               mode,
	}
	if active {
		dst.markActive()
	}
	return dst
}

// newView constructs a *Snapshot that holds the supplied per-view action
// clones but otherwise shares the base's immutable rule / index data by
// pointer (see DESIGN.md "router.Route integration" — only `active` and
// `Mode` differ per view; everything else is share-by-pointer).
//
// Returns nil if actions is empty: callers consume nil as "this partition
// has no actions, skip the corresponding dispatch path."
func newView(base *Snapshot, actions map[s3lifecycle.ActionKey]*CompiledAction) *Snapshot {
	if len(actions) == 0 {
		return nil
	}
	// allActionsSorted mirrors the base order: same comparator (Bucket,
	// RuleHash, ActionKind) so downstream iteration order is stable per view.
	sorted := make([]*CompiledAction, 0, len(actions))
	for _, a := range actions {
		sorted = append(sorted, a)
	}
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if a.Bucket != b.Bucket {
			return a.Bucket < b.Bucket
		}
		if c := bytes.Compare(a.Key.RuleHash[:], b.Key.RuleHash[:]); c != 0 {
			return c < 0
		}
		return a.Key.ActionKind < b.Key.ActionKind
	})
	return &Snapshot{
		id:                  base.id,
		buckets:             base.buckets,
		actions:             actions,
		allActionsSorted:    sorted,
		originalDelayGroups: base.originalDelayGroups,
		predicateActions:    base.predicateActions,
		dateActions:         base.dateActions,
	}
}

// isReplayKind reports whether ActionKind is a candidate for the replay
// partition: its trigger derives from a single event's TsNs (or a stamped
// noncurrent_since), so the meta-log scan can drive dispatch directly.
// ExpirationDate / ExpiredDeleteMarker / NewerNoncurrent are walker-only
// because their due time depends on current bucket state, not event age.
func isReplayKind(k s3lifecycle.ActionKind) bool {
	switch k {
	case s3lifecycle.ActionKindExpirationDays,
		s3lifecycle.ActionKindNoncurrentDays,
		s3lifecycle.ActionKindAbortMPU:
		return true
	}
	return false
}

// effectiveTTL is the duration used both for partition membership
// (TTL ≤ retentionWindow → replay) and MaxEffectiveTTL math (driving the
// cursor's sliding scan window). Walker-only kinds return 0 so they never
// contribute to MaxEffectiveTTL and never satisfy the partition gate when
// retentionWindow is finite.
func effectiveTTL(a *CompiledAction) time.Duration {
	if a == nil || a.Rule == nil {
		return 0
	}
	switch a.Key.ActionKind {
	case s3lifecycle.ActionKindExpirationDays:
		if a.Rule.ExpirationDays > 0 {
			return s3lifecycle.DaysToDuration(a.Rule.ExpirationDays)
		}
	case s3lifecycle.ActionKindNoncurrentDays:
		if a.Rule.NoncurrentVersionExpirationDays > 0 {
			return s3lifecycle.DaysToDuration(a.Rule.NoncurrentVersionExpirationDays)
		}
	case s3lifecycle.ActionKindAbortMPU:
		if a.Rule.AbortMPUDaysAfterInitiation > 0 {
			return s3lifecycle.DaysToDuration(a.Rule.AbortMPUDaysAfterInitiation)
		}
	}
	return 0
}

// RulesForShard partitions the base snapshot's compiled actions into a
// replay view and a walk view.
//
// shardID is **reserved for forward-compatibility** and not consumed by
// this implementation. Every shard sees every rule today because the
// shard filter runs at the entry-iteration site (meta-log subscription
// per shard, walker entry-loop ShardID check) rather than at view
// construction. Keeping the parameter in the signature preserves the
// API surface for a future move to per-shard rule sets (e.g. when
// bucket-shard ownership is added) — removing it now would be a
// breaking change at that future point. See DESIGN.md "open questions
// → per-shard rule snapshots vs. global."
//
// retentionWindow is the only window input the engine sees; the
// daily_run caller computes it as `now - earliest_available` and
// passes it in so the partition is stable across one daily_run
// invocation.
//
// Membership:
//   - replay: clones of ExpirationDays / NoncurrentDays / AbortMPU actions
//     whose TTL ≤ retentionWindow. active=true, Mode forced to
//     ModeEventDriven (rehabilitating any prior ModeScanOnly lock-in so
//     router.Route's gate accepts the clone).
//   - walk: clones of ExpirationDate / ExpiredDeleteMarker / NewerNoncurrent
//     actions, plus any replay-eligible action whose TTL exceeds
//     retentionWindow (the "scan_only promotion" path). active=true, Mode
//     preserved from the base — the walker only rejects ModeDisabled.
//
// Either return value may be nil when its partition has no actions.
// Disabled actions (Mode == ModeDisabled) are excluded from both views.
func (s *Snapshot) RulesForShard(shardID int, retentionWindow time.Duration) (replay, walk *Snapshot) {
	_ = shardID
	if s == nil {
		return nil, nil
	}
	replayActions := map[s3lifecycle.ActionKey]*CompiledAction{}
	walkActions := map[s3lifecycle.ActionKey]*CompiledAction{}
	for key, a := range s.actions {
		if a == nil || a.Mode == ModeDisabled {
			continue
		}
		if isReplayKind(key.ActionKind) {
			ttl := effectiveTTL(a)
			// retentionWindow == 0 means "no retention info supplied" — the
			// caller should treat the partition as walk-only to stay safe,
			// since promoting every rule into replay without retention
			// proof would defeat the scan_only protection. ttl == 0 is a
			// malformed rule (kind says replay but no TTL): also routes to
			// walk so the walker can decide.
			if ttl > 0 && ttl <= retentionWindow {
				replayActions[key] = cloneAction(a, ModeEventDriven, true)
			} else {
				walkActions[key] = cloneAction(a, a.Mode, true)
			}
			continue
		}
		// Walker-only kinds: preserve Mode (ModeScanAtDate for
		// ExpirationDate, ModeEventDriven for the others) so the walker's
		// per-rule evaluator runs as today.
		walkActions[key] = cloneAction(a, a.Mode, true)
	}
	return newView(s, replayActions), newView(s, walkActions)
}

// RecoveryView returns a *Snapshot that contains a clone of every action in
// the base, with active=true regardless of compile-time IsActive/
// BootstrapComplete and Mode preserved as-is. The recovery walker uses this
// view to catch already-due objects across the full rule set on cold start,
// rule edit, retention loss, or partition flip. Disabled actions stay out —
// recovery shouldn't resurrect an explicitly disabled rule.
//
// Unlike RulesForShard, RecoveryView takes no retentionWindow: every
// non-disabled action is activated, partition-independent, because recovery
// must reach rules whose subjects sit outside the replay scan window.
func RecoveryView(s *Snapshot) *Snapshot {
	if s == nil {
		return nil
	}
	actions := map[s3lifecycle.ActionKey]*CompiledAction{}
	for key, a := range s.actions {
		if a == nil || a.Mode == ModeDisabled {
			continue
		}
		actions[key] = cloneAction(a, a.Mode, true)
	}
	return newView(s, actions)
}
