// Package engine compiles per-bucket lifecycle rules into a CompiledAction
// snapshot the worker drives. One XML <Rule> expands into N compiled actions
// (one per populated action sub-element); each is keyed by
// s3lifecycle.ActionKey{rule_hash, action_kind} and carries its own delay
// group, mode, and predicate-sensitivity flag.
//
// The engine itself is a small wrapper around an immutable Snapshot pointer
// that callers atomically swap when policy changes. A rebuild produces a
// fresh Snapshot and a new snapshot_id; existing readers continue to see
// the previous snapshot for the duration of their evaluation pass.
package engine

import (
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// Engine is the runtime handle. The current Snapshot is loaded atomically;
// callers grab a pointer and use it for the rest of their evaluation pass.
type Engine struct {
	current atomic.Pointer[Snapshot]
}

// New returns an empty engine; callers must Compile before use.
func New() *Engine {
	e := &Engine{}
	e.current.Store(&Snapshot{actions: map[s3lifecycle.ActionKey]*CompiledAction{}})
	return e
}

// Snapshot returns the currently active snapshot.
func (e *Engine) Snapshot() *Snapshot { return e.current.Load() }

// Snapshot is an immutable view of the compiled engine state.
//
// Snapshot fields are append-only after construction except for
// CompiledAction.engineState, which transitions inactive -> active on
// markActive. The transition is atomic per action and visible to subsequent
// reader passes; existing in-flight passes see the value as of their initial
// load.
type Snapshot struct {
	id      uint64
	buckets map[string]*BucketIndex
	actions map[s3lifecycle.ActionKey]*CompiledAction

	// Pre-sorted view of `actions` in (bucket, rule_hash, action_kind)
	// order. Built once during Compile so AllActions() doesn't re-sort
	// on every call. The slice is immutable; callers must not mutate.
	allActionsSorted []*CompiledAction

	// Routing indexes (subset of `actions`). Only ActionKeys whose
	// engineState == EngineStateActive may be added here at compile time;
	// the reader filters again on engineState before invoking handlers, so
	// markActive transitions become visible as soon as the engine flips
	// the bit (no recompile needed for steady-state activation).
	originalDelayGroups map[time.Duration][]s3lifecycle.ActionKey
	predicateActions    []s3lifecycle.ActionKey
	dateActions         map[s3lifecycle.ActionKey]time.Time
}

// SnapshotID is the monotonic identifier bumped on every Compile. Worker
// callers stamp pending writes with this id; a pending entry whose
// snapshot_id is older than the engine's current id is still valid as long
// as its ActionKey survives in the new snapshot.
func (s *Snapshot) SnapshotID() uint64 { return s.id }

// BucketIndex carries per-bucket routing data. The optional prefixTrie /
// tagIndex are present in the design; this implementation starts with a
// linear scan over the bucket's ActionKeys and adds the optimizations as
// the engine actually carries enough rules to need them.
type BucketIndex struct {
	bucket     string
	versioned  bool
	actionKeys []s3lifecycle.ActionKey
}

// CompiledAction is one (rule, action_kind) pair the engine evaluates. A
// single XML <Rule> with N populated action sub-elements compiles to N
// CompiledActions sharing the same Rule and rule_hash but differing in
// ActionKind, Delay, Mode, and engineState.
type CompiledAction struct {
	Rule               *s3lifecycle.Rule // shared with sibling actions of the same XML rule
	Bucket             string
	Key                s3lifecycle.ActionKey // {rule_hash, action_kind}
	Delay              time.Duration         // per-kind delay group; 0 for date / count / immediate kinds
	PredicateSensitive bool                  // true when the rule's filter has tag/size predicates
	Mode               RuleMode              // mirrored from durable state at compile time
	engineState        atomic.Uint32         // EngineStateActive / EngineStateInactive
}

// EngineState atom values. Held as uint32 so atomic ops work without
// requiring a wrapper type.
const (
	engineStateInactive uint32 = 0
	engineStateActive   uint32 = 1
)

// IsActive returns true if this CompiledAction has been markActive'd by
// bootstrap completion (or compiled with an already-bootstrap_complete
// state). The reader/router must filter on this before dispatching.
func (a *CompiledAction) IsActive() bool {
	return a.engineState.Load() == engineStateActive
}

// markActive flips this action to active. Idempotent and safe to call
// concurrently with reader passes.
func (a *CompiledAction) markActive() {
	a.engineState.Store(engineStateActive)
}

// MarkActive transitions an ActionKey to active, mirroring the in-memory
// hint after the durable bootstrap_complete + mode write commits. No-op if
// the key isn't in the snapshot (e.g. a stale callback from a prior
// snapshot).
func (s *Snapshot) MarkActive(key s3lifecycle.ActionKey) {
	if a, ok := s.actions[key]; ok {
		a.markActive()
	}
}

// Action returns the CompiledAction for a key, or nil.
func (s *Snapshot) Action(key s3lifecycle.ActionKey) *CompiledAction {
	return s.actions[key]
}

// AllActions returns every CompiledAction in deterministic order (by bucket,
// rule_hash, action_kind). Used by the bootstrap walker, which evaluates
// every applicable action per object. The returned slice is the snapshot's
// pre-sorted view; callers must not mutate it.
func (s *Snapshot) AllActions() []*CompiledAction {
	return s.allActionsSorted
}

// OriginalDelayGroups returns the delay -> ActionKey mapping the reader
// uses to schedule one sweep per delay group across all event-driven
// age-origin actions. Returns a defensive copy: the snapshot's internal
// indexes are immutable per the contract above; callers may freely
// mutate the returned map / slices without affecting other readers.
func (s *Snapshot) OriginalDelayGroups() map[time.Duration][]s3lifecycle.ActionKey {
	out := make(map[time.Duration][]s3lifecycle.ActionKey, len(s.originalDelayGroups))
	for d, keys := range s.originalDelayGroups {
		copied := make([]s3lifecycle.ActionKey, len(keys))
		copy(copied, keys)
		out[d] = copied
	}
	return out
}

// PredicateActions returns ActionKeys with tag/size predicate sensitivity.
// The reader sweeps these once per pass against predicate-change events.
// Defensive copy — see OriginalDelayGroups.
func (s *Snapshot) PredicateActions() []s3lifecycle.ActionKey {
	out := make([]s3lifecycle.ActionKey, len(s.predicateActions))
	copy(out, s.predicateActions)
	return out
}

// DateActions returns ActionKey -> date for SCAN_AT_DATE actions. The
// detector schedules a single bootstrap at each date. Defensive copy —
// see OriginalDelayGroups.
func (s *Snapshot) DateActions() map[s3lifecycle.ActionKey]time.Time {
	out := make(map[s3lifecycle.ActionKey]time.Time, len(s.dateActions))
	for k, v := range s.dateActions {
		out[k] = v
	}
	return out
}

// BucketVersioned reports whether the bucket is configured with versioning
// (set at compile time from the bucket index).
func (s *Snapshot) BucketVersioned(bucket string) bool {
	if bi, ok := s.buckets[bucket]; ok {
		return bi.versioned
	}
	return false
}
