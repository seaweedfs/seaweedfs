// Package engine compiles per-bucket lifecycle rules into a CompiledAction
// snapshot. One XML <Rule> expands into N compiled actions (one per populated
// action sub-element); each is keyed by ActionKey{bucket, rule_hash,
// action_kind} so sibling actions schedule and degrade independently.
package engine

import (
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

type Engine struct {
	current atomic.Pointer[Snapshot]
}

func New() *Engine {
	e := &Engine{}
	e.current.Store(&Snapshot{actions: map[s3lifecycle.ActionKey]*CompiledAction{}})
	return e
}

func (e *Engine) Snapshot() *Snapshot { return e.current.Load() }

// Snapshot fields are append-only after Compile except CompiledAction.engineState,
// which transitions inactive -> active atomically via markActive.
type Snapshot struct {
	id      uint64
	buckets map[string]*BucketIndex
	actions map[s3lifecycle.ActionKey]*CompiledAction

	allActionsSorted []*CompiledAction

	// Routing indexes hold every ActionKey by mode regardless of activation;
	// dispatch filters on IsActive().
	originalDelayGroups map[time.Duration][]s3lifecycle.ActionKey
	predicateActions    []s3lifecycle.ActionKey
	dateActions         map[s3lifecycle.ActionKey]time.Time
}

func (s *Snapshot) SnapshotID() uint64 { return s.id }

type BucketIndex struct {
	bucket     string
	versioned  bool
	actionKeys []s3lifecycle.ActionKey
}

type CompiledAction struct {
	Rule               *s3lifecycle.Rule
	Bucket             string
	Key                s3lifecycle.ActionKey
	Delay              time.Duration
	PredicateSensitive bool
	Mode               RuleMode
	engineState        atomic.Uint32
}

const (
	engineStateInactive uint32 = 0
	engineStateActive   uint32 = 1
)

func (a *CompiledAction) IsActive() bool {
	return a.engineState.Load() == engineStateActive
}

func (a *CompiledAction) markActive() {
	a.engineState.Store(engineStateActive)
}

// MarkActive mirrors the in-memory hint after a durable bootstrap_complete
// write. No-op if the key isn't in the snapshot.
func (s *Snapshot) MarkActive(key s3lifecycle.ActionKey) {
	if a, ok := s.actions[key]; ok {
		a.markActive()
	}
}

func (s *Snapshot) Action(key s3lifecycle.ActionKey) *CompiledAction {
	return s.actions[key]
}

// AllActions: caller must not mutate.
func (s *Snapshot) AllActions() []*CompiledAction {
	return s.allActionsSorted
}

// OriginalDelayGroups / PredicateActions / DateActions return defensive copies.
func (s *Snapshot) OriginalDelayGroups() map[time.Duration][]s3lifecycle.ActionKey {
	out := make(map[time.Duration][]s3lifecycle.ActionKey, len(s.originalDelayGroups))
	for d, keys := range s.originalDelayGroups {
		copied := make([]s3lifecycle.ActionKey, len(keys))
		copy(copied, keys)
		out[d] = copied
	}
	return out
}

func (s *Snapshot) PredicateActions() []s3lifecycle.ActionKey {
	out := make([]s3lifecycle.ActionKey, len(s.predicateActions))
	copy(out, s.predicateActions)
	return out
}

func (s *Snapshot) DateActions() map[s3lifecycle.ActionKey]time.Time {
	out := make(map[s3lifecycle.ActionKey]time.Time, len(s.dateActions))
	for k, v := range s.dateActions {
		out[k] = v
	}
	return out
}

func (s *Snapshot) BucketVersioned(bucket string) bool {
	if bi, ok := s.buckets[bucket]; ok {
		return bi.versioned
	}
	return false
}

// BucketActionKeys returns a defensive copy of the action keys for bucket,
// or nil if the bucket has no compiled actions in this snapshot.
func (s *Snapshot) BucketActionKeys(bucket string) []s3lifecycle.ActionKey {
	bi, ok := s.buckets[bucket]
	if !ok {
		return nil
	}
	out := make([]s3lifecycle.ActionKey, len(bi.actionKeys))
	copy(out, bi.actionKeys)
	return out
}
