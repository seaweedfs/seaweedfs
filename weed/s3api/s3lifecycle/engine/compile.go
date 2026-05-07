package engine

import (
	"bytes"
	"sort"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// snapshotIDSeq is the cluster-wide monotonic ID stamped on every Compile.
// One process-wide counter is sufficient: callers that need cross-process
// monotonicity stamp pending writes with the snapshot id when they record
// the entry; the entry stays valid as long as its ActionKey survives, not
// as long as the snapshot id matches.
var snapshotIDSeq atomic.Uint64

// CompileInput is one (bucket, rule) pair the engine compiles. The caller
// converts XML lifecycle config to []*s3lifecycle.Rule via
// s3api.LifecycleToCanonical and groups by bucket.
type CompileInput struct {
	Bucket    string
	Rules     []*s3lifecycle.Rule
	Versioned bool // true if the bucket has versioning enabled
}

// PriorState is the durable per-action state the compiler reads to decide
// whether a freshly-compiled action is bootstrap_complete. Missing keys are
// treated as bootstrap_complete=false (new action, must run bootstrap).
type PriorState struct {
	BootstrapComplete bool
	Mode              RuleMode
}

// CompileOptions tunes engine compilation. All fields have safe zero values.
type CompileOptions struct {
	// MetaLogRetention is the upper bound on how far back the meta-log
	// reader can replay events. Used by the retention mode gate to
	// downgrade actions whose event-log horizon exceeds retention.
	// A zero value means "unbounded retention" — the gate never trips
	// (matches the SeaweedFS default deployment per Phase 0 verification).
	MetaLogRetention time.Duration

	// BootstrapLookbackMin is the safety floor added to the retention gate:
	// metaLogRetention < eventLogHorizon(rule, kind) + BootstrapLookbackMin
	// promotes the action to scan_only. Default 5 * SmallDelay (~5 minutes).
	BootstrapLookbackMin time.Duration

	// PriorStates is the durable state map keyed by ActionKey. Used at
	// compile time to decide engine activation per action.
	PriorStates map[s3lifecycle.ActionKey]PriorState
}

// defaultBootstrapLookbackMin is the minimum extra slack the retention gate
// requires beyond the rule's eventLogHorizon. Five SmallDelays gives a few
// minutes for the reader to drain residual events at edge cases without
// promoting an otherwise-healthy rule to scan_only.
const defaultBootstrapLookbackMin = 5 * s3lifecycle.SmallDelay

// Compile produces a fresh Snapshot from per-bucket rules and applies it to
// the engine. Returns the new Snapshot.
//
// Each input rule expands into N CompiledActions via RuleActionKinds. Each
// CompiledAction's mode is decided by decideMode (date kind -> SCAN_AT_DATE;
// reader-driven kind whose horizon exceeds retention -> SCAN_ONLY; disabled
// rule -> DISABLED; otherwise EVENT_DRIVEN provided bootstrap is complete).
// Activation (engineState=active) requires both bootstrap_complete from
// PriorStates and mode==EVENT_DRIVEN.
func (e *Engine) Compile(inputs []CompileInput, opts CompileOptions) *Snapshot {
	if opts.BootstrapLookbackMin == 0 {
		opts.BootstrapLookbackMin = defaultBootstrapLookbackMin
	}

	snap := &Snapshot{
		id:                  snapshotIDSeq.Add(1),
		buckets:             make(map[string]*BucketIndex),
		actions:             make(map[s3lifecycle.ActionKey]*CompiledAction),
		originalDelayGroups: make(map[time.Duration][]s3lifecycle.ActionKey),
		dateActions:         make(map[s3lifecycle.ActionKey]time.Time),
	}

	for _, in := range inputs {
		bi := &BucketIndex{bucket: in.Bucket, versioned: in.Versioned}
		snap.buckets[in.Bucket] = bi

		for _, rule := range in.Rules {
			ruleHash := s3lifecycle.RuleHash(rule)
			for _, kind := range s3lifecycle.RuleActionKinds(rule) {
				key := s3lifecycle.ActionKey{Bucket: in.Bucket, RuleHash: ruleHash, ActionKind: kind}
				mode := decideMode(rule, kind, opts.MetaLogRetention, opts.BootstrapLookbackMin)
				prior := opts.PriorStates[key]
				active := prior.BootstrapComplete && mode == ModeEventDriven

				ca := &CompiledAction{
					Rule:               rule,
					Bucket:             in.Bucket,
					Key:                key,
					Delay:              s3lifecycle.MinTriggerAge(rule, kind),
					PredicateSensitive: rulePredicateSensitive(rule),
					Mode:               mode,
				}
				if active {
					ca.markActive()
				}
				snap.actions[key] = ca
				bi.actionKeys = append(bi.actionKeys, key)

				// Routing indexes by mode:
				// - SCAN_AT_DATE: always indexed in dateActions so the
				//   detector can schedule the bootstrap at rule.date.
				//   Active flag is unused for this stream (no reader sweep).
				// - EVENT_DRIVEN + active: indexed in originalDelayGroups
				//   (and predicateActions when applicable) so the reader
				//   sweeps it. The reader filters again on engineState
				//   before dispatching, so subsequent markActive flips are
				//   visible without recompile.
				// - SCAN_ONLY / DISABLED / pending_bootstrap: not indexed;
				//   handled by safety-scan tick or explicit operator action.
				if mode == ModeScanAtDate {
					snap.dateActions[key] = rule.ExpirationDate
				}
				// mode==EVENT_DRIVEN already excludes EXPIRATION_DATE
				// (decideMode routes the date kind to SCAN_AT_DATE); no
				// extra kind check needed.
				if active && mode == ModeEventDriven {
					snap.originalDelayGroups[ca.Delay] = append(snap.originalDelayGroups[ca.Delay], key)
					if ca.PredicateSensitive {
						snap.predicateActions = append(snap.predicateActions, key)
					}
				}
			}
		}
	}

	// Build the pre-sorted AllActions view once. Snapshot is immutable
	// post-Compile (engineState transitions don't affect membership), so
	// the sorted slice can be shared by every AllActions() call.
	snap.allActionsSorted = make([]*CompiledAction, 0, len(snap.actions))
	for _, a := range snap.actions {
		snap.allActionsSorted = append(snap.allActionsSorted, a)
	}
	sort.Slice(snap.allActionsSorted, func(i, j int) bool {
		a, b := snap.allActionsSorted[i], snap.allActionsSorted[j]
		if a.Bucket != b.Bucket {
			return a.Bucket < b.Bucket
		}
		if c := bytes.Compare(a.Key.RuleHash[:], b.Key.RuleHash[:]); c != 0 {
			return c < 0
		}
		return a.Key.ActionKind < b.Key.ActionKind
	})

	e.current.Store(snap)
	return snap
}

// rulePredicateSensitive returns true if the rule's filter has any predicate
// that can flip post-PUT. Only tags qualify: prefix is fixed by the object's
// path at write time, and an object's size is immutable once written (any
// content change is a fresh write that flows through the original-write
// stream). Size filters do not need predicate-change sweeps; including them
// here would add to predicateActions for no purpose and waste sweep cycles.
func rulePredicateSensitive(rule *s3lifecycle.Rule) bool {
	if rule == nil {
		return false
	}
	return len(rule.FilterTags) > 0
}
