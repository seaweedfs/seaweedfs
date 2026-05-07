package engine

import (
	"bytes"
	"sort"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

var snapshotIDSeq atomic.Uint64

type CompileInput struct {
	Bucket    string
	Rules     []*s3lifecycle.Rule
	Versioned bool
}

// PriorState carries durable per-action state into Compile. Missing keys are
// treated as bootstrap_complete=false (new action, must run bootstrap).
type PriorState struct {
	BootstrapComplete bool
	Mode              RuleMode
}

type CompileOptions struct {
	// MetaLogRetention=0 means unbounded (the SeaweedFS default; meta-log
	// files are written without TTL). The retention gate doesn't trip then.
	MetaLogRetention     time.Duration
	BootstrapLookbackMin time.Duration
	PriorStates          map[s3lifecycle.ActionKey]PriorState
}

const defaultBootstrapLookbackMin = 5 * s3lifecycle.SmallDelay

// Compile builds a fresh Snapshot, atomically swaps it onto the engine, and
// returns it. Each input rule expands into N CompiledActions via
// RuleActionKinds; activation requires bootstrap_complete from PriorStates
// and mode==EVENT_DRIVEN.
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

				// SCAN_AT_DATE actions index regardless of activation; the
				// detector schedules them at rule.date. Other modes are
				// only indexed for routing once active.
				if mode == ModeScanAtDate {
					snap.dateActions[key] = rule.ExpirationDate
				}
				if active && mode == ModeEventDriven {
					snap.originalDelayGroups[ca.Delay] = append(snap.originalDelayGroups[ca.Delay], key)
					if ca.PredicateSensitive {
						snap.predicateActions = append(snap.predicateActions, key)
					}
				}
			}
		}
	}

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

// rulePredicateSensitive: only tag filters can flip post-PUT. Size is
// immutable once written; a size change is a fresh write through the
// original-write stream.
func rulePredicateSensitive(rule *s3lifecycle.Rule) bool {
	if rule == nil {
		return false
	}
	return len(rule.FilterTags) > 0
}
