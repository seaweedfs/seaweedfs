package engine

import (
	"bytes"
	"sort"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

var snapshotIDSeq atomic.Uint64

// CompileInput: at most one entry per Bucket. Duplicates would overwrite
// earlier entries' BucketIndex.
type CompileInput struct {
	Bucket    string
	Rules     []*s3lifecycle.Rule
	Versioned bool
}

// PriorState carries durable per-action state into Compile. Missing keys
// are treated as bootstrap_complete=false.
type PriorState struct {
	BootstrapComplete bool
	Mode              RuleMode
}

// MetaLogRetention=0 means unbounded; the retention gate doesn't trip.
type CompileOptions struct {
	MetaLogRetention     time.Duration
	BootstrapLookbackMin time.Duration
	PriorStates          map[s3lifecycle.ActionKey]PriorState
}

const defaultBootstrapLookbackMin = 5 * s3lifecycle.SmallDelay

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
				// Durable Mode wins; decideMode is only the seed for new
				// or legacy-zero actions. Otherwise lag-fallback / operator
				// SCAN_ONLY would re-promote on every rebuild.
				prior, hasPrior := opts.PriorStates[key]
				mode := prior.Mode
				if !hasPrior || mode == ModeUnspecified {
					mode = decideMode(rule, kind, opts.MetaLogRetention, opts.BootstrapLookbackMin)
				}
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

				// Index every action regardless of `active`; routing
				// re-filters on IsActive() so MarkActive flips are visible
				// without a recompile.
				if mode == ModeScanAtDate {
					snap.dateActions[key] = rule.ExpirationDate
				}
				if mode == ModeEventDriven {
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
