package dailyrun

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// UnsupportedRuleError is returned by Run when the bucket's compiled
// rules include any action kind Phase 2 cannot service. The handler
// surfaces this so admin marks the run failed in the activity log;
// flipping the algorithm flag to daily_replay on a bucket with these
// rules is a loud failure rather than a silent dropped rule.
type UnsupportedRuleError struct {
	Bucket string
	Kind   s3lifecycle.ActionKind
	Reason string
}

func (e *UnsupportedRuleError) Error() string {
	return fmt.Sprintf("daily_replay: unsupported action kind %s on bucket %q: %s", e.Kind, e.Bucket, e.Reason)
}

// IsUnsupportedRule reports whether err is or wraps an
// UnsupportedRuleError. Callers (the worker handler) use this to
// classify the run outcome.
func IsUnsupportedRule(err error) bool {
	var u *UnsupportedRuleError
	return errors.As(err, &u)
}

// isReplayEligibleKind reports whether the engine's daily-replay path
// can service action kind k. Mirror this list when adding new kinds.
func isReplayEligibleKind(k s3lifecycle.ActionKind) bool {
	switch k {
	case s3lifecycle.ActionKindExpirationDays,
		s3lifecycle.ActionKindNoncurrentDays,
		s3lifecycle.ActionKindAbortMPU:
		return true
	}
	return false
}

// checkSnapshotForUnsupported walks every active CompiledAction in snap
// and returns the first kind that isn't replay-eligible. Phase 2 uses
// this gate before invoking the replay loop. Phase 4 partitions these
// into walk-bound actions and runs them through the walker, removing
// the gate.
func checkSnapshotForUnsupported(snap *engine.Snapshot) *UnsupportedRuleError {
	if snap == nil {
		return nil
	}
	for _, a := range snap.AllActions() {
		if a == nil || !a.IsActive() {
			continue
		}
		if isReplayEligibleKind(a.Key.ActionKind) {
			continue
		}
		return &UnsupportedRuleError{
			Bucket: a.Bucket,
			Kind:   a.Key.ActionKind,
			Reason: "Phase 2 only routes ExpirationDays / NoncurrentDays / AbortMPU; ExpirationDate, ExpiredDeleteMarker, NewerNoncurrent, and scan_only land in Phase 4",
		}
	}
	return nil
}

// localReplayContentHash hashes the rule definitions of replay-eligible
// actions in snap. Stable across reorderings — actions are sorted by
// (bucket, rule_hash, action_kind) before mixing. Phase 4 replaces
// this with engine.ReplayContentHash; both must produce the same value
// on a snapshot that's already fully replay-eligible (which Phase 2
// enforces via checkSnapshotForUnsupported).
//
// Includes the effective TTL in the hash so a TTL change (e.g. 30 → 60
// days) is detected as a rule-content change, even though the rule's
// RuleHash also captures it — defense in depth against any future
// RuleHash collision and an explicit dependency for the cursor
// rewind decision.
func localReplayContentHash(snap *engine.Snapshot) [32]byte {
	if snap == nil {
		return [32]byte{}
	}
	type rec struct {
		bucket     string
		ruleHash   [8]byte
		actionKind s3lifecycle.ActionKind
		ttlNs      int64
	}
	var recs []rec
	for _, a := range snap.AllActions() {
		if a == nil || !a.IsActive() {
			continue
		}
		if !isReplayEligibleKind(a.Key.ActionKind) {
			continue
		}
		recs = append(recs, rec{
			bucket:     a.Bucket,
			ruleHash:   a.Key.RuleHash,
			actionKind: a.Key.ActionKind,
			ttlNs:      int64(effectiveTTL(a)),
		})
	}
	if len(recs) == 0 {
		return [32]byte{}
	}
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].bucket != recs[j].bucket {
			return recs[i].bucket < recs[j].bucket
		}
		if recs[i].ruleHash != recs[j].ruleHash {
			for k := 0; k < len(recs[i].ruleHash); k++ {
				if recs[i].ruleHash[k] != recs[j].ruleHash[k] {
					return recs[i].ruleHash[k] < recs[j].ruleHash[k]
				}
			}
		}
		return int(recs[i].actionKind) < int(recs[j].actionKind)
	})
	h := sha256.New()
	var scratch [16]byte
	for _, r := range recs {
		h.Write([]byte(r.bucket))
		h.Write([]byte{0})
		h.Write(r.ruleHash[:])
		binary.LittleEndian.PutUint64(scratch[:8], uint64(r.actionKind))
		binary.LittleEndian.PutUint64(scratch[8:], uint64(r.ttlNs))
		h.Write(scratch[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// effectiveTTL returns the per-action TTL the lifecycle engine clocks
// against. Walker-bound kinds (ExpirationDate, ExpiredDeleteMarker,
// NewerNoncurrent) return 0 — they don't participate in the replay
// sliding window so they don't contribute to MaxEffectiveTTL or the
// content hash. Phase 2 guarantees those kinds never reach here via
// checkSnapshotForUnsupported, but the switch stays exhaustive so a
// future kind addition is a compile-time prompt to decide.
func effectiveTTL(a *engine.CompiledAction) time.Duration {
	if a == nil || a.Rule == nil {
		return 0
	}
	switch a.Key.ActionKind {
	case s3lifecycle.ActionKindExpirationDays:
		return s3lifecycle.DaysToDuration(a.Rule.ExpirationDays)
	case s3lifecycle.ActionKindNoncurrentDays:
		return s3lifecycle.DaysToDuration(a.Rule.NoncurrentVersionExpirationDays)
	case s3lifecycle.ActionKindAbortMPU:
		return s3lifecycle.DaysToDuration(a.Rule.AbortMPUDaysAfterInitiation)
	}
	return 0
}

// localMaxEffectiveTTL returns the largest effective TTL across active
// replay-eligible actions. Returns zero when snap is empty/nil — caller
// is responsible for routing through the empty-replay branch in that
// case. Phase 4 replaces this with engine.MaxEffectiveTTL.
func localMaxEffectiveTTL(snap *engine.Snapshot) time.Duration {
	if snap == nil {
		return 0
	}
	var max time.Duration
	for _, a := range snap.AllActions() {
		if a == nil || !a.IsActive() {
			continue
		}
		if !isReplayEligibleKind(a.Key.ActionKind) {
			continue
		}
		if d := effectiveTTL(a); d > max {
			max = d
		}
	}
	return max
}
