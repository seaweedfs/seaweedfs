package dailyrun

import (
	"errors"
	"fmt"

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
// and returns the first kind that isn't replay-eligible OR is in a Mode
// other than ModeEventDriven. router.Route gates dispatch on
// `Mode == ModeEventDriven` (see weed/s3api/s3lifecycle/router/router.go),
// so a replay-kind action that's been promoted to ModeScanOnly would
// silently get no matches at all — the daily run must reject it loudly
// so admin sees the failure in the activity log. Phase 4 partitions
// these into walk-bound actions and runs them through the walker,
// removing the gate.
func checkSnapshotForUnsupported(snap *engine.Snapshot) *UnsupportedRuleError {
	if snap == nil {
		return nil
	}
	for _, a := range snap.AllActions() {
		if a == nil || !a.IsActive() {
			continue
		}
		if !isReplayEligibleKind(a.Key.ActionKind) {
			return &UnsupportedRuleError{
				Bucket: a.Bucket,
				Kind:   a.Key.ActionKind,
				Reason: "Phase 2 only routes ExpirationDays / NoncurrentDays / AbortMPU; ExpirationDate, ExpiredDeleteMarker, NewerNoncurrent land in Phase 4",
			}
		}
		if a.Mode != engine.ModeEventDriven {
			return &UnsupportedRuleError{
				Bucket: a.Bucket,
				Kind:   a.Key.ActionKind,
				Reason: fmt.Sprintf("action is in Mode=%v (router.Route only dispatches ModeEventDriven); scan_only promotions land in Phase 4", a.Mode),
			}
		}
	}
	return nil
}
