package dailyrun

import (
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// UnsupportedRuleError fails the run loudly when the snapshot contains
// a rule the Phase 2 replay path can't service. Surfaced verbatim to
// the activity log so flipping algorithm=daily_replay on an
// incompatible bucket isn't a silent dropped rule.
type UnsupportedRuleError struct {
	Bucket string
	Kind   s3lifecycle.ActionKind
	Reason string
}

func (e *UnsupportedRuleError) Error() string {
	return fmt.Sprintf("daily_replay: unsupported action kind %s on bucket %q: %s", e.Kind, e.Bucket, e.Reason)
}

func IsUnsupportedRule(err error) bool {
	var u *UnsupportedRuleError
	return errors.As(err, &u)
}

func isReplayEligibleKind(k s3lifecycle.ActionKind) bool {
	switch k {
	case s3lifecycle.ActionKindExpirationDays,
		s3lifecycle.ActionKindNoncurrentDays,
		s3lifecycle.ActionKindAbortMPU:
		return true
	}
	return false
}

// checkSnapshotForUnsupported rejects (a) walker-bound action kinds and
// (b) replay-kind actions in any Mode other than ModeEventDriven.
// router.Route silently drops non-ModeEventDriven actions; rejecting
// them here turns the silent drop into a loud failure. Phase 4
// partitions these into walk-bound actions and removes the gate.
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
