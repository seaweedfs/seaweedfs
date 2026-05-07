package s3lifecycle

import "time"

// MinTriggerAge returns the day threshold defined by `kind` on `rule`. Used by
// the safety-scan cadence as `max(MinTriggerAge(rule, kind), kindFloor)` —
// see the per-kind cadence table in the design doc. Returns 0 when the kind
// has no day-style threshold (date / count / immediate kinds), in which case
// the caller's kind-floor is the cadence directly.
//
// One XML rule may declare multiple actions; this helper takes a kind so the
// cadence is computed independently per compiled action.
func MinTriggerAge(rule *Rule, kind ActionKind) time.Duration {
	if rule == nil {
		return 0
	}
	const day = 24 * time.Hour
	switch kind {
	case ActionKindExpirationDays:
		if rule.ExpirationDays > 0 {
			return time.Duration(rule.ExpirationDays) * day
		}
	case ActionKindNoncurrentDays:
		if rule.NoncurrentVersionExpirationDays > 0 {
			return time.Duration(rule.NoncurrentVersionExpirationDays) * day
		}
	case ActionKindAbortMPU:
		if rule.AbortMPUDaysAfterInitiation > 0 {
			return time.Duration(rule.AbortMPUDaysAfterInitiation) * day
		}
	}
	return 0
}
