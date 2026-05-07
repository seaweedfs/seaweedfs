package s3lifecycle

import "time"

// MinTriggerAge returns the smallest non-zero day threshold defined by rule's
// actions. Used by the safety-scan cadence as `max(MinTriggerAge, kindFloor)`
// — see the per-kind cadence table in the design doc.
//
// Returns 0 if the rule has no day-based threshold (e.g. a pure
// ExpirationDate rule, a NewerNoncurrentVersions-only rule, or an
// ExpiredObjectDeleteMarker-only rule). Callers handle this by applying the
// per-kind floor unconditionally.
func MinTriggerAge(rule *Rule) time.Duration {
	if rule == nil {
		return 0
	}
	const day = 24 * time.Hour
	var min time.Duration
	consider := func(days int) {
		if days <= 0 {
			return
		}
		d := time.Duration(days) * day
		if min == 0 || d < min {
			min = d
		}
	}
	consider(rule.ExpirationDays)
	consider(rule.NoncurrentVersionExpirationDays)
	consider(rule.AbortMPUDaysAfterInitiation)
	return min
}
