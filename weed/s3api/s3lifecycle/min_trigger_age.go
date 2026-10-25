package s3lifecycle

import "time"

// MinTriggerAge returns the day threshold defined by kind on rule, or 0 if
// the kind has no day-style threshold (date / count / immediate). Callers
// use it as max(MinTriggerAge, kindFloor) when computing safety-scan cadence.
func MinTriggerAge(rule *Rule, kind ActionKind) time.Duration {
	if rule == nil {
		return 0
	}
	switch kind {
	case ActionKindExpirationDays:
		if rule.ExpirationDays > 0 {
			return DaysToDuration(rule.ExpirationDays)
		}
	case ActionKindNoncurrentDays:
		if rule.NoncurrentVersionExpirationDays > 0 {
			return DaysToDuration(rule.NoncurrentVersionExpirationDays)
		}
	case ActionKindAbortMPU:
		if rule.AbortMPUDaysAfterInitiation > 0 {
			return DaysToDuration(rule.AbortMPUDaysAfterInitiation)
		}
	}
	return 0
}
