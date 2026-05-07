package s3lifecycle

import "time"

// EventLogHorizon returns the maximum age of an event the reader needs to be
// able to observe to drive rule end-to-end. Used by the retention mode gate:
// if metaLogRetention < EventLogHorizon(rule) + bootstrapLookbackMin, the
// rule is promoted to scan_only with degraded_reason=RETENTION_BELOW_HORIZON.
//
// Per-kind values:
//
//	ExpirationDays                    -> rule.ExpirationDays
//	NoncurrentVersionExpirationDays   -> rule.NoncurrentVersionExpirationDays
//	AbortMPUDaysAfterInitiation       -> rule.AbortMPUDaysAfterInitiation
//	NewerNoncurrentVersions (count)   -> SmallDelay
//	ExpiredObjectDeleteMarker         -> SmallDelay
//	ExpirationDate                    -> 0 (date rules bypass this gate)
//
// For multi-action rules, returns the maximum across all reader-driven
// aspects so the gate applies the strictest horizon.
func EventLogHorizon(rule *Rule) time.Duration {
	if rule == nil {
		return 0
	}
	const day = 24 * time.Hour
	var max time.Duration
	bump := func(d time.Duration) {
		if d > max {
			max = d
		}
	}
	if rule.ExpirationDays > 0 {
		bump(time.Duration(rule.ExpirationDays) * day)
	}
	if rule.NoncurrentVersionExpirationDays > 0 {
		bump(time.Duration(rule.NoncurrentVersionExpirationDays) * day)
	}
	if rule.AbortMPUDaysAfterInitiation > 0 {
		bump(time.Duration(rule.AbortMPUDaysAfterInitiation) * day)
	}
	if rule.NewerNoncurrentVersions > 0 && rule.NoncurrentVersionExpirationDays == 0 {
		bump(SmallDelay)
	}
	if rule.ExpiredObjectDeleteMarker {
		bump(SmallDelay)
	}
	return max
}
