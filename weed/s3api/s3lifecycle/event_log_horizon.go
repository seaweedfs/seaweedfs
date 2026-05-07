package s3lifecycle

import "time"

// EventLogHorizon returns the maximum age of an event the reader needs to
// observe to drive the (rule, kind) compiled action. Used by the retention
// mode gate: if metaLogRetention < EventLogHorizon(rule, kind) +
// bootstrapLookbackMin, this action is promoted to scan_only with
// degraded_reason=RETENTION_BELOW_HORIZON.
//
// One XML rule may declare multiple actions with different horizons (a 90d
// EXPIRATION_DAYS sibling alongside a 7d ABORT_MPU); the gate runs per
// compiled action so each can degrade independently.
//
// Per-kind values:
//
//	EXPIRATION_DAYS         -> rule.ExpirationDays
//	NONCURRENT_DAYS         -> rule.NoncurrentVersionExpirationDays
//	ABORT_MPU               -> rule.AbortMPUDaysAfterInitiation
//	NEWER_NONCURRENT        -> SmallDelay (count-only retention; immediate at flip)
//	EXPIRED_DELETE_MARKER   -> SmallDelay (immediate when sole survivor)
//	EXPIRATION_DATE         -> 0 (date kind bypasses the gate)
func EventLogHorizon(rule *Rule, kind ActionKind) time.Duration {
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
	case ActionKindNewerNoncurrent:
		if rule.NewerNoncurrentVersions > 0 && rule.NoncurrentVersionExpirationDays == 0 {
			return SmallDelay
		}
	case ActionKindExpiredDeleteMarker:
		if rule.ExpiredObjectDeleteMarker {
			return SmallDelay
		}
	}
	return 0
}
