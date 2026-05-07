package s3lifecycle

import "time"

// EventLogHorizon returns the max event age the reader needs to drive the
// (rule, kind) action. Used by the retention mode gate: when
// metaLogRetention < EventLogHorizon + bootstrapLookbackMin, the action is
// promoted to scan_only. EXPIRATION_DATE returns 0 (date kind bypasses);
// count / immediate kinds return SmallDelay.
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
