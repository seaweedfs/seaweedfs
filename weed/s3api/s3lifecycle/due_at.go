package s3lifecycle

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// DaysToDuration converts a "days" lifecycle threshold into a wall-clock
// duration. Production builds keep one day = 24h; the s3tests build tag
// shrinks it to LifeCycleInterval (10s) so the upstream s3-tests
// expiration suite can complete inside its 30s polling window. Exported so
// tests in sibling packages can pin their math to the same scale.
func DaysToDuration(days int) time.Duration {
	return time.Duration(days) * util.LifeCycleInterval
}

// ComputeDueAt returns the earliest wall-clock time the (rule, kind) action
// can fire for info. Returns zero time when no action can fire for this
// entry. Used by reader/bootstrap to decide pending vs. inline-delete.
func ComputeDueAt(rule *Rule, kind ActionKind, info *ObjectInfo) time.Time {
	if rule == nil || info == nil || rule.Status != StatusEnabled {
		return time.Time{}
	}
	if !filterMatches(rule, info) {
		return time.Time{}
	}

	switch kind {
	case ActionKindAbortMPU:
		if info.IsMPUInit && rule.AbortMPUDaysAfterInitiation > 0 {
			return info.ModTime.Add(DaysToDuration(rule.AbortMPUDaysAfterInitiation))
		}
	case ActionKindExpiredDeleteMarker:
		if info.IsLatest && info.IsDeleteMarker && rule.ExpiredObjectDeleteMarker && info.NumVersions == 1 {
			return info.ModTime
		}
	case ActionKindExpirationDays:
		if info.IsLatest && !info.IsDeleteMarker && rule.ExpirationDays > 0 {
			return info.ModTime.Add(DaysToDuration(rule.ExpirationDays))
		}
	case ActionKindExpirationDate:
		if info.IsLatest && !info.IsDeleteMarker && !rule.ExpirationDate.IsZero() {
			return rule.ExpirationDate
		}
	case ActionKindNoncurrentDays:
		if !info.IsLatest && rule.NoncurrentVersionExpirationDays > 0 {
			base := info.SuccessorModTime
			if base.IsZero() {
				base = info.ModTime
			}
			return base.Add(DaysToDuration(rule.NoncurrentVersionExpirationDays))
		}
	case ActionKindNewerNoncurrent:
		if !info.IsLatest && rule.NoncurrentVersionExpirationDays == 0 && rule.NewerNoncurrentVersions > 0 {
			if !info.SuccessorModTime.IsZero() {
				return info.SuccessorModTime
			}
			return info.ModTime
		}
	}
	return time.Time{}
}
