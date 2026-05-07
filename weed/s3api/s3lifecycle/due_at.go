package s3lifecycle

import "time"

// ComputeDueAt returns the earliest wall-clock time the rule could fire for
// info, given the object's current shape. Returns the zero time when no
// action defined by rule applies to this object (filter rejects, no matching
// action shape, etc.).
//
// Used by the reader/bootstrap to decide whether to enqueue a pending entry
// (when due_at > now + smallDelay) versus inline-delete.
func ComputeDueAt(rule *Rule, info *ObjectInfo) time.Time {
	if rule == nil || info == nil || rule.Status != StatusEnabled {
		return time.Time{}
	}
	if !filterMatches(rule, info) {
		return time.Time{}
	}

	switch {
	case info.IsMPUInit:
		if rule.AbortMPUDaysAfterInitiation > 0 {
			return info.ModTime.AddDate(0, 0, rule.AbortMPUDaysAfterInitiation)
		}
		return time.Time{}

	case info.IsDeleteMarker:
		if rule.ExpiredObjectDeleteMarker && info.NumVersions == 1 {
			return info.ModTime
		}
		return time.Time{}

	case info.IsLatest:
		if rule.ExpirationDays > 0 {
			return info.ModTime.AddDate(0, 0, rule.ExpirationDays)
		}
		if !rule.ExpirationDate.IsZero() {
			return rule.ExpirationDate
		}
		return time.Time{}

	default:
		if rule.NoncurrentVersionExpirationDays > 0 {
			base := info.SuccessorModTime
			if base.IsZero() {
				base = info.ModTime
			}
			return base.AddDate(0, 0, rule.NoncurrentVersionExpirationDays)
		}
		if rule.NewerNoncurrentVersions > 0 {
			// Count-only retention is immediate when the version exceeds the
			// keep-latest-N window. We don't know the index here without
			// info; the evaluator does the eligibility check. Return
			// info.SuccessorModTime (or ModTime as fallback) so the caller
			// can compare against now.
			if !info.SuccessorModTime.IsZero() {
				return info.SuccessorModTime
			}
			return info.ModTime
		}
		return time.Time{}
	}
}
