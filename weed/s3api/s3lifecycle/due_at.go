package s3lifecycle

import "time"

// ComputeDueAt returns the earliest wall-clock time the (rule, kind) compiled
// action can fire for info given the object's current shape. Returns the
// zero time when the action cannot fire for this entry (filter rejects, kind
// not declared on the rule, wrong object shape, etc.).
//
// Used by the reader/bootstrap to decide pending-vs-inline-delete for one
// specific action. Sibling actions of the same XML rule are computed
// separately so a rule's 7d AbortMPU due time does not influence its 90d
// ExpirationDays sibling.
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
			return info.ModTime.AddDate(0, 0, rule.AbortMPUDaysAfterInitiation)
		}
	case ActionKindExpiredDeleteMarker:
		if info.IsLatest && info.IsDeleteMarker && rule.ExpiredObjectDeleteMarker && info.NumVersions == 1 {
			return info.ModTime
		}
	case ActionKindExpirationDays:
		if info.IsLatest && !info.IsDeleteMarker && rule.ExpirationDays > 0 {
			return info.ModTime.AddDate(0, 0, rule.ExpirationDays)
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
			return base.AddDate(0, 0, rule.NoncurrentVersionExpirationDays)
		}
	case ActionKindNewerNoncurrent:
		// Pure count-based: only when NoncurrentDays is unset.
		if !info.IsLatest && rule.NoncurrentVersionExpirationDays == 0 && rule.NewerNoncurrentVersions > 0 {
			if !info.SuccessorModTime.IsZero() {
				return info.SuccessorModTime
			}
			return info.ModTime
		}
	}
	return time.Time{}
}
