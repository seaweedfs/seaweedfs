package s3lifecycle

import (
	"strings"
	"time"
)

// EvaluateAction returns whether the (rule, kind) action fires for info at
// now. A non-current delete marker is just another non-current version under
// NONCURRENT_DAYS / NEWER_NONCURRENT; only a current sole-survivor marker
// routes to EXPIRED_DELETE_MARKER.
func EvaluateAction(rule *Rule, kind ActionKind, info *ObjectInfo, now time.Time) EvalResult {
	none := EvalResult{Action: ActionNone}
	if rule == nil || info == nil || rule.Status != StatusEnabled {
		return none
	}
	if !filterMatches(rule, info) {
		return none
	}
	// MPU init records carry IsLatest=false (they are not yet versions);
	// without this guard NoncurrentDays / NewerNoncurrent fire on them
	// and the dispatcher BLOCKs because version_id is empty, freezing
	// the cursor. Only ABORT_MPU is meaningful for an in-flight upload.
	if info.IsMPUInit && kind != ActionKindAbortMPU {
		return none
	}

	switch kind {
	case ActionKindAbortMPU:
		if !info.IsMPUInit || rule.AbortMPUDaysAfterInitiation <= 0 {
			return none
		}
		due := info.ModTime.Add(DaysToDuration(rule.AbortMPUDaysAfterInitiation))
		if now.Before(due) {
			return none
		}
		return EvalResult{Action: ActionAbortMultipartUpload, RuleID: rule.ID}

	case ActionKindExpiredDeleteMarker:
		if !info.IsLatest || !info.IsDeleteMarker || !rule.ExpiredObjectDeleteMarker {
			return none
		}
		if info.NumVersions != 1 {
			return none
		}
		return EvalResult{Action: ActionExpireDeleteMarker, RuleID: rule.ID}

	case ActionKindExpirationDays:
		if !info.IsLatest || info.IsDeleteMarker || rule.ExpirationDays <= 0 {
			return none
		}
		due := info.ModTime.Add(DaysToDuration(rule.ExpirationDays))
		if now.Before(due) {
			return none
		}
		return EvalResult{Action: ActionDeleteObject, RuleID: rule.ID}

	case ActionKindExpirationDate:
		if !info.IsLatest || info.IsDeleteMarker || rule.ExpirationDate.IsZero() {
			return none
		}
		if now.Before(rule.ExpirationDate) {
			return none
		}
		return EvalResult{Action: ActionDeleteObject, RuleID: rule.ID}

	case ActionKindNoncurrentDays:
		if info.IsLatest || rule.NoncurrentVersionExpirationDays <= 0 {
			return none
		}
		base := info.SuccessorModTime
		if base.IsZero() {
			base = info.ModTime
		}
		due := base.Add(DaysToDuration(rule.NoncurrentVersionExpirationDays))
		if now.Before(due) {
			return none
		}
		// nil index = can't evaluate retention; safety-scan revisits.
		if rule.NewerNoncurrentVersions > 0 {
			if info.NoncurrentIndex == nil || *info.NoncurrentIndex < rule.NewerNoncurrentVersions {
				return none
			}
		}
		return EvalResult{Action: ActionDeleteVersion, RuleID: rule.ID}

	case ActionKindNewerNoncurrent:
		// Count-only; when paired with NoncurrentDays the rule expands to
		// NONCURRENT_DAYS instead (RuleActionKinds).
		if info.IsLatest || rule.NoncurrentVersionExpirationDays > 0 || rule.NewerNoncurrentVersions <= 0 {
			return none
		}
		if info.NoncurrentIndex == nil || *info.NoncurrentIndex < rule.NewerNoncurrentVersions {
			return none
		}
		return EvalResult{Action: ActionDeleteVersion, RuleID: rule.ID}
	}
	return none
}

func filterMatches(rule *Rule, info *ObjectInfo) bool {
	if rule.Prefix != "" && !strings.HasPrefix(info.Key, rule.Prefix) {
		return false
	}
	if rule.FilterSizeGreaterThan > 0 && info.Size <= rule.FilterSizeGreaterThan {
		return false
	}
	if rule.FilterSizeLessThan > 0 && info.Size >= rule.FilterSizeLessThan {
		return false
	}
	for k, v := range rule.FilterTags {
		if got, ok := info.Tags[k]; !ok || got != v {
			return false
		}
	}
	return true
}
