package s3lifecycle

import (
	"strings"
	"time"
)

// EvaluateAction decides whether the (rule, kind) compiled action fires for
// info at the given wall-clock time. Returns ActionNone when the rule is
// disabled, the filter rejects, the object shape doesn't match this kind, or
// the kind isn't yet due.
//
// One XML rule may declare multiple actions in parallel. The engine compiles
// each into its own ActionKey and calls EvaluateAction once per (rule, kind,
// entry); sibling actions are independent and may produce different verdicts
// for the same entry. Callers that need to evaluate every action of a rule
// iterate `RuleActionKinds(rule)` and call this helper per kind.
//
// Action selection by object shape per kind:
//
//	IsMPUInit                   + ActionKindAbortMPU             -> AbortIncompleteMultipartUpload
//	IsLatest && IsDeleteMarker  + ActionKindExpiredDeleteMarker  -> ExpiredObjectDeleteMarker (sole survivor)
//	IsLatest                    + ActionKindExpirationDays       -> DeleteObject (Days threshold)
//	IsLatest                    + ActionKindExpirationDate       -> DeleteObject (date threshold)
//	non-current                 + ActionKindNoncurrentDays       -> DeleteVersion (Days + NewerNoncurrent retention)
//	non-current                 + ActionKindNewerNoncurrent      -> DeleteVersion (count-only)
//
// Per AWS S3 semantics, a non-current delete marker is treated as a regular
// non-current version under NONCURRENT_DAYS / NEWER_NONCURRENT. Only the
// *current* delete marker (and only as sole survivor) routes to
// EXPIRED_DELETE_MARKER.
func EvaluateAction(rule *Rule, kind ActionKind, info *ObjectInfo, now time.Time) EvalResult {
	none := EvalResult{Action: ActionNone}
	if rule == nil || info == nil || rule.Status != StatusEnabled {
		return none
	}
	if !filterMatches(rule, info) {
		return none
	}

	switch kind {
	case ActionKindAbortMPU:
		if !info.IsMPUInit || rule.AbortMPUDaysAfterInitiation <= 0 {
			return none
		}
		due := info.ModTime.AddDate(0, 0, rule.AbortMPUDaysAfterInitiation)
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
		due := info.ModTime.AddDate(0, 0, rule.ExpirationDays)
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
		due := base.AddDate(0, 0, rule.NoncurrentVersionExpirationDays)
		if now.Before(due) {
			return none
		}
		if rule.NewerNoncurrentVersions > 0 && info.NoncurrentIndex < rule.NewerNoncurrentVersions {
			return none
		}
		return EvalResult{Action: ActionDeleteVersion, RuleID: rule.ID}

	case ActionKindNewerNoncurrent:
		// Pure count-based: only when NoncurrentDays is unset (when paired,
		// the rule expands to NONCURRENT_DAYS instead — see RuleActionKinds).
		if info.IsLatest || rule.NoncurrentVersionExpirationDays > 0 || rule.NewerNoncurrentVersions <= 0 {
			return none
		}
		if info.NoncurrentIndex < rule.NewerNoncurrentVersions {
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
