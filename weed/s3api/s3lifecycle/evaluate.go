package s3lifecycle

import (
	"strings"
	"time"
)

// Evaluate decides which lifecycle action applies to info under rule at the
// given wall-clock time. Returns ActionNone when the rule is disabled, the
// filter rejects, the object shape doesn't match any action this rule defines,
// or no action is yet due.
//
// Action selection is by object shape (in priority order):
//
//	IsMPUInit       -> AbortIncompleteMultipartUpload
//	IsDeleteMarker  -> ExpiredObjectDeleteMarker (only when sole survivor)
//	IsLatest        -> Expiration (Days or Date)
//	non-current     -> NoncurrentVersionExpiration (Days + NewerNoncurrentVersions)
func Evaluate(rule *Rule, info *ObjectInfo, now time.Time) EvalResult {
	none := EvalResult{Action: ActionNone}
	if rule == nil || info == nil || rule.Status != StatusEnabled {
		return none
	}
	if !filterMatches(rule, info) {
		return none
	}

	switch {
	case info.IsMPUInit:
		if rule.AbortMPUDaysAfterInitiation > 0 {
			due := info.ModTime.AddDate(0, 0, rule.AbortMPUDaysAfterInitiation)
			if !now.Before(due) {
				return EvalResult{Action: ActionAbortMultipartUpload, RuleID: rule.ID}
			}
		}
		return none

	case info.IsDeleteMarker:
		if rule.ExpiredObjectDeleteMarker && info.NumVersions == 1 {
			return EvalResult{Action: ActionExpireDeleteMarker, RuleID: rule.ID}
		}
		return none

	case info.IsLatest:
		if rule.ExpirationDays > 0 {
			due := info.ModTime.AddDate(0, 0, rule.ExpirationDays)
			if !now.Before(due) {
				return EvalResult{Action: ActionDeleteObject, RuleID: rule.ID}
			}
		}
		if !rule.ExpirationDate.IsZero() && !now.Before(rule.ExpirationDate) {
			return EvalResult{Action: ActionDeleteObject, RuleID: rule.ID}
		}
		return none

	default:
		// Non-current version path.
		if rule.NoncurrentVersionExpirationDays > 0 {
			base := info.SuccessorModTime
			if base.IsZero() {
				base = info.ModTime
			}
			due := base.AddDate(0, 0, rule.NoncurrentVersionExpirationDays)
			if !now.Before(due) {
				if rule.NewerNoncurrentVersions <= 0 || info.NoncurrentIndex >= rule.NewerNoncurrentVersions {
					return EvalResult{Action: ActionDeleteVersion, RuleID: rule.ID}
				}
			}
			return none
		}
		// NewerNoncurrentVersions without a day threshold: count-only retention.
		if rule.NewerNoncurrentVersions > 0 && info.NoncurrentIndex >= rule.NewerNoncurrentVersions {
			return EvalResult{Action: ActionDeleteVersion, RuleID: rule.ID}
		}
		return none
	}
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
