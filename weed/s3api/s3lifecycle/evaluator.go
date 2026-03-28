package s3lifecycle

import "time"

// Evaluate checks the given lifecycle rules against an object and returns
// the highest-priority action that applies. The evaluation follows S3's
// action priority:
//  1. ExpiredObjectDeleteMarker (delete marker is sole version)
//  2. NoncurrentVersionExpiration (non-current version age/count)
//  3. Current version Expiration (Days or Date)
//
// AbortIncompleteMultipartUpload is evaluated separately since it applies
// to uploads, not objects. Use EvaluateMPUAbort for that.
func Evaluate(rules []Rule, obj ObjectInfo, now time.Time) EvalResult {
	// Phase 1: ExpiredObjectDeleteMarker
	if obj.IsDeleteMarker && obj.IsLatest && obj.NumVersions == 1 {
		for _, rule := range rules {
			if rule.Status != "Enabled" {
				continue
			}
			if !MatchesFilter(rule, obj) {
				continue
			}
			if rule.ExpiredObjectDeleteMarker {
				return EvalResult{Action: ActionExpireDeleteMarker, RuleID: rule.ID}
			}
		}
	}

	// Phase 2: NoncurrentVersionExpiration
	if !obj.IsLatest && !obj.SuccessorModTime.IsZero() {
		for _, rule := range rules {
			if ShouldExpireNoncurrentVersion(rule, obj, obj.NoncurrentIndex, now) {
				return EvalResult{Action: ActionDeleteVersion, RuleID: rule.ID}
			}
		}
	}

	// Phase 3: Current version Expiration
	if obj.IsLatest && !obj.IsDeleteMarker {
		for _, rule := range rules {
			if rule.Status != "Enabled" {
				continue
			}
			if !MatchesFilter(rule, obj) {
				continue
			}
			// Date-based expiration
			if !rule.ExpirationDate.IsZero() && !now.Before(rule.ExpirationDate) {
				return EvalResult{Action: ActionDeleteObject, RuleID: rule.ID}
			}
			// Days-based expiration
			if rule.ExpirationDays > 0 {
				expiryTime := expectedExpiryTime(obj.ModTime, rule.ExpirationDays)
				if !now.Before(expiryTime) {
					return EvalResult{Action: ActionDeleteObject, RuleID: rule.ID}
				}
			}
		}
	}

	return EvalResult{Action: ActionNone}
}

// ShouldExpireNoncurrentVersion checks whether a non-current version should
// be expired considering both NoncurrentDays and NewerNoncurrentVersions.
// noncurrentIndex is the 0-based position among non-current versions sorted
// newest-first (0 = newest non-current version).
func ShouldExpireNoncurrentVersion(rule Rule, obj ObjectInfo, noncurrentIndex int, now time.Time) bool {
	if rule.Status != "Enabled" {
		return false
	}
	if rule.NoncurrentVersionExpirationDays <= 0 {
		return false
	}
	if obj.IsLatest || obj.SuccessorModTime.IsZero() {
		return false
	}
	if !MatchesFilter(rule, obj) {
		return false
	}

	// Check age threshold.
	expiryTime := expectedExpiryTime(obj.SuccessorModTime, rule.NoncurrentVersionExpirationDays)
	if now.Before(expiryTime) {
		return false
	}

	// Check NewerNoncurrentVersions count threshold.
	if rule.NewerNoncurrentVersions > 0 && noncurrentIndex < rule.NewerNoncurrentVersions {
		return false
	}

	return true
}

// EvaluateMPUAbort finds the applicable AbortIncompleteMultipartUpload rule
// for a multipart upload with the given key prefix and creation time.
func EvaluateMPUAbort(rules []Rule, uploadKey string, createdAt time.Time, now time.Time) EvalResult {
	for _, rule := range rules {
		if rule.Status != "Enabled" {
			continue
		}
		if rule.AbortMPUDaysAfterInitiation <= 0 {
			continue
		}
		if !matchesPrefix(rule.Prefix, uploadKey) {
			continue
		}
		cutoff := createdAt.Add(time.Duration(rule.AbortMPUDaysAfterInitiation) * 24 * time.Hour)
		if !now.Before(cutoff) {
			return EvalResult{Action: ActionAbortMultipartUpload, RuleID: rule.ID}
		}
	}
	return EvalResult{Action: ActionNone}
}

// expectedExpiryTime computes the expiration time given a reference time and
// a number of days. Following S3 semantics, expiration happens at midnight UTC
// of the day after the specified number of days.
func expectedExpiryTime(refTime time.Time, days int) time.Time {
	if days == 0 {
		return refTime
	}
	t := refTime.UTC().Add(time.Duration(days+1) * 24 * time.Hour)
	return t.Truncate(24 * time.Hour)
}
