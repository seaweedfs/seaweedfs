package s3api

import (
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// secondsPerDay is the conversion factor between Lifecycle Expiration.Days
// (a calendar-day count) and the volume server's TTL field (seconds since
// creation). This intentionally does NOT use AWS's "next 00:00 UTC" rounding;
// that's an expiration-firing nuance the lifecycle worker enforces, not
// something the per-write fast path can model without reading the clock at
// each write.
const secondsPerDay = int64(86400)

// buildTTLFastPathRules pre-filters the canonical rules down to the ones the
// PutObject TTL resolver could ever fire for and sorts by descending prefix
// length so first match is longest match. Disabled / non-Expiration.Days
// rules are dropped entirely so the per-PUT walk skips them without an
// inline check.
func buildTTLFastPathRules(rules []*s3lifecycle.Rule) []*s3lifecycle.Rule {
	if len(rules) == 0 {
		return nil
	}
	out := make([]*s3lifecycle.Rule, 0, len(rules))
	for _, r := range rules {
		if r == nil {
			continue
		}
		if r.Status != s3lifecycle.StatusEnabled {
			continue
		}
		if r.ExpirationDays <= 0 {
			continue
		}
		out = append(out, r)
	}
	if len(out) == 0 {
		return nil
	}
	// Stable sort so rules with equal prefix length keep their XML order
	// (matches AWS's "rules are evaluated in document order for ties").
	sort.SliceStable(out, func(i, j int) bool {
		return len(out[i].Prefix) > len(out[j].Prefix)
	})
	return out
}

// resolveLifecycleTTLForWrite returns the volume TTL (in seconds) the
// PutObject path should pass to AssignVolume and stamp on the new entry.
//
// Returns 0 ("no TTL — let the lifecycle worker handle it later") when:
//   - the bucket has no fast-path-eligible rules (no XML, all rules
//     disabled, or only NoncurrentVersionExpiration / AbortMPU / etc.),
//   - the bucket is versioned (TTL volumes expire as a unit, which would
//     destroy noncurrent versions),
//   - no rule's prefix matches the object key,
//   - the matching rule's tag or size filter doesn't match this request.
//
// Cost: one HasPrefix per rule walked, exiting on first match. The cache
// pre-sorts by descending prefix length so first match is also longest
// match — a typical PUT walks one rule.
func resolveLifecycleTTLForWrite(cfg *BucketConfig, objectKey string, requestTags map[string]string, size int64) int32 {
	if cfg == nil || len(cfg.ttlFastPathRules) == 0 {
		return 0
	}
	if cfg.Versioning == s3_constants.VersioningEnabled || cfg.Versioning == s3_constants.VersioningSuspended {
		return 0
	}
	for _, rule := range cfg.ttlFastPathRules {
		if !strings.HasPrefix(objectKey, rule.Prefix) {
			continue
		}
		if !ruleTagsMatch(rule.FilterTags, requestTags) {
			continue
		}
		if !ruleSizeMatches(rule, size) {
			continue
		}
		secs := int64(rule.ExpirationDays) * secondsPerDay
		if secs > math.MaxInt32 {
			// AWS allows Expiration.Days up to 2,147,483,647 — that
			// overflows int32 seconds (~24,855 days). Cap at int32 max;
			// the rule effectively becomes "never expire via volume TTL"
			// and the lifecycle worker enforces it on its own schedule.
			return math.MaxInt32
		}
		return int32(secs)
	}
	return 0
}

// parseRequestTags pulls k=v pairs from the X-Amz-Tagging header. AWS sends
// them URL-encoded; duplicates and parse errors yield an empty map so a
// malformed header doesn't accidentally match a tag-filtered lifecycle rule.
func parseRequestTags(r *http.Request) map[string]string {
	raw := r.Header.Get(s3_constants.AmzObjectTagging)
	if raw == "" {
		return nil
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return nil
	}
	out := make(map[string]string, len(values))
	for k, v := range values {
		if len(v) != 1 {
			return nil
		}
		out[k] = v[0]
	}
	return out
}

func ruleTagsMatch(want, got map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	for k, v := range want {
		if g, ok := got[k]; !ok || g != v {
			return false
		}
	}
	return true
}

func ruleSizeMatches(rule *s3lifecycle.Rule, size int64) bool {
	hasFilter := rule.FilterSizeGreaterThan > 0 || rule.FilterSizeLessThan > 0
	if hasFilter && size < 0 {
		// Content-Length wasn't sent; any "size > N" / "size < N" filter
		// is unevaluable, so be safe and skip the rule. The lifecycle
		// worker re-evaluates with the real size at scan time.
		return false
	}
	if rule.FilterSizeGreaterThan > 0 && size <= rule.FilterSizeGreaterThan {
		return false
	}
	if rule.FilterSizeLessThan > 0 && size >= rule.FilterSizeLessThan {
		return false
	}
	return true
}
