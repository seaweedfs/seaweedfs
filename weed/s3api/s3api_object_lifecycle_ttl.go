package s3api

import (
	"math"
	"net/http"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// secondsPerDay is the conversion factor between Lifecycle Expiration.Days
// (a calendar-day count) and the volume server's TTL field (seconds since
// creation). This intentionally does NOT use AWS's "next 00:00 UTC" rounding;
// that's an expiration-firing nuance the lifecycle worker enforces, not
// something the per-write fast path can model without reading the clock at
// each write.
const secondsPerDay = int64(86400)

// LifecycleTTLResolver answers "what volume TTL should this write get?" for
// the PutObject path. Constructed once per bucket-config load with a
// pre-filtered, pre-sorted slice of rules so per-write cost is one
// HasPrefix per rule walked, exiting on first match.
//
// Stable predicates only: prefix and size. Tag-filtered rules are NOT in
// the fast path because tags can be replaced post-PUT via PutObjectTagging
// while volume TTL is irreversible — an object that matched at write time
// would still expire after the tag was removed. The lifecycle worker
// re-evaluates current tags at scan time.
//
// nil receiver means "no TTL applies" (no eligible rules, bucket
// versioned, or rules that overflow int32 seconds); callers can use a nil
// resolver freely.
type LifecycleTTLResolver struct {
	rules []*s3lifecycle.Rule
}

// NewLifecycleTTLResolver pre-filters and pre-sorts rules. Returns nil
// when nothing on the fast path can apply — callers don't need to special-
// case the empty-bucket / versioned-bucket / tag-only-rules cases.
//
// Sort is ascending by ExpirationDays so first prefix match is also the
// shortest matching expiration — AWS's overlapping-rule precedence (see
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-conflicts.html).
// Stable so equal-Days rules keep their XML order.
func NewLifecycleTTLResolver(rules []*s3lifecycle.Rule, versioned bool) *LifecycleTTLResolver {
	if versioned || len(rules) == 0 {
		// Versioned buckets: TTL volumes expire as a unit, which would
		// destroy noncurrent versions. Worker drives expiration there.
		return nil
	}
	out := make([]*s3lifecycle.Rule, 0, len(rules))
	for _, r := range rules {
		if r == nil || r.Status != s3lifecycle.StatusEnabled {
			continue
		}
		if r.ExpirationDays <= 0 {
			// NoncurrentVersionExpiration / AbortMPU / etc. — worker only.
			continue
		}
		if len(r.FilterTags) > 0 {
			// Tag-mutable; defer to the worker so a tag flip can't leave
			// us with a volume-TTL stamp the policy no longer dictates.
			continue
		}
		out = append(out, r)
	}
	if len(out) == 0 {
		return nil
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ExpirationDays < out[j].ExpirationDays
	})
	return &LifecycleTTLResolver{rules: out}
}

// Resolve returns the volume TTL (in seconds) for a write of the given
// object key and size, or 0 when no fast-path rule applies.
//
// The receiver may be nil — that's the common "no rules" case and it
// returns 0 without allocating.
func (r *LifecycleTTLResolver) Resolve(objectKey string, size int64) int32 {
	if r == nil {
		return 0
	}
	for _, rule := range r.rules {
		if !strings.HasPrefix(objectKey, rule.Prefix) {
			continue
		}
		if !ruleSizeMatches(rule, size) {
			continue
		}
		secs := int64(rule.ExpirationDays) * secondsPerDay
		if secs > math.MaxInt32 {
			// Volume TTL field is int32 seconds (~68 years). A longer
			// rule can't be represented without expiring early, so let
			// the lifecycle worker enforce it on its own schedule.
			return 0
		}
		return int32(secs)
	}
	return 0
}

// lifecycleTTLForObjectWrite is the PutObject call-site wrapper. Returns 0
// for any caller (MPU part, copy-part) that shouldn't bind a TTL clock —
// see putToFiler's signature comment for which paths pass 0 directly.
func (s3a *S3ApiServer) lifecycleTTLForObjectWrite(bucket, objectKey string, r *http.Request) int32 {
	cfg, _ := s3a.getBucketConfig(bucket)
	if cfg == nil || cfg.LifecycleTTL == nil {
		return 0
	}
	return cfg.LifecycleTTL.Resolve(objectKey, r.ContentLength)
}

func ruleSizeMatches(rule *s3lifecycle.Rule, size int64) bool {
	hasFilter := rule.FilterSizeGreaterThan > 0 || rule.FilterSizeLessThan > 0
	if hasFilter && size < 0 {
		// Content-Length wasn't sent; the size filter is unevaluable so
		// be safe and skip the rule. The worker re-evaluates at scan
		// time with the actual size.
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
