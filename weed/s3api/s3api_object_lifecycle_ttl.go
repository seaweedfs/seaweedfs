package s3api

import (
	"math"
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
// pre-filtered, pre-sorted slice of compact rules so per-write cost is one
// HasPrefix per rule walked, exiting on first match.
//
// Stable predicates only: prefix and size. Tag-filtered rules are NOT in
// the fast path because tags can be replaced post-PUT via PutObjectTagging
// while volume TTL is irreversible — an object that matched at write time
// would still expire after the tag was removed. The lifecycle worker
// re-evaluates current tags at scan time.
//
// nil receiver means "no TTL applies" (no eligible rules, bucket
// versioned, or every rule overflows int32 seconds); callers can use a
// nil resolver freely.
type LifecycleTTLResolver struct {
	rules []ttlRule
}

// ttlRule is the compact, hot-path projection of an Expiration.Days rule:
// just the four fields Resolve reads, with ExpirationDays already converted
// to int32 seconds so the inner loop has no arithmetic and no overflow
// branch.
type ttlRule struct {
	prefix string
	ttlSec int32
	sizeGT int64
	sizeLT int64
}

// NewLifecycleTTLResolver pre-filters and pre-sorts rules. Returns nil
// when nothing on the fast path can apply — callers don't need to special-
// case the empty-bucket / versioned-bucket / tag-only-rules cases.
//
// Sort is ascending by ttlSec so first prefix match is also the shortest
// matching expiration — AWS's overlapping-rule precedence (see
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-conflicts.html).
// Stable so equal-Days rules keep their XML order.
//
// Rules that overflow int32 seconds (~68 years) are dropped at construction
// rather than handled per-resolve: capping would expire long policies
// early, and returning 0 from Resolve in the inner loop would prevent any
// shorter overlapping rule from being considered. Drop-at-construction
// composes correctly with the ascending sort.
func NewLifecycleTTLResolver(rules []*s3lifecycle.Rule, versioned bool) *LifecycleTTLResolver {
	if versioned || len(rules) == 0 {
		// Versioned buckets: TTL volumes expire as a unit, which would
		// destroy noncurrent versions. Worker drives expiration there.
		return nil
	}
	out := make([]ttlRule, 0, len(rules))
	for _, r := range rules {
		if r == nil || r.Status != s3lifecycle.StatusEnabled {
			continue
		}
		if r.ExpirationDays <= 0 {
			continue // NoncurrentVersionExpiration / AbortMPU / etc.
		}
		if len(r.FilterTags) > 0 {
			// Tag-mutable; defer to the worker so a tag flip can't leave
			// us with a volume-TTL stamp the policy no longer dictates.
			continue
		}
		secs := int64(r.ExpirationDays) * secondsPerDay
		if secs > math.MaxInt32 {
			// Volume TTL is int32 seconds. A rule that doesn't fit
			// can't be represented without expiring early; the
			// lifecycle worker enforces it on its own schedule.
			continue
		}
		out = append(out, ttlRule{
			prefix: r.Prefix,
			ttlSec: int32(secs),
			sizeGT: r.FilterSizeGreaterThan,
			sizeLT: r.FilterSizeLessThan,
		})
	}
	if len(out) == 0 {
		return nil
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ttlSec < out[j].ttlSec
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
	for i := range r.rules {
		rule := &r.rules[i]
		if !strings.HasPrefix(objectKey, rule.prefix) {
			continue
		}
		// Size filter: unevaluable when Content-Length is unknown
		// (size<0) and the rule has any size predicate; otherwise
		// either bound short-circuits.
		if rule.sizeGT > 0 || rule.sizeLT > 0 {
			if size < 0 {
				continue
			}
			if rule.sizeGT > 0 && size <= rule.sizeGT {
				continue
			}
			if rule.sizeLT > 0 && size >= rule.sizeLT {
				continue
			}
		}
		return rule.ttlSec
	}
	return 0
}

// lifecycleTTLForObjectWrite is the PutObject call-site wrapper. Returns 0
// for any caller (MPU part, copy-part) that shouldn't bind a TTL clock —
// see putToFiler's signature comment for which paths pass 0 directly.
//
// Callers MUST pass the actual object size, not r.ContentLength when those
// differ. r.ContentLength is the wire size: for a multipart PostPolicy
// upload it includes form fields and boundaries, so a size-filtered rule
// would mis-evaluate against the form total instead of the file body.
// objectSize<0 is "unknown" — the resolver skips any size-filtered rule.
func (s3a *S3ApiServer) lifecycleTTLForObjectWrite(bucket, objectKey string, objectSize int64) int32 {
	cfg, _ := s3a.getBucketConfig(bucket)
	if cfg == nil || cfg.LifecycleTTL == nil {
		return 0
	}
	return cfg.LifecycleTTL.Resolve(objectKey, objectSize)
}
