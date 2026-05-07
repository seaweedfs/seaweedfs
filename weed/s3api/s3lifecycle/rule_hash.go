package s3lifecycle

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"
)

// RuleHash returns the first 8 bytes of sha256 over a canonicalized rule
// representation. The hash is stable across:
//
//   - tag-key reorder (FilterTags is sorted before hashing)
//   - prefix trailing-slash variation
//   - rule.ID changes (ID is excluded — it's display-only)
//   - rule.Status flips (Enabled <-> Disabled — state continuity preserved
//     across operator toggles)
//
// Different action shapes (different days, different filter, different
// action types) hash to different values.
func RuleHash(rule *Rule) [8]byte {
	if rule == nil {
		var zero [8]byte
		return zero
	}
	var b strings.Builder
	// Filter.
	fmt.Fprintf(&b, "prefix=%s\n", normalizePrefix(rule.Prefix))
	tagKeys := make([]string, 0, len(rule.FilterTags))
	for k := range rule.FilterTags {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)
	for _, k := range tagKeys {
		fmt.Fprintf(&b, "tag=%s=%s\n", k, rule.FilterTags[k])
	}
	fmt.Fprintf(&b, "size_gt=%d\n", rule.FilterSizeGreaterThan)
	fmt.Fprintf(&b, "size_lt=%d\n", rule.FilterSizeLessThan)
	// Actions.
	fmt.Fprintf(&b, "exp_days=%d\n", rule.ExpirationDays)
	fmt.Fprintf(&b, "exp_date=%s\n", canonicalTime(rule.ExpirationDate))
	fmt.Fprintf(&b, "exp_dm=%t\n", rule.ExpiredObjectDeleteMarker)
	fmt.Fprintf(&b, "noncur_days=%d\n", rule.NoncurrentVersionExpirationDays)
	fmt.Fprintf(&b, "noncur_keep=%d\n", rule.NewerNoncurrentVersions)
	fmt.Fprintf(&b, "mpu_days=%d\n", rule.AbortMPUDaysAfterInitiation)

	sum := sha256.Sum256([]byte(b.String()))
	var out [8]byte
	copy(out[:], sum[:8])
	return out
}

func normalizePrefix(p string) string {
	return strings.TrimRight(p, "/")
}

func canonicalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
