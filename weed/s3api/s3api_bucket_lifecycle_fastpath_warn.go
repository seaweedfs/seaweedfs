package s3api

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/lifecycle_xml"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// fastpathWarningHeader is set on Put/DeleteBucketLifecycle responses when
// the bucket has the lifecycle TTL fast path enabled and the configuration
// change leaves objects already written under the previous rules on their
// baked-in volume TTL. A volume TTL is decided once, at write time, and
// can't be taken back: removing or lengthening a rule does not retroactively
// apply to objects already stamped, unlike the default worker-driven path
// which re-evaluates the current rules each pass. The header lets an
// operator notice (and act before the old deadline) that "extend retention
// / remove the rule" did not protect already-recorded objects.
const fastpathWarningHeader = "X-Seaweed-Lifecycle-Fastpath-Warning"

// fastpathConfigChangeLeavesStampedObjects reports whether a lifecycle
// configuration change from oldXML to newXML on a fastpath-enabled bucket
// leaves previously-stamped objects on a volume TTL that no longer matches
// the new policy. Returns a short human-readable reason when it does, and
// an empty string when it does not.
//
// A change "leaves stamped objects" when, for any rule that was on the
// fast path before (Enabled, ExpirationDays>0, no tag filter, fits int32
// seconds — the same eligibility NewLifecycleTTLResolver applies):
//
//   - it is absent or Disabled in the new config (removed/disabled), or
//   - its ExpirationDays increased (lengthened — old objects keep the
//     shorter baked-in TTL), or
//   - its prefix or size filter changed (objects that matched before may
//     keep an expiry the new rule no longer describes).
//
// Rule identity is matched in two passes: first by ID, then by fast-path
// predicates (prefix + size filters). An ID-only rename with unchanged
// predicates and days is NOT a warning — the policy is equivalent, only
// the label moved.
//
// Shortening a rule (e.g. 30d -> 7d) does NOT warn: old objects simply
// expire later than the new shorter rule, which is not the data-loss
// direction. Tag-filtered rules are never on the fast path, so their
// removal/change does not warn here (the worker re-evaluates tags).
//
// newXML may be nil/empty, which represents DeleteBucketLifecycle: every
// previously-eligible rule is removed.
func fastpathConfigChangeLeavesStampedObjects(oldXML, newXML []byte) string {
	oldRules := fastpathEligibleRules(oldXML)
	if len(oldRules) == 0 {
		return ""
	}
	newRules := fastpathEligibleRules(newXML)

	// Match old rules to new rules in two passes so an ID-only rename
	// (same prefix/size/days, different ID) does not produce a false
	// "removed" warning:
	//   1. Match by ID. A rule that kept its ID is the same rule; check
	//      for filter or days changes.
	//   2. For unmatched old rules, try matching by fast-path predicates
	//      (prefix + size). A predicate match with different days means
	//      the rule was renamed and possibly lengthened; same days means
	//      a pure rename — no warning.
	// Greedy: each new rule is consumed by at most one old rule so
	// overlapping rules can't be double-matched.
	used := make([]bool, len(newRules))
	newByID := make(map[string]int, len(newRules))
	for i, r := range newRules {
		if r.ID != "" {
			newByID[r.ID] = i
		}
	}

	var reasons []string
	for _, r := range oldRules {
		// Pass 1: ID match.
		if r.ID != "" {
			if idx, ok := newByID[r.ID]; ok && !used[idx] {
				used[idx] = true
				nr := newRules[idx]
				if nr.Prefix != r.Prefix || nr.FilterSizeGreaterThan != r.FilterSizeGreaterThan || nr.FilterSizeLessThan != r.FilterSizeLessThan {
					reasons = append(reasons, fmt.Sprintf("rule %q filter changed", ruleName(r)))
				} else if nr.ExpirationDays > r.ExpirationDays {
					reasons = append(reasons, fmt.Sprintf("rule %q lengthened %d -> %d days", ruleName(r), r.ExpirationDays, nr.ExpirationDays))
				}
				continue
			}
		}
		// Pass 2: predicate match (prefix + size filters).
		matched := false
		for i, nr := range newRules {
			if used[i] {
				continue
			}
			if nr.Prefix == r.Prefix && nr.FilterSizeGreaterThan == r.FilterSizeGreaterThan && nr.FilterSizeLessThan == r.FilterSizeLessThan {
				used[i] = true
				// Same coverage; only days can differ. An ID-only
				// rename with unchanged days is not a warning.
				if nr.ExpirationDays > r.ExpirationDays {
					reasons = append(reasons, fmt.Sprintf("rule %q lengthened %d -> %d days", ruleName(r), r.ExpirationDays, nr.ExpirationDays))
				}
				matched = true
				break
			}
		}
		if !matched {
			reasons = append(reasons, fmt.Sprintf("rule %q removed or disabled", ruleName(r)))
		}
	}
	if len(reasons) == 0 {
		return ""
	}
	sort.Strings(reasons)
	return "fast path enabled; objects already written keep their baked-in volume TTL: " + strings.Join(reasons, "; ")
}

// fastpathEligibleRules parses lifecycle XML and returns the subset of
// rules the per-write fast path would stamp: Enabled, ExpirationDays>0,
// no tag filter, and ExpirationDays fits int32 seconds. Mirrors the
// filter in NewLifecycleTTLResolver so the warning fires exactly for
// rules that could have stamped objects. A nil/empty XML yields nil.
func fastpathEligibleRules(xmlBytes []byte) []*s3lifecycle.Rule {
	if len(xmlBytes) == 0 {
		return nil
	}
	rules, err := lifecycle_xml.ParseCanonical(xmlBytes)
	if err != nil {
		return nil
	}
	out := make([]*s3lifecycle.Rule, 0, len(rules))
	for _, r := range rules {
		if r == nil || r.Status != s3lifecycle.StatusEnabled {
			continue
		}
		if r.ExpirationDays <= 0 {
			continue
		}
		if len(r.FilterTags) > 0 {
			continue
		}
		if int64(r.ExpirationDays)*secondsPerDay > math.MaxInt32 {
			continue
		}
		out = append(out, r)
	}
	return out
}

func ruleName(r *s3lifecycle.Rule) string {
	if r.ID != "" {
		return r.ID
	}
	if r.Prefix != "" {
		return "prefix=" + r.Prefix
	}
	return "(whole-bucket)"
}
