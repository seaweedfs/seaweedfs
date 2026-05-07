package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// LifecycleToCanonical converts the XML-deserialized Lifecycle into the flat
// s3lifecycle.Rule shape the engine compiles against. One XML <Rule> maps to
// exactly one s3lifecycle.Rule (with potentially multiple action sub-fields
// populated); the engine then expands each into its compiled actions via
// s3lifecycle.RuleActionKinds.
//
// Filter resolution mirrors AWS semantics: the optional <Filter> element may
// contain a single <Prefix> | <Tag> | <And>, or be absent (in which case the
// older top-level <Prefix> applies). When <And> is used, its sub-elements
// (Prefix + multiple Tags + size filters) are flattened into the canonical
// Rule's individual fields.
func LifecycleToCanonical(lc *Lifecycle) []*s3lifecycle.Rule {
	if lc == nil {
		return nil
	}
	out := make([]*s3lifecycle.Rule, 0, len(lc.Rules))
	for i := range lc.Rules {
		out = append(out, ruleToCanonical(&lc.Rules[i]))
	}
	return out
}

func ruleToCanonical(r *Rule) *s3lifecycle.Rule {
	out := &s3lifecycle.Rule{
		ID:     r.ID,
		Status: string(r.Status),
	}

	// Prefix: <Filter><Prefix> or <Filter><And><Prefix> or top-level <Prefix>.
	prefix, tags, sizeGT, sizeLT := flattenFilter(&r.Filter)
	if prefix == "" && r.Prefix.set {
		prefix = r.Prefix.val
	}
	out.Prefix = prefix
	if len(tags) > 0 {
		out.FilterTags = tags
	}
	out.FilterSizeGreaterThan = sizeGT
	out.FilterSizeLessThan = sizeLT

	// Expiration sub-fields.
	if r.Expiration.set {
		out.ExpirationDays = r.Expiration.Days
		if !r.Expiration.Date.Time.IsZero() {
			out.ExpirationDate = r.Expiration.Date.Time
		}
		if r.Expiration.DeleteMarker.set {
			out.ExpiredObjectDeleteMarker = r.Expiration.DeleteMarker.val
		}
	}

	// Non-current version expiration.
	if r.NoncurrentVersionExpiration.set {
		out.NoncurrentVersionExpirationDays = r.NoncurrentVersionExpiration.NoncurrentDays
		out.NewerNoncurrentVersions = r.NoncurrentVersionExpiration.NewerNoncurrentVersions
	}

	// Abort multipart.
	if r.AbortIncompleteMultipartUpload.set {
		out.AbortMPUDaysAfterInitiation = r.AbortIncompleteMultipartUpload.DaysAfterInitiation
	}

	return out
}

// flattenFilter pulls Prefix / Tags / Size constraints out of the XML Filter
// element. Returns zero values when the field is unset; the caller falls back
// to the rule's top-level Prefix when prefix is "".
func flattenFilter(f *Filter) (prefix string, tags map[string]string, sizeGT, sizeLT int64) {
	if !f.set {
		return
	}
	if f.andSet {
		if f.And.Prefix.set {
			prefix = f.And.Prefix.val
		}
		if len(f.And.Tags) > 0 {
			tags = make(map[string]string, len(f.And.Tags))
			for _, t := range f.And.Tags {
				tags[t.Key] = t.Value
			}
		}
		sizeGT = f.And.ObjectSizeGreaterThan
		sizeLT = f.And.ObjectSizeLessThan
		return
	}
	if f.tagSet {
		tags = map[string]string{f.Tag.Key: f.Tag.Value}
	} else if f.Prefix.set {
		prefix = f.Prefix.val
	}
	sizeGT = f.ObjectSizeGreaterThan
	sizeLT = f.ObjectSizeLessThan
	return
}
