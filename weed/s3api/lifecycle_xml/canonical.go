package lifecycle_xml

import (
	"bytes"
	"encoding/xml"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// Parse decodes a BucketLifecycleConfiguration XML body into the wire-form
// Lifecycle struct.
func Parse(xmlBytes []byte) (*Lifecycle, error) {
	var lc Lifecycle
	if err := xml.NewDecoder(bytes.NewReader(xmlBytes)).Decode(&lc); err != nil {
		return nil, err
	}
	return &lc, nil
}

// ParseCanonical is the one-shot path most non-server callers want:
// raw XML in, []*s3lifecycle.Rule out.
func ParseCanonical(xmlBytes []byte) ([]*s3lifecycle.Rule, error) {
	lc, err := Parse(xmlBytes)
	if err != nil {
		return nil, err
	}
	return LifecycleToCanonical(lc), nil
}

// LifecycleToCanonical flattens the XML-deserialized Lifecycle into the
// engine's flat Rule shape. The optional <Filter> element may contain
// <Prefix> | <Tag> | <And>, or be absent (in which case the older top-level
// <Prefix> applies).
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

	if r.Expiration.set {
		out.ExpirationDays = r.Expiration.Days
		if !r.Expiration.Date.Time.IsZero() {
			out.ExpirationDate = r.Expiration.Date.Time
		}
		if r.Expiration.DeleteMarker.set {
			out.ExpiredObjectDeleteMarker = r.Expiration.DeleteMarker.val
		}
	}

	if r.NoncurrentVersionExpiration.set {
		out.NoncurrentVersionExpirationDays = r.NoncurrentVersionExpiration.NoncurrentDays
		out.NewerNoncurrentVersions = r.NoncurrentVersionExpiration.NewerNoncurrentVersions
	}

	if r.AbortIncompleteMultipartUpload.set {
		out.AbortMPUDaysAfterInitiation = r.AbortIncompleteMultipartUpload.DaysAfterInitiation
	}

	return out
}

// CanonicalToLifecycle is the inverse of LifecycleToCanonical: it builds a
// marshalable Lifecycle from the engine's flat Rule shape. Only the fields
// s3lifecycle.Rule can represent are populated — there is no way back to
// Transition / NoncurrentVersionTransition, which the canonical form never
// carries.
func CanonicalToLifecycle(rules []*s3lifecycle.Rule) *Lifecycle {
	lc := &Lifecycle{
		Rules: make([]Rule, 0, len(rules)),
	}
	for _, r := range rules {
		lc.Rules = append(lc.Rules, ruleFromCanonical(r))
	}
	return lc
}

// MarshalCanonical serializes the canonical rules straight to a
// BucketLifecycleConfiguration XML document, mirroring ParseCanonical.
func MarshalCanonical(rules []*s3lifecycle.Rule) ([]byte, error) {
	out, err := xml.Marshal(CanonicalToLifecycle(rules))
	if err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), out...), nil
}

func ruleFromCanonical(r *s3lifecycle.Rule) Rule {
	out := Rule{
		ID:     r.ID,
		Status: RuleStatus(r.Status),
		Filter: filterFromCanonical(r.Prefix, r.FilterTags, r.FilterSizeGreaterThan, r.FilterSizeLessThan),
	}

	if r.ExpirationDays > 0 || !r.ExpirationDate.IsZero() || r.ExpiredObjectDeleteMarker {
		out.Expiration = Expiration{
			set:  true,
			Days: r.ExpirationDays,
		}
		if !r.ExpirationDate.IsZero() {
			out.Expiration.Date = ExpirationDate{Time: r.ExpirationDate}
		}
		if r.ExpiredObjectDeleteMarker {
			out.Expiration.DeleteMarker = ExpireDeleteMarker{val: true, set: true}
		}
	}

	if r.NoncurrentVersionExpirationDays > 0 || r.NewerNoncurrentVersions > 0 {
		out.NoncurrentVersionExpiration = NoncurrentVersionExpiration{
			set:                     true,
			NoncurrentDays:          r.NoncurrentVersionExpirationDays,
			NewerNoncurrentVersions: r.NewerNoncurrentVersions,
		}
	}

	if r.AbortMPUDaysAfterInitiation > 0 {
		out.AbortIncompleteMultipartUpload = AbortIncompleteMultipartUpload{
			set:                 true,
			DaysAfterInitiation: r.AbortMPUDaysAfterInitiation,
		}
	}

	return out
}

// filterFromCanonical is the inverse of flattenFilter: it picks the
// narrowest Filter shape that represents the given prefix/tags/size bounds,
// matching what Filter.MarshalXML (single Prefix|Tag branch plus optional
// size bounds) and its And branch can each express.
func filterFromCanonical(prefix string, tags map[string]string, sizeGT, sizeLT int64) Filter {
	hasSize := sizeGT > 0 || sizeLT > 0

	// A size range (GT and/or LT together) describes one attribute — object
	// size — so it doesn't need <And> on its own; it only forces <And> when
	// paired with a different attribute (prefix or tag).
	discriminants := 0
	if prefix != "" {
		discriminants++
	}
	discriminants += len(tags)
	if hasSize {
		discriminants++
	}

	f := Filter{set: true, ObjectSizeGreaterThan: sizeGT, ObjectSizeLessThan: sizeLT}

	switch {
	case discriminants > 1:
		f.andSet = true
		f.And = And{
			ObjectSizeGreaterThan: sizeGT,
			ObjectSizeLessThan:    sizeLT,
		}
		if prefix != "" {
			f.And.Prefix = NewPrefix(prefix)
		}
		if len(tags) > 0 {
			keys := make([]string, 0, len(tags))
			for k := range tags {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			f.And.Tags = make([]Tag, 0, len(keys))
			for _, k := range keys {
				f.And.Tags = append(f.And.Tags, Tag{Key: k, Value: tags[k]})
			}
		}
		// The And branch carries its own size bounds; the enclosing
		// Filter only emits them on the non-And path (see
		// Filter.MarshalXML), so clear them here to avoid duplication.
		f.ObjectSizeGreaterThan = 0
		f.ObjectSizeLessThan = 0
	case len(tags) == 1:
		f.tagSet = true
		for k, v := range tags {
			f.Tag = Tag{Key: k, Value: v}
		}
	case hasSize:
		// Single discriminant and it's a size range: the bounds set above
		// already cover it — no <Prefix> (not even an empty one; nothing
		// was requested) and no <And> (nothing else to combine with).
	default:
		// Either a single prefix or no discriminant at all (whole-bucket
		// filter) — both are expressed as a <Prefix> element, empty or not.
		f.Prefix = NewPrefix(prefix)
	}

	return f
}

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
