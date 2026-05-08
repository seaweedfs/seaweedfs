package lifecycle_xml

import (
	"bytes"
	"encoding/xml"

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
