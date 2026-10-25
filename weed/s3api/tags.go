package s3api

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type TagSet struct {
	Tag []Tag `xml:"Tag"`
}

type Tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	TagSet  TagSet   `xml:"TagSet"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (t *Tagging) ToTags() map[string]string {
	output := make(map[string]string)
	for _, tag := range t.TagSet.Tag {
		output[tag.Key] = tag.Value
	}
	return output
}

func FromTags(tags map[string]string) (t *Tagging) {
	t = &Tagging{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	for k, v := range tags {
		t.TagSet.Tag = append(t.TagSet.Tag, Tag{
			Key:   k,
			Value: v,
		})
	}
	if tagArr := t.TagSet.Tag; len(tagArr) > 0 {
		sort.SliceStable(tagArr, func(i, j int) bool {
			return tagArr[i].Key < tagArr[j].Key
		})
	}
	return
}

func parseTagsHeader(tags string) (map[string]string, error) {
	parsedTags := make(map[string]string)
	for _, v := range util.StringSplit(tags, "&") {
		// url.ParseQuery skips empty segments too, so a copy stores the
		// same tag keys that PutObject and bucket policies see.
		if v == "" {
			continue
		}
		key, value, hasValue := strings.Cut(v, "=")
		decodedKey, err := url.QueryUnescape(key)
		if err != nil {
			return nil, fmt.Errorf("failed to decode tag key '%s': %w", key, err)
		}
		if _, exists := parsedTags[decodedKey]; exists {
			return nil, fmt.Errorf("duplicate tag key '%s'", decodedKey)
		}
		if !hasValue {
			parsedTags[decodedKey] = ""
			continue
		}
		decodedValue, err := url.QueryUnescape(value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode tag value '%s': %w", value, err)
		}
		parsedTags[decodedKey] = decodedValue
	}
	return parsedTags, nil
}

func ValidateTags(tags map[string]string) error {
	return s3tables.ValidateTags(tags)
}

// maxBucketTags is how many tags a bucket's tag set can hold; an object's
// tag set holds 10.
const maxBucketTags = 50

func ValidateBucketTags(tags map[string]string) error {
	return s3tables.ValidateTagsWithLimit(tags, maxBucketTags)
}
