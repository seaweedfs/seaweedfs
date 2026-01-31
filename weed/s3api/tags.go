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
		tag := strings.Split(v, "=")
		if len(tag) == 2 {
			// URL decode both key and value
			decodedKey, err := url.QueryUnescape(tag[0])
			if err != nil {
				return nil, fmt.Errorf("failed to decode tag key '%s': %w", tag[0], err)
			}
			decodedValue, err := url.QueryUnescape(tag[1])
			if err != nil {
				return nil, fmt.Errorf("failed to decode tag value '%s': %w", tag[1], err)
			}
			parsedTags[decodedKey] = decodedValue
		} else if len(tag) == 1 {
			// URL decode key for empty value tags
			decodedKey, err := url.QueryUnescape(tag[0])
			if err != nil {
				return nil, fmt.Errorf("failed to decode tag key '%s': %w", tag[0], err)
			}
			parsedTags[decodedKey] = ""
		}
	}
	return parsedTags, nil
}

func ValidateTags(tags map[string]string) error {
	return s3tables.ValidateTags(tags)
}
