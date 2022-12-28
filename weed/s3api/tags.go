package s3api

import (
	"encoding/xml"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"regexp"
	"sort"
	"strings"
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
			parsedTags[tag[0]] = tag[1]
		} else if len(tag) == 1 {
			parsedTags[tag[0]] = ""
		}
	}
	return parsedTags, nil
}

func ValidateTags(tags map[string]string) error {
	if len(tags) > 10 {
		return fmt.Errorf("validate tags: %d tags more than 10", len(tags))
	}
	for k, v := range tags {
		if len(k) > 128 {
			return fmt.Errorf("validate tags: tag key longer than 128")
		}
		validateKey, err := regexp.MatchString(`^([\p{L}\p{Z}\p{N}_.:/=+\-@]*)$`, k)
		if !validateKey || err != nil {
			return fmt.Errorf("validate tags key %s error, incorrect key", k)
		}
		if len(v) > 256 {
			return fmt.Errorf("validate tags: tag value longer than 256")
		}
		validateValue, err := regexp.MatchString(`^([\p{L}\p{Z}\p{N}_.:/=+\-@]*)$`, v)
		if !validateValue || err != nil {
			return fmt.Errorf("validate tags value %s error, incorrect value", v)
		}
	}

	return nil
}
