package lifecycle

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// lifecycleConfig mirrors the XML structure just enough to parse rules.
// We define a minimal local struct to avoid importing the s3api package
// (which would create a circular dependency if s3api ever imports the worker).
type lifecycleConfig struct {
	XMLName xml.Name              `xml:"LifecycleConfiguration"`
	Rules   []lifecycleConfigRule `xml:"Rule"`
}

type lifecycleConfigRule struct {
	ID         string                        `xml:"ID"`
	Status     string                        `xml:"Status"`
	Filter     lifecycleFilter               `xml:"Filter"`
	Prefix     string                        `xml:"Prefix"`
	Expiration lifecycleExpiration            `xml:"Expiration"`
	NoncurrentVersionExpiration noncurrentVersionExpiration `xml:"NoncurrentVersionExpiration"`
	AbortIncompleteMultipartUpload abortMPU   `xml:"AbortIncompleteMultipartUpload"`
}

type lifecycleFilter struct {
	Prefix                string          `xml:"Prefix"`
	Tag                   lifecycleTag    `xml:"Tag"`
	And                   lifecycleAnd    `xml:"And"`
	ObjectSizeGreaterThan int64           `xml:"ObjectSizeGreaterThan"`
	ObjectSizeLessThan    int64           `xml:"ObjectSizeLessThan"`
}

type lifecycleAnd struct {
	Prefix                string          `xml:"Prefix"`
	Tags                  []lifecycleTag  `xml:"Tag"`
	ObjectSizeGreaterThan int64           `xml:"ObjectSizeGreaterThan"`
	ObjectSizeLessThan    int64           `xml:"ObjectSizeLessThan"`
}

type lifecycleTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type lifecycleExpiration struct {
	Days                       int    `xml:"Days"`
	Date                       string `xml:"Date"`
	ExpiredObjectDeleteMarker  bool   `xml:"ExpiredObjectDeleteMarker"`
}

type noncurrentVersionExpiration struct {
	NoncurrentDays          int `xml:"NoncurrentDays"`
	NewerNoncurrentVersions int `xml:"NewerNoncurrentVersions"`
}

type abortMPU struct {
	DaysAfterInitiation int `xml:"DaysAfterInitiation"`
}

// loadLifecycleRulesFromBucket reads the lifecycle XML from a bucket's
// metadata and converts it to evaluator-friendly rules.
func loadLifecycleRulesFromBucket(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketsPath, bucket string,
) ([]s3lifecycle.Rule, error) {
	bucketDir := bucketsPath
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: bucketDir,
		Name:      bucket,
	})
	if err != nil {
		return nil, fmt.Errorf("lookup bucket %s: %w", bucket, err)
	}
	if resp.Entry == nil || resp.Entry.Extended == nil {
		return nil, nil
	}
	xmlData := resp.Entry.Extended[lifecycleXMLKey]
	if len(xmlData) == 0 {
		return nil, nil
	}
	return parseLifecycleXML(xmlData)
}

// parseLifecycleXML parses lifecycle configuration XML and converts it
// to evaluator-friendly rules.
func parseLifecycleXML(data []byte) ([]s3lifecycle.Rule, error) {
	var config lifecycleConfig
	if err := xml.NewDecoder(bytes.NewReader(data)).Decode(&config); err != nil {
		return nil, fmt.Errorf("decode lifecycle XML: %w", err)
	}

	var rules []s3lifecycle.Rule
	for _, r := range config.Rules {
		rule := s3lifecycle.Rule{
			ID:     r.ID,
			Status: r.Status,
		}

		// Resolve prefix: Filter.And.Prefix > Filter.Prefix > Rule.Prefix
		switch {
		case r.Filter.And.Prefix != "" || len(r.Filter.And.Tags) > 0 ||
			r.Filter.And.ObjectSizeGreaterThan > 0 || r.Filter.And.ObjectSizeLessThan > 0:
			rule.Prefix = r.Filter.And.Prefix
			rule.FilterTags = tagsToMap(r.Filter.And.Tags)
			rule.FilterSizeGreaterThan = r.Filter.And.ObjectSizeGreaterThan
			rule.FilterSizeLessThan = r.Filter.And.ObjectSizeLessThan
		case r.Filter.Tag.Key != "":
			rule.Prefix = r.Filter.Prefix
			rule.FilterTags = map[string]string{r.Filter.Tag.Key: r.Filter.Tag.Value}
		default:
			if r.Filter.Prefix != "" {
				rule.Prefix = r.Filter.Prefix
			} else {
				rule.Prefix = r.Prefix
			}
			rule.FilterSizeGreaterThan = r.Filter.ObjectSizeGreaterThan
			rule.FilterSizeLessThan = r.Filter.ObjectSizeLessThan
		}

		rule.ExpirationDays = r.Expiration.Days
		rule.ExpiredObjectDeleteMarker = r.Expiration.ExpiredObjectDeleteMarker
		rule.NoncurrentVersionExpirationDays = r.NoncurrentVersionExpiration.NoncurrentDays
		rule.NewerNoncurrentVersions = r.NoncurrentVersionExpiration.NewerNoncurrentVersions
		rule.AbortMPUDaysAfterInitiation = r.AbortIncompleteMultipartUpload.DaysAfterInitiation

		// Parse Date if present.
		if r.Expiration.Date != "" {
			// Date may be RFC3339 or ISO 8601 date-only.
			parsed, parseErr := parseExpirationDate(r.Expiration.Date)
			if parseErr != nil {
				glog.V(1).Infof("s3_lifecycle: skipping rule %s: invalid expiration date %q: %v", r.ID, r.Expiration.Date, parseErr)
				continue
			}
			rule.ExpirationDate = parsed
		}

		rules = append(rules, rule)
	}
	return rules, nil
}

func tagsToMap(tags []lifecycleTag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}
	return m
}

func parseExpirationDate(s string) (time.Time, error) {
	// Try RFC3339 first, then ISO 8601 date-only.
	formats := []string{
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02",
	}
	for _, f := range formats {
		t, err := time.Parse(f, s)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized date format: %s", s)
}
