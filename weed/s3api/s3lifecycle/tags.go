package s3lifecycle

import "strings"

const tagPrefix = "X-Amz-Tagging-"

// ExtractTags extracts S3 object tags from a filer entry's Extended metadata.
// Tags are stored with the key prefix "X-Amz-Tagging-" followed by the tag key.
func ExtractTags(extended map[string][]byte) map[string]string {
	if len(extended) == 0 {
		return nil
	}
	var tags map[string]string
	for k, v := range extended {
		if strings.HasPrefix(k, tagPrefix) {
			if tags == nil {
				tags = make(map[string]string)
			}
			tags[k[len(tagPrefix):]] = string(v)
		}
	}
	return tags
}

// HasTagRules returns true if any enabled rule in the set uses tag-based filtering.
// This is used as an optimization to skip tag extraction when no rules need it.
func HasTagRules(rules []Rule) bool {
	for _, r := range rules {
		if r.Status == "Enabled" && len(r.FilterTags) > 0 {
			return true
		}
	}
	return false
}
