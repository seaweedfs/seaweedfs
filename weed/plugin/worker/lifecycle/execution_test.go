package lifecycle

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

func TestMatchesDeleteMarkerRule(t *testing.T) {
	t.Run("nil_rules_legacy_fallback", func(t *testing.T) {
		if !matchesDeleteMarkerRule(nil, "any/key") {
			t.Error("nil rules should return true (legacy fallback)")
		}
	})

	t.Run("empty_rules_xml_present_no_match", func(t *testing.T) {
		rules := []s3lifecycle.Rule{}
		if matchesDeleteMarkerRule(rules, "any/key") {
			t.Error("empty rules (XML present) should return false")
		}
	})

	t.Run("matching_prefix_rule", func(t *testing.T) {
		rules := []s3lifecycle.Rule{
			{ID: "cleanup", Status: "Enabled", Prefix: "logs/", ExpiredObjectDeleteMarker: true},
		}
		if !matchesDeleteMarkerRule(rules, "logs/app.log") {
			t.Error("should match rule with matching prefix")
		}
	})

	t.Run("non_matching_prefix", func(t *testing.T) {
		rules := []s3lifecycle.Rule{
			{ID: "cleanup", Status: "Enabled", Prefix: "logs/", ExpiredObjectDeleteMarker: true},
		}
		if matchesDeleteMarkerRule(rules, "data/file.txt") {
			t.Error("should not match rule with non-matching prefix")
		}
	})

	t.Run("disabled_rule", func(t *testing.T) {
		rules := []s3lifecycle.Rule{
			{ID: "cleanup", Status: "Disabled", ExpiredObjectDeleteMarker: true},
		}
		if matchesDeleteMarkerRule(rules, "any/key") {
			t.Error("disabled rule should not match")
		}
	})

	t.Run("rule_without_delete_marker_flag", func(t *testing.T) {
		rules := []s3lifecycle.Rule{
			{ID: "expire", Status: "Enabled", ExpirationDays: 30},
		}
		if matchesDeleteMarkerRule(rules, "any/key") {
			t.Error("rule without ExpiredObjectDeleteMarker should not match")
		}
	})

	t.Run("tag_filtered_rule_no_tags_on_marker", func(t *testing.T) {
		rules := []s3lifecycle.Rule{
			{
				ID: "tagged", Status: "Enabled",
				ExpiredObjectDeleteMarker: true,
				FilterTags:               map[string]string{"env": "dev"},
			},
		}
		// Delete markers have no tags, so a tag-filtered rule should not match.
		if matchesDeleteMarkerRule(rules, "any/key") {
			t.Error("tag-filtered rule should not match delete marker (no tags)")
		}
	})
}
