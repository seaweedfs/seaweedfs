package s3lifecycle

import "testing"

func TestExtractTags(t *testing.T) {
	t.Run("extracts_tags_with_prefix", func(t *testing.T) {
		extended := map[string][]byte{
			"X-Amz-Tagging-env":     []byte("prod"),
			"X-Amz-Tagging-project": []byte("foo"),
			"Content-Type":          []byte("text/plain"),
			"X-Amz-Meta-Custom":     []byte("value"),
		}
		tags := ExtractTags(extended)
		if len(tags) != 2 {
			t.Fatalf("expected 2 tags, got %d", len(tags))
		}
		if tags["env"] != "prod" {
			t.Errorf("expected env=prod, got %q", tags["env"])
		}
		if tags["project"] != "foo" {
			t.Errorf("expected project=foo, got %q", tags["project"])
		}
	})

	t.Run("nil_extended_returns_nil", func(t *testing.T) {
		tags := ExtractTags(nil)
		if tags != nil {
			t.Errorf("expected nil, got %v", tags)
		}
	})

	t.Run("no_tags_returns_nil", func(t *testing.T) {
		extended := map[string][]byte{
			"Content-Type": []byte("text/plain"),
		}
		tags := ExtractTags(extended)
		if tags != nil {
			t.Errorf("expected nil, got %v", tags)
		}
	})

	t.Run("empty_tag_value", func(t *testing.T) {
		extended := map[string][]byte{
			"X-Amz-Tagging-empty": []byte(""),
		}
		tags := ExtractTags(extended)
		if len(tags) != 1 {
			t.Fatalf("expected 1 tag, got %d", len(tags))
		}
		if tags["empty"] != "" {
			t.Errorf("expected empty value, got %q", tags["empty"])
		}
	})
}

func TestHasTagRules(t *testing.T) {
	t.Run("has_tag_rules", func(t *testing.T) {
		rules := []Rule{
			{Status: "Enabled", FilterTags: map[string]string{"env": "dev"}},
		}
		if !HasTagRules(rules) {
			t.Error("expected true")
		}
	})

	t.Run("no_tag_rules", func(t *testing.T) {
		rules := []Rule{
			{Status: "Enabled", ExpirationDays: 30},
		}
		if HasTagRules(rules) {
			t.Error("expected false")
		}
	})

	t.Run("disabled_tag_rule", func(t *testing.T) {
		rules := []Rule{
			{Status: "Disabled", FilterTags: map[string]string{"env": "dev"}},
		}
		if HasTagRules(rules) {
			t.Error("expected false for disabled rule")
		}
	})

	t.Run("empty_rules", func(t *testing.T) {
		if HasTagRules(nil) {
			t.Error("expected false for nil rules")
		}
	})
}
