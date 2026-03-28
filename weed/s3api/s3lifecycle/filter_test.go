package s3lifecycle

import "testing"

func TestMatchesPrefix(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		key    string
		want   bool
	}{
		{"empty_prefix_matches_all", "", "any/key.txt", true},
		{"exact_prefix_match", "logs/", "logs/app.log", true},
		{"prefix_mismatch", "logs/", "data/file.txt", false},
		{"key_shorter_than_prefix", "very/long/prefix/", "short", false},
		{"prefix_equals_key", "exact", "exact", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesPrefix(tt.prefix, tt.key); got != tt.want {
				t.Errorf("matchesPrefix(%q, %q) = %v, want %v", tt.prefix, tt.key, got, tt.want)
			}
		})
	}
}

func TestMatchesTags(t *testing.T) {
	tests := []struct {
		name     string
		ruleTags map[string]string
		objTags  map[string]string
		want     bool
	}{
		{"nil_rule_tags_match_all", nil, map[string]string{"a": "1"}, true},
		{"empty_rule_tags_match_all", map[string]string{}, map[string]string{"a": "1"}, true},
		{"nil_obj_tags_no_match", map[string]string{"a": "1"}, nil, false},
		{"single_tag_match", map[string]string{"env": "dev"}, map[string]string{"env": "dev", "foo": "bar"}, true},
		{"single_tag_value_mismatch", map[string]string{"env": "dev"}, map[string]string{"env": "prod"}, false},
		{"single_tag_key_missing", map[string]string{"env": "dev"}, map[string]string{"foo": "bar"}, false},
		{"multi_tag_all_match", map[string]string{"env": "dev", "tier": "hot"}, map[string]string{"env": "dev", "tier": "hot", "extra": "x"}, true},
		{"multi_tag_partial_match", map[string]string{"env": "dev", "tier": "hot"}, map[string]string{"env": "dev"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesTags(tt.ruleTags, tt.objTags); got != tt.want {
				t.Errorf("matchesTags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchesSize(t *testing.T) {
	tests := []struct {
		name        string
		greaterThan int64
		lessThan    int64
		objSize     int64
		want        bool
	}{
		{"no_constraints", 0, 0, 1000, true},
		{"only_greater_than_pass", 100, 0, 200, true},
		{"only_greater_than_fail", 100, 0, 50, false},
		{"only_greater_than_equal_fail", 100, 0, 100, false},
		{"only_less_than_pass", 0, 1000, 500, true},
		{"only_less_than_fail", 0, 1000, 2000, false},
		{"only_less_than_equal_fail", 0, 1000, 1000, false},
		{"both_constraints_pass", 100, 1000, 500, true},
		{"both_constraints_too_small", 100, 1000, 50, false},
		{"both_constraints_too_large", 100, 1000, 2000, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesSize(tt.greaterThan, tt.lessThan, tt.objSize); got != tt.want {
				t.Errorf("matchesSize(%d, %d, %d) = %v, want %v",
					tt.greaterThan, tt.lessThan, tt.objSize, got, tt.want)
			}
		})
	}
}
