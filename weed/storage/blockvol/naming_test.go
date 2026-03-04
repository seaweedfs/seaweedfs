package blockvol

import (
	"strings"
	"testing"
)

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"VolA", "vola"},
		{"pvc-abc-123", "pvc-abc-123"},
		{"has spaces", "has-spaces"},
		{"UPPER_CASE", "upper_case"},
		{"special!@#$%chars", "special-----chars"},
		{"dots.and-dashes", "dots.and-dashes"},
	}
	for _, tt := range tests {
		got := SanitizeFilename(tt.input)
		if got != tt.want {
			t.Errorf("SanitizeFilename(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestSanitizeIQN(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"VolA", "vola"},
		{"pvc-abc-123", "pvc-abc-123"},
		{"has spaces", "has-spaces"},
		{"under_score", "under-score"},
	}
	for _, tt := range tests {
		got := SanitizeIQN(tt.input)
		if got != tt.want {
			t.Errorf("SanitizeIQN(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestSanitizeIQN_Truncation(t *testing.T) {
	long := strings.Repeat("a", 100)
	got := SanitizeIQN(long)
	if len(got) > 64 {
		t.Errorf("SanitizeIQN should truncate to 64 chars, got %d", len(got))
	}
	// Should end with hash suffix.
	if !strings.Contains(got, "-") {
		t.Error("truncated IQN should have hash suffix separated by dash")
	}
}

func TestSanitizeConsistency(t *testing.T) {
	// SanitizeFilename and SanitizeIQN should agree on lowercasing.
	// "VolA" and "vola" should produce the same sanitized output from both.
	names := []string{"VolA", "vola"}
	for _, fn := range []struct {
		name string
		f    func(string) string
	}{
		{"SanitizeFilename", SanitizeFilename},
		{"SanitizeIQN", SanitizeIQN},
	} {
		results := make(map[string]bool)
		for _, n := range names {
			results[fn.f(n)] = true
		}
		if len(results) != 1 {
			t.Errorf("%s: 'VolA' and 'vola' should produce same result, got %v", fn.name, results)
		}
	}
}
