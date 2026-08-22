package app

import (
	"strings"
	"testing"
)

func TestPolicyActionSuggestions_NotEmpty(t *testing.T) {
	if len(PolicyActionSuggestions) == 0 {
		t.Fatal("expected at least one suggestion")
	}
}

func TestPolicyActionSuggestions_NoDuplicates(t *testing.T) {
	seen := make(map[string]bool, len(PolicyActionSuggestions))
	for _, action := range PolicyActionSuggestions {
		if seen[action] {
			t.Errorf("duplicate action suggestion: %q", action)
		}
		seen[action] = true
	}
}

func TestPolicyActionSuggestions_AllPrefixed(t *testing.T) {
	for _, action := range PolicyActionSuggestions {
		if !strings.HasPrefix(action, "s3:") && !strings.HasPrefix(action, "s3tables:") {
			t.Errorf("action suggestion %q is not prefixed with a known service", action)
		}
	}
}

func TestPolicyActionSuggestions_NoEmptyStrings(t *testing.T) {
	for i, action := range PolicyActionSuggestions {
		if strings.TrimSpace(action) == "" {
			t.Errorf("suggestion at index %d is empty", i)
		}
	}
}
