package oidc

import (
	"reflect"
	"testing"
)

func TestExtractClaimPoliciesString(t *testing.T) {
	got := extractClaimPolicies(map[string]interface{}{"policy": "readonly"}, "policy")
	if !reflect.DeepEqual(got, []string{"readonly"}) {
		t.Fatalf("got=%v", got)
	}
}

func TestExtractClaimPoliciesCommaSeparated(t *testing.T) {
	got := extractClaimPolicies(map[string]interface{}{"policy": "readonly, billing , "}, "policy")
	if !reflect.DeepEqual(got, []string{"readonly", "billing"}) {
		t.Fatalf("got=%v", got)
	}
}

func TestExtractClaimPoliciesArray(t *testing.T) {
	got := extractClaimPolicies(map[string]interface{}{
		"policy": []interface{}{"readonly", "billing", 42, ""}, // non-string + empty filtered
	}, "policy")
	if !reflect.DeepEqual(got, []string{"readonly", "billing"}) {
		t.Fatalf("got=%v", got)
	}
}

func TestExtractClaimPoliciesMissing(t *testing.T) {
	if got := extractClaimPolicies(map[string]interface{}{}, "policy"); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestExtractClaimPoliciesEmptyClaimName(t *testing.T) {
	// Provider not in claim-mode -> never read the claim, even if present.
	if got := extractClaimPolicies(map[string]interface{}{"policy": "readonly"}, ""); got != nil {
		t.Fatalf("empty claim name should return nil, got %v", got)
	}
}

func TestExtractClaimPoliciesUnsupportedShape(t *testing.T) {
	// Numeric / object / bool values should be ignored, not panic.
	cases := []interface{}{
		42,
		3.14,
		map[string]interface{}{"x": "y"},
		true,
	}
	for _, v := range cases {
		if got := extractClaimPolicies(map[string]interface{}{"policy": v}, "policy"); got != nil {
			t.Fatalf("value %v: expected nil, got %v", v, got)
		}
	}
}
