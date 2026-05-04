package oidc

import (
	"reflect"
	"testing"
)

func TestExtractPrincipalTagsObjectShape(t *testing.T) {
	claims := map[string]interface{}{
		PrincipalTagsClaim: map[string]interface{}{
			"team": "infra",
			"env":  "prod",
			"empty": "",
			// non-string values are dropped
			"count": 42,
		},
	}
	got := extractPrincipalTags(claims)
	want := map[string]string{"team": "infra", "env": "prod", "empty": ""}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
}

func TestExtractPrincipalTagsArrayValueTakesFirst(t *testing.T) {
	claims := map[string]interface{}{
		PrincipalTagsClaim: map[string]interface{}{
			"team": []interface{}{"infra", "ignored"},
		},
	}
	got := extractPrincipalTags(claims)
	if got["team"] != "infra" {
		t.Fatalf("expected first array element, got %q", got["team"])
	}
}

func TestExtractPrincipalTagsAbsent(t *testing.T) {
	if got := extractPrincipalTags(map[string]interface{}{}); got != nil {
		t.Fatalf("expected nil for missing claim, got %v", got)
	}
}

func TestExtractPrincipalTagsWrongShape(t *testing.T) {
	claims := map[string]interface{}{PrincipalTagsClaim: "not-an-object"}
	if got := extractPrincipalTags(claims); got != nil {
		t.Fatalf("expected nil for non-object claim, got %v", got)
	}
}

func TestFilterPrincipalTagsAllowlist(t *testing.T) {
	in := map[string]string{"team": "infra", "env": "prod", "secret": "shh"}

	t.Run("empty allowlist denies all", func(t *testing.T) {
		if got := filterPrincipalTags(in, nil); got != nil {
			t.Fatalf("expected nil, got %v", got)
		}
	})

	t.Run("partial allowlist", func(t *testing.T) {
		got := filterPrincipalTags(in, []string{"team", "env"})
		want := map[string]string{"team": "infra", "env": "prod"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got=%v want=%v", got, want)
		}
	})

	t.Run("allowlist with no matches", func(t *testing.T) {
		if got := filterPrincipalTags(in, []string{"unknown"}); got != nil {
			t.Fatalf("expected nil, got %v", got)
		}
	})
}
