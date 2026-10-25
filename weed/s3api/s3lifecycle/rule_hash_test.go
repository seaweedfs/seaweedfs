package s3lifecycle

import (
	"testing"
)

func TestRuleHash_Stable(t *testing.T) {
	r := &Rule{Status: StatusEnabled, ExpirationDays: 30, Prefix: "logs/"}
	a := RuleHash(r)
	b := RuleHash(r)
	if a != b {
		t.Fatalf("hash should be deterministic, got %x vs %x", a, b)
	}
}

func TestRuleHash_TagOrderInvariant(t *testing.T) {
	r1 := &Rule{ExpirationDays: 30, FilterTags: map[string]string{"a": "1", "b": "2"}}
	r2 := &Rule{ExpirationDays: 30, FilterTags: map[string]string{"b": "2", "a": "1"}}
	if RuleHash(r1) != RuleHash(r2) {
		t.Fatalf("tag order should not affect hash")
	}
}

func TestRuleHash_PrefixTrailingSlashMattersToHash(t *testing.T) {
	// "logs" matches "logs", "logsmore/x", "logs/x" (literal HasPrefix);
	// "logs/" matches only "logs/x". Different match sets -> different rules
	// -> different hashes. Collapsing would let an edit silently reuse
	// state for a rule that no longer matches the same objects.
	r1 := &Rule{ExpirationDays: 30, Prefix: "logs"}
	r2 := &Rule{ExpirationDays: 30, Prefix: "logs/"}
	if RuleHash(r1) == RuleHash(r2) {
		t.Fatalf("trailing slash MUST affect hash; rules match different objects")
	}
}

func TestRuleHash_IDIgnored(t *testing.T) {
	r1 := &Rule{ID: "first", ExpirationDays: 30}
	r2 := &Rule{ID: "renamed", ExpirationDays: 30}
	if RuleHash(r1) != RuleHash(r2) {
		t.Fatalf("ID change should not affect hash")
	}
}

func TestRuleHash_StatusIgnored(t *testing.T) {
	r1 := &Rule{Status: StatusEnabled, ExpirationDays: 30}
	r2 := &Rule{Status: StatusDisabled, ExpirationDays: 30}
	if RuleHash(r1) != RuleHash(r2) {
		t.Fatalf("status flip should not affect hash (state continuity)")
	}
}

func TestRuleHash_DifferentDaysHashDifferent(t *testing.T) {
	r1 := &Rule{ExpirationDays: 30}
	r2 := &Rule{ExpirationDays: 31}
	if RuleHash(r1) == RuleHash(r2) {
		t.Fatalf("different days must hash differently")
	}
}

func TestRuleHash_DifferentActionTypesHashDifferent(t *testing.T) {
	r1 := &Rule{ExpirationDays: 30}
	r2 := &Rule{NoncurrentVersionExpirationDays: 30}
	r3 := &Rule{AbortMPUDaysAfterInitiation: 30}
	if RuleHash(r1) == RuleHash(r2) || RuleHash(r2) == RuleHash(r3) || RuleHash(r1) == RuleHash(r3) {
		t.Fatalf("different action types must hash differently")
	}
}

func TestRuleHash_FilterMatters(t *testing.T) {
	r1 := &Rule{ExpirationDays: 30, Prefix: "logs/"}
	r2 := &Rule{ExpirationDays: 30, Prefix: "data/"}
	r3 := &Rule{ExpirationDays: 30, Prefix: "logs/", FilterTags: map[string]string{"env": "prod"}}
	r4 := &Rule{ExpirationDays: 30, FilterSizeGreaterThan: 1000}
	if RuleHash(r1) == RuleHash(r2) || RuleHash(r1) == RuleHash(r3) || RuleHash(r1) == RuleHash(r4) {
		t.Fatalf("different filters must hash differently")
	}
}

func TestRuleHash_NilSafe(t *testing.T) {
	if h := RuleHash(nil); h != ([8]byte{}) {
		t.Fatalf("nil rule should yield zero hash, got %x", h)
	}
}

func TestRuleHash_TagDelimiterCollisionResistant(t *testing.T) {
	// With a naive "tag=K=V\n" encoding, ("a=b", "c") and ("a", "b=c")
	// serialize identically. Length-prefixed encoding must keep them
	// distinct.
	r1 := &Rule{ExpirationDays: 30, FilterTags: map[string]string{"a=b": "c"}}
	r2 := &Rule{ExpirationDays: 30, FilterTags: map[string]string{"a": "b=c"}}
	if RuleHash(r1) == RuleHash(r2) {
		t.Fatalf("delimiter collision: tag(a=b,c) and tag(a,b=c) hash equal")
	}
}

func TestRuleHash_TagNewlineCollisionResistant(t *testing.T) {
	// "a\nb" -> single tag with a key that embeds a newline; naively this
	// could be split across lines and matched by an unrelated 2-tag rule.
	r1 := &Rule{ExpirationDays: 30, FilterTags: map[string]string{"a\nb": "c"}}
	r2 := &Rule{ExpirationDays: 30, FilterTags: map[string]string{"a": "b", "c": ""}}
	if RuleHash(r1) == RuleHash(r2) {
		t.Fatalf("delimiter collision via embedded newline")
	}
}

func TestRuleHash_PrefixSeparatorIsolation(t *testing.T) {
	// A prefix containing whatever the previous encoder used as a separator
	// ("=" / "\n") must not be able to "escape" into another field.
	r1 := &Rule{ExpirationDays: 30, Prefix: "logs\nexp_days=99"}
	r2 := &Rule{ExpirationDays: 99, Prefix: "logs"}
	if RuleHash(r1) == RuleHash(r2) {
		t.Fatalf("prefix-side delimiter forgery hashes equal")
	}
}
