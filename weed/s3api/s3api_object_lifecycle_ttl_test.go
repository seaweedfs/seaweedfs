package s3api

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

func enabledRule(prefix string, days int) *s3lifecycle.Rule {
	return &s3lifecycle.Rule{
		ID:             "r-" + prefix,
		Status:         s3lifecycle.StatusEnabled,
		Prefix:         prefix,
		ExpirationDays: days,
	}
}

func TestResolveLifecycleTTL_NoConfig(t *testing.T) {
	if got := resolveLifecycleTTLForWrite(nil, "logs/foo", nil, 1); got != 0 {
		t.Fatalf("nil config should yield 0, got %d", got)
	}
	if got := resolveLifecycleTTLForWrite(&BucketConfig{}, "logs/foo", nil, 1); got != 0 {
		t.Fatalf("empty rules should yield 0, got %d", got)
	}
}

func TestResolveLifecycleTTL_PrefixMatch(t *testing.T) {
	cfg := &BucketConfig{
		LifecycleRules: []*s3lifecycle.Rule{enabledRule("logs/", 7)},
	}
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo.txt", nil, 1); got != 7*86400 {
		t.Fatalf("want 7d in seconds, got %d", got)
	}
	if got := resolveLifecycleTTLForWrite(cfg, "data/foo.txt", nil, 1); got != 0 {
		t.Fatalf("non-matching prefix should yield 0, got %d", got)
	}
}

func TestResolveLifecycleTTL_LongestPrefixWins(t *testing.T) {
	cfg := &BucketConfig{
		LifecycleRules: []*s3lifecycle.Rule{
			enabledRule("logs/", 30),
			enabledRule("logs/critical/", 7),
		},
	}
	// "logs/foo" → only the broader rule matches → 30d.
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", nil, 1); got != 30*86400 {
		t.Fatalf("broad-only match: got %d, want 30d", got)
	}
	// "logs/critical/x" → both match → longer prefix wins → 7d.
	if got := resolveLifecycleTTLForWrite(cfg, "logs/critical/x", nil, 1); got != 7*86400 {
		t.Fatalf("longest-prefix should win: got %d, want 7d", got)
	}
}

func TestResolveLifecycleTTL_SkipDisabled(t *testing.T) {
	rule := enabledRule("logs/", 7)
	rule.Status = s3lifecycle.StatusDisabled
	cfg := &BucketConfig{LifecycleRules: []*s3lifecycle.Rule{rule}}
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", nil, 1); got != 0 {
		t.Fatalf("disabled rule must not apply, got %d", got)
	}
}

func TestResolveLifecycleTTL_SkipNonExpirationDays(t *testing.T) {
	// Rules without Expiration.Days (e.g., NoncurrentVersionExpiration only)
	// stay on the worker — the volume-TTL fast path can't model them.
	cfg := &BucketConfig{
		LifecycleRules: []*s3lifecycle.Rule{{
			ID: "r", Status: s3lifecycle.StatusEnabled, Prefix: "logs/",
			NoncurrentVersionExpirationDays: 7,
		}},
	}
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", nil, 1); got != 0 {
		t.Fatalf("noncurrent-only rule must not apply, got %d", got)
	}
}

func TestResolveLifecycleTTL_VersionedSkipped(t *testing.T) {
	cfg := &BucketConfig{
		Versioning:     s3_constants.VersioningEnabled,
		LifecycleRules: []*s3lifecycle.Rule{enabledRule("logs/", 7)},
	}
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", nil, 1); got != 0 {
		t.Fatalf("versioned bucket must not get TTL, got %d", got)
	}
	cfg.Versioning = s3_constants.VersioningSuspended
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", nil, 1); got != 0 {
		t.Fatalf("suspended-versioning bucket must not get TTL, got %d", got)
	}
}

func TestResolveLifecycleTTL_TagFilter(t *testing.T) {
	rule := enabledRule("logs/", 7)
	rule.FilterTags = map[string]string{"k": "v"}
	cfg := &BucketConfig{LifecycleRules: []*s3lifecycle.Rule{rule}}

	// Request without the tag → rule doesn't apply.
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", nil, 1); got != 0 {
		t.Fatalf("missing tag must skip rule, got %d", got)
	}
	// Request with mismatched tag value.
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", map[string]string{"k": "other"}, 1); got != 0 {
		t.Fatalf("wrong tag value must skip rule, got %d", got)
	}
	// Matching tag → applies.
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", map[string]string{"k": "v"}, 1); got != 7*86400 {
		t.Fatalf("matching tag must apply, got %d", got)
	}
}

func TestResolveLifecycleTTL_SizeFilter(t *testing.T) {
	rule := enabledRule("logs/", 7)
	rule.FilterSizeGreaterThan = 1024
	cfg := &BucketConfig{LifecycleRules: []*s3lifecycle.Rule{rule}}

	// Below threshold → skip.
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", nil, 100); got != 0 {
		t.Fatalf("size <= threshold must skip, got %d", got)
	}
	// Above threshold → apply.
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", nil, 2048); got != 7*86400 {
		t.Fatalf("size > threshold must apply, got %d", got)
	}
	// Unknown size + size filter → skip (conservative).
	if got := resolveLifecycleTTLForWrite(cfg, "logs/foo", nil, -1); got != 0 {
		t.Fatalf("unknown size with filter must skip, got %d", got)
	}
}

func TestResolveLifecycleTTL_OverflowCappedAtMaxInt32(t *testing.T) {
	// AWS allows Days up to 2^31-1, which overflows int32 seconds.
	cfg := &BucketConfig{
		LifecycleRules: []*s3lifecycle.Rule{enabledRule("", 1<<30)},
	}
	got := resolveLifecycleTTLForWrite(cfg, "anything", nil, 1)
	if got != (1<<31)-1 {
		t.Fatalf("overflow should cap at int32 max, got %d", got)
	}
}

func TestParseRequestTags(t *testing.T) {
	t.Run("empty header", func(t *testing.T) {
		r := &http.Request{Header: http.Header{}}
		if got := parseRequestTags(r); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})
	t.Run("single tag", func(t *testing.T) {
		r := &http.Request{Header: http.Header{}}
		r.Header.Set(s3_constants.AmzObjectTagging, url.Values{"k": []string{"v"}}.Encode())
		got := parseRequestTags(r)
		if got["k"] != "v" || len(got) != 1 {
			t.Errorf("got %v, want {k:v}", got)
		}
	})
	t.Run("duplicate key returns nil", func(t *testing.T) {
		// AWS rejects duplicate tag keys; the resolver must not match a
		// tag-filtered rule with ambiguous tag input.
		r := &http.Request{Header: http.Header{}}
		r.Header.Set(s3_constants.AmzObjectTagging, "k=a&k=b")
		if got := parseRequestTags(r); got != nil {
			t.Errorf("duplicate key should return nil, got %v", got)
		}
	})
}
