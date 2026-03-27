package s3api

import (
	"encoding/json"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestMatchBucketLifecycleTTLSeconds(t *testing.T) {
	rules := []bucketLifecycleTTLRule{
		{Prefix: "", TtlSec: 3600},
		{Prefix: "logs/", TtlSec: 7200},
		{Prefix: "logs/archive/", TtlSec: 10800},
	}

	if got := matchBucketLifecycleTTLSeconds(rules, "logs/file.txt"); got != 7200 {
		t.Fatalf("expected 7200 for logs/file.txt, got %d", got)
	}
	if got := matchBucketLifecycleTTLSeconds(rules, "logs/archive/file.txt"); got != 10800 {
		t.Fatalf("expected 10800 for logs/archive/file.txt, got %d", got)
	}
	if got := matchBucketLifecycleTTLSeconds(rules, "other/file.txt"); got != 3600 {
		t.Fatalf("expected 3600 for other/file.txt, got %d", got)
	}
}

func TestDecodeBucketLifecycleTTLRules(t *testing.T) {
	extended := map[string][]byte{
		s3_constants.ExtBucketLifecycleTTLRulesKey: []byte(`[{"prefix":"logs/","ttlSec":86400}]`),
	}
	rules := decodeBucketLifecycleTTLRules(extended)
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	if rules[0].Prefix != "logs/" || rules[0].TtlSec != 86400 {
		t.Fatalf("unexpected rule: %+v", rules[0])
	}

	extended[s3_constants.ExtBucketLifecycleTTLRulesKey] = []byte(`{invalid json`)
	if got := decodeBucketLifecycleTTLRules(extended); got != nil {
		t.Fatalf("expected nil rules for invalid JSON, got %+v", got)
	}
}

func TestEncodeBucketLifecycleTTLRules(t *testing.T) {
	serialized, err := encodeBucketLifecycleTTLRules([]bucketLifecycleTTLRule{
		{Prefix: "/logs/", TtlSec: 86400},
		{Prefix: "tmp/", TtlSec: 0},
		{Prefix: "", TtlSec: 3600},
	})
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	var decoded []bucketLifecycleTTLRule
	if err := json.Unmarshal(serialized, &decoded); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("expected 2 persisted rules, got %d", len(decoded))
	}
	if decoded[0].Prefix != "" || decoded[0].TtlSec != 3600 {
		t.Fatalf("unexpected first rule: %+v", decoded[0])
	}
	if decoded[1].Prefix != "logs/" || decoded[1].TtlSec != 86400 {
		t.Fatalf("unexpected second rule: %+v", decoded[1])
	}
}
