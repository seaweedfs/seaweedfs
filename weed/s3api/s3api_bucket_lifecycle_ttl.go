package s3api

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

type bucketLifecycleTTLRule struct {
	Prefix string `json:"prefix"`
	TtlSec int32  `json:"ttlSec"`
}

func normalizeLifecycleRulePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	prefix = strings.TrimPrefix(prefix, "/")
	if prefix == "." || prefix == "/" {
		return ""
	}
	return prefix
}

func encodeBucketLifecycleTTLRules(rules []bucketLifecycleTTLRule) ([]byte, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	sorted := make([]bucketLifecycleTTLRule, 0, len(rules))
	for _, rule := range rules {
		if rule.TtlSec <= 0 {
			continue
		}
		sorted = append(sorted, bucketLifecycleTTLRule{
			Prefix: normalizeLifecycleRulePrefix(rule.Prefix),
			TtlSec: rule.TtlSec,
		})
	}
	if len(sorted) == 0 {
		return nil, nil
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Prefix < sorted[j].Prefix
	})
	return json.Marshal(sorted)
}

func decodeBucketLifecycleTTLRules(extended map[string][]byte) []bucketLifecycleTTLRule {
	if len(extended) == 0 {
		return nil
	}
	serialized := extended[s3_constants.ExtBucketLifecycleTTLRulesKey]
	if len(serialized) == 0 {
		return nil
	}
	var rules []bucketLifecycleTTLRule
	if err := json.Unmarshal(serialized, &rules); err != nil {
		glog.Warningf("decode bucket lifecycle ttl rules: %v", err)
		return nil
	}
	return rules
}

func matchBucketLifecycleTTLSeconds(rules []bucketLifecycleTTLRule, objectKey string) int32 {
	if len(rules) == 0 {
		return 0
	}
	objectKey = normalizeLifecycleRulePrefix(objectKey)
	var bestPrefix string
	var ttlSeconds int32
	for _, rule := range rules {
		prefix := normalizeLifecycleRulePrefix(rule.Prefix)
		if prefix != "" && !strings.HasPrefix(objectKey, prefix) {
			continue
		}
		if len(prefix) >= len(bestPrefix) {
			bestPrefix = prefix
			ttlSeconds = rule.TtlSec
		}
	}
	return ttlSeconds
}

func (s3a *S3ApiServer) resolveBucketLifecycleTTLSeconds(bucket, filePath string) int32 {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone || config == nil || config.Entry == nil {
		return 0
	}
	bucketPrefix := s3a.bucketDir(bucket) + "/"
	objectKey := strings.TrimPrefix(filePath, bucketPrefix)
	if objectKey == filePath {
		return 0
	}
	rules := decodeBucketLifecycleTTLRules(config.Entry.Extended)
	return matchBucketLifecycleTTLSeconds(rules, objectKey)
}

func (s3a *S3ApiServer) persistBucketLifecycleTTLRules(bucket string, rules []bucketLifecycleTTLRule) error {
	serialized, err := encodeBucketLifecycleTTLRules(rules)
	if err != nil {
		return fmt.Errorf("encode lifecycle ttl rules: %w", err)
	}
	errCode := s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		if config.Entry == nil {
			return fmt.Errorf("bucket %s has no entry", bucket)
		}
		if config.Entry.Extended == nil {
			config.Entry.Extended = make(map[string][]byte)
		}
		if len(serialized) == 0 {
			delete(config.Entry.Extended, s3_constants.ExtBucketLifecycleTTLRulesKey)
		} else {
			config.Entry.Extended[s3_constants.ExtBucketLifecycleTTLRulesKey] = serialized
		}
		return nil
	})
	if errCode != s3err.ErrNone {
		return fmt.Errorf("persist lifecycle ttl rules: %v", errCode)
	}
	return nil
}
