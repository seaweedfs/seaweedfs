package policy_engine

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConditionValuesLookingAlikeStayDistinct(t *testing.T) {
	key := "aws:username"
	list := PolicyConditions{"StringEquals": {key: NewStringOrStringSlice("distinct-a", "distinct-b")}}
	spaced := PolicyConditions{"StringEquals": {key: NewStringOrStringSlice("distinct-a distinct-b")}}

	if !EvaluateConditions(list, map[string][]string{key: {"distinct-a"}}, nil, nil) {
		t.Fatal("list condition must match one of its values")
	}
	if EvaluateConditions(spaced, map[string][]string{key: {"distinct-a"}}, nil, nil) {
		t.Error(`"distinct-a distinct-b" matched "distinct-a"`)
	}
	if !EvaluateConditions(spaced, map[string][]string{key: {"distinct-a distinct-b"}}, nil, nil) {
		t.Error(`"distinct-a distinct-b" did not match itself`)
	}

	empty := PolicyConditions{"StringEquals": {key: NewStringOrStringSlice()}}
	blank := PolicyConditions{"StringEquals": {key: NewStringOrStringSlice("")}}
	if EvaluateConditions(empty, map[string][]string{key: {""}}, nil, nil) {
		t.Error("empty value list matched")
	}
	if !EvaluateConditions(blank, map[string][]string{key: {""}}, nil, nil) {
		t.Error(`"" did not match itself after an empty value list`)
	}

	eval := &StringEqualsEvaluator{}
	if !eval.Evaluate([]interface{}{"json-a", "json-b"}, []string{"json-a"}) {
		t.Fatal("JSON list must match one of its values")
	}
	if eval.Evaluate([]interface{}{"json-a json-b"}, []string{"json-a"}) {
		t.Error(`JSON "json-a json-b" matched "json-a"`)
	}
}

func TestBucketPolicyConditionValuesDoNotLeakAcrossBuckets(t *testing.T) {
	engine := NewPolicyEngine()
	policy := func(bucket, effect, key, value string) string {
		return fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":%q,"Principal":"*",
			"Action":"s3:PutObject","Resource":"arn:aws:s3:::%s/*",
			"Condition":{"StringEquals":{%q:%s}}}]}`, effect, bucket, key, value)
	}
	if err := engine.SetBucketPolicy("victim", policy("victim", "Deny", "s3:x-amz-acl", `["public-read","public-read-write"]`)); err != nil {
		t.Fatal(err)
	}
	if err := engine.SetBucketPolicy("other", policy("other", "Allow", "aws:UserAgent", `"public-read public-read-write"`)); err != nil {
		t.Fatal(err)
	}
	put := func(bucket string, headers map[string]string) PolicyEvaluationResult {
		r := httptest.NewRequest(http.MethodPut, "/"+bucket+"/key", nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		return engine.EvaluatePolicyForRequest(bucket, "key", "PutObject", "*", r)
	}

	if got := put("other", map[string]string{"User-Agent": "public-read public-read-write"}); got != PolicyResultAllow {
		t.Fatalf("other bucket: got %v, want Allow", got)
	}
	if got := put("victim", map[string]string{"x-amz-acl": "public-read"}); got != PolicyResultDeny {
		t.Errorf("victim bucket: got %v, want Deny", got)
	}
}
