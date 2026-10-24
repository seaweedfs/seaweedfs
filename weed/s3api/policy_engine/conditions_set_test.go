package policy_engine

import (
	"testing"
)

func TestConditionSetOperators(t *testing.T) {
	allowed := []string{"department", "project"}
	tests := []struct {
		name           string
		operator       string
		conditionValue interface{}
		contextValues  []string
		expected       bool
	}{
		{"ForAllValues all match", "ForAllValues:StringEquals", allowed, []string{"department", "project"}, true},
		{"ForAllValues subset matches", "ForAllValues:StringEquals", allowed, []string{"project"}, true},
		{"ForAllValues one outsider fails", "ForAllValues:StringEquals", allowed, []string{"department", "owner"}, false},
		{"ForAllValues empty set matches", "ForAllValues:StringEquals", allowed, []string{}, true},
		{"ForAllValues nil set matches", "ForAllValues:StringEquals", allowed, nil, true},
		{"ForAllValues is case sensitive", "ForAllValues:StringEquals", allowed, []string{"Department"}, false},
		{"ForAllValues StringLike all match", "ForAllValues:StringLike", "proj-*", []string{"proj-a", "proj-b"}, true},
		{"ForAllValues StringLike one outsider fails", "ForAllValues:StringLike", "proj-*", []string{"proj-a", "team"}, false},
		{"ForAllValues StringNotEquals none listed", "ForAllValues:StringNotEquals", allowed, []string{"owner", "team"}, true},
		{"ForAllValues StringNotEquals one listed fails", "ForAllValues:StringNotEquals", allowed, []string{"owner", "project"}, false},

		{"ForAnyValue one match", "ForAnyValue:StringEquals", allowed, []string{"owner", "project"}, true},
		{"ForAnyValue no match", "ForAnyValue:StringEquals", allowed, []string{"owner", "team"}, false},
		{"ForAnyValue empty set fails", "ForAnyValue:StringEquals", allowed, []string{}, false},
		{"ForAnyValue nil set fails", "ForAnyValue:StringEquals", allowed, nil, false},
		{"ForAnyValue StringLike one match", "ForAnyValue:StringLike", []string{"secret*"}, []string{"owner", "secret-level"}, true},
		{"ForAnyValue StringLike no match", "ForAnyValue:StringLike", []string{"secret*"}, []string{"owner"}, false},
		{"ForAnyValue StringNotEquals one unlisted", "ForAnyValue:StringNotEquals", allowed, []string{"department", "owner"}, true},
		{"ForAnyValue StringNotEquals all listed fails", "ForAnyValue:StringNotEquals", allowed, []string{"department", "project"}, false},
		{"ForAnyValue StringEqualsIgnoreCase", "ForAnyValue:StringEqualsIgnoreCase", allowed, []string{"PROJECT"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evaluator, err := GetConditionEvaluator(tt.operator)
			if err != nil {
				t.Fatalf("Failed to get condition evaluator: %v", err)
			}
			if got := evaluator.Evaluate(tt.conditionValue, tt.contextValues); got != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestGetConditionEvaluatorRejectsUnknownSetOperators(t *testing.T) {
	for _, op := range []string{
		"ForAllValues:StringBogus",
		"ForAnyValue:",
		"ForEachValue:StringEquals",
		"ForAllValues:ForAnyValue:StringEquals",
	} {
		if _, err := GetConditionEvaluator(op); err == nil {
			t.Errorf("GetConditionEvaluator accepted %q; expected error", op)
		}
	}
}

func TestValidateBucketPolicyAcceptsSetOperators(t *testing.T) {
	policy := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Deny",
			"Principal": "*",
			"Action": "s3:PutObject",
			"Resource": "arn:aws:s3:::test-bucket/*",
			"Condition": {
				"ForAllValues:StringNotEquals": {"s3:RequestObjectTagKeys": ["department", "project"]},
				"ForAnyValue:StringLike": {"s3:RequestObjectTagKeys": ["secret*"]}
			}
		}]
	}`
	engine := NewPolicyEngine()
	if err := engine.SetBucketPolicy("test-bucket", policy); err != nil {
		t.Fatalf("SetBucketPolicy rejected set operators: %v", err)
	}
}

func TestOriginalGrantsSupplementOnlyForAnyValue(t *testing.T) {
	const key = "s3:x-amz-grant-read"
	contextValues := map[string][]string{key: {"id=canonical"}}
	originalGrants := map[string][]string{key: {"uri=original"}}

	anyValue := PolicyConditions{
		"ForAnyValue:StringEquals": {key: NewStringOrStringSlice("uri=original")},
	}
	if !evaluateConditions(anyValue, contextValues, nil, nil, originalGrants) {
		t.Error("ForAnyValue should see the original grant values")
	}

	allValues := PolicyConditions{
		"ForAllValues:StringEquals": {key: NewStringOrStringSlice("id=canonical")},
	}
	if !evaluateConditions(allValues, contextValues, nil, nil, originalGrants) {
		t.Error("ForAllValues should be evaluated on the canonical values only")
	}
}
