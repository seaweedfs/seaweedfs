package policy_engine

import (
	"testing"
)

func TestConditionEvaluatorsIgnoreCase(t *testing.T) {
	tests := []struct {
		name           string
		operator       string
		conditionValue interface{}
		contextValues  []string
		expected       bool
	}{
		{"StringEqualsIgnoreCase - match", "StringEqualsIgnoreCase", "ALICE", []string{"alice"}, true},
		{"StringEqualsIgnoreCase - no match", "StringEqualsIgnoreCase", "alice", []string{"bob"}, false},
		{"StringNotEqualsIgnoreCase - match", "StringNotEqualsIgnoreCase", "alice", []string{"bob"}, true},
		{"StringNotEqualsIgnoreCase - no match", "StringNotEqualsIgnoreCase", "ALICE", []string{"alice"}, false},
		{"StringLikeIgnoreCase - wildcard match", "StringLikeIgnoreCase", "TEST-*", []string{"test-value"}, true},
		{"StringLikeIgnoreCase - wildcard no match", "StringLikeIgnoreCase", "TEST-*", []string{"other-value"}, false},
		{"StringNotLikeIgnoreCase - wildcard match", "StringNotLikeIgnoreCase", "TEST-*", []string{"other-value"}, true},
		{"StringNotLikeIgnoreCase - wildcard no match", "StringNotLikeIgnoreCase", "TEST-*", []string{"test-value"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evaluator, err := GetConditionEvaluator(tt.operator)
			if err != nil {
				t.Fatalf("Failed to get condition evaluator: %v", err)
			}
			result := evaluator.Evaluate(tt.conditionValue, tt.contextValues)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}
