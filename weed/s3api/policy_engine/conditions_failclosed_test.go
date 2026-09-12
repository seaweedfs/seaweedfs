package policy_engine

import (
	"testing"
)

func TestEvaluateConditionsFailsClosedOnUnsupportedOperator(t *testing.T) {
	conditions := PolicyConditions{
		"StringEqualsBogus": {
			"aws:username": NewStringOrStringSlice("alice"),
		},
	}
	contextValues := map[string][]string{}
	got := EvaluateConditions(conditions, contextValues, nil, nil)
	if got {
		t.Fatalf("EvaluateConditions returned true for unsupported operator; expected false (fail closed)")
	}
}

func TestEvaluateConditionsLegacyFailsClosedOnUnsupportedOperator(t *testing.T) {
	conditions := map[string]interface{}{
		"StringEqualsBogus": map[string]interface{}{
			"aws:username": "alice",
		},
	}
	contextValues := map[string][]string{}
	got := EvaluateConditionsLegacy(conditions, contextValues, nil)
	if got {
		t.Fatalf("EvaluateConditionsLegacy returned true for unsupported operator; expected false (fail closed)")
	}
}
