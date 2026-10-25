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
