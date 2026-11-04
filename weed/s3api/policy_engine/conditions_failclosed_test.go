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

// A policy parsed into interface{} can hand an evaluator a null condition
// value; no operator, set-qualified or not, may panic on it.
func TestConditionEvaluatorsTolerateNilValue(t *testing.T) {
	operators := []string{
		"StringEquals", "StringNotEquals", "StringLike", "StringNotLike",
		"StringEqualsIgnoreCase", "StringNotEqualsIgnoreCase",
		"NumericEquals", "NumericNotEquals", "NumericLessThan", "NumericLessThanEquals",
		"NumericGreaterThan", "NumericGreaterThanEquals",
		"DateEquals", "DateNotEquals", "DateLessThan", "DateLessThanEquals",
		"DateGreaterThan", "DateGreaterThanEquals",
		"Bool", "IpAddress", "NotIpAddress", "ArnEquals", "ArnLike", "Null",
	}
	for _, base := range operators {
		for _, operator := range []string{base, "ForAnyValue:" + base, "ForAllValues:" + base} {
			evaluator, err := GetConditionEvaluator(operator)
			if err != nil {
				t.Fatalf("%s: %v", operator, err)
			}
			for _, contextValues := range [][]string{nil, {"x"}} {
				func() {
					defer func() {
						if r := recover(); r != nil {
							t.Errorf("%s with a nil value and context %v panicked: %v", operator, contextValues, r)
						}
					}()
					evaluator.Evaluate(nil, contextValues)
				}()
			}
		}
	}
}
