package policy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNegationSetOperators(t *testing.T) {
	engine := setupTestPolicyEngine(t)

	t.Run("ForAllValues:StringNotEquals", func(t *testing.T) {
		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "DenyAdmin",
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Condition: map[string]map[string]interface{}{
						"ForAllValues:StringNotEquals": {
							"oidc:roles": []string{"Admin"},
						},
					},
				},
			},
		}

		// All roles are NOT "Admin" -> Should Allow
		evalCtxAllow := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"oidc:roles": []string{"User", "Developer"},
			},
		}
		resultAllow, err := engine.EvaluateTrustPolicy(context.Background(), policy, evalCtxAllow)
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, resultAllow.Effect, "Should allow when ALL roles satisfy StringNotEquals Admin")

		// One role is "Admin" -> Should Deny
		evalCtxDeny := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"oidc:roles": []string{"Admin", "User"},
			},
		}
		resultDeny, err := engine.EvaluateTrustPolicy(context.Background(), policy, evalCtxDeny)
		require.NoError(t, err)
		// CURRENTLY FAILS: actually returns EffectAllow
		assert.Equal(t, EffectDeny, resultDeny.Effect, "Should deny when one role is Admin and fails StringNotEquals")
	})

	t.Run("ForAnyValue:StringNotEquals", func(t *testing.T) {
		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "Requirement",
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Condition: map[string]map[string]interface{}{
						"ForAnyValue:StringNotEquals": {
							"oidc:roles": []string{"Prohibited"},
						},
					},
				},
			},
		}

		// At least one role is NOT prohibited -> Should Allow
		evalCtxAllow := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"oidc:roles": []string{"Prohibited", "Allowed"},
			},
		}
		resultAllow, err := engine.EvaluateTrustPolicy(context.Background(), policy, evalCtxAllow)
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, resultAllow.Effect, "Should allow when at least one role is NOT Prohibited")

		// All roles are Prohibited -> Should Deny
		evalCtxDeny := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"oidc:roles": []string{"Prohibited", "Prohibited"},
			},
		}
		resultDeny, err := engine.EvaluateTrustPolicy(context.Background(), policy, evalCtxDeny)
		require.NoError(t, err)
		assert.Equal(t, EffectDeny, resultDeny.Effect, "Should deny when ALL roles are Prohibited")
	})
}
