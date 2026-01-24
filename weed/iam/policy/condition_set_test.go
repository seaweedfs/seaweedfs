package policy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConditionSetOperators(t *testing.T) {
	engine := setupTestPolicyEngine(t)

	t.Run("ForAnyValue:StringEquals", func(t *testing.T) {
		trustPolicy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "AllowOIDC",
					Effect: "Allow",
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
					Condition: map[string]map[string]interface{}{
						"ForAnyValue:StringEquals": {
							"oidc:roles": []string{"Dev.SeaweedFS.TestBucket.ReadWrite", "Dev.SeaweedFS.Admin"},
						},
					},
				},
			},
		}

		// Match: Admin is in the requested roles
		evalCtxMatch := &EvaluationContext{
			Principal: "web-identity-user",
			Action:    "sts:AssumeRoleWithWebIdentity",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"oidc:roles": []string{"Dev.SeaweedFS.Admin", "OtherRole"},
			},
		}
		resultMatch, err := engine.EvaluateTrustPolicy(context.Background(), trustPolicy, evalCtxMatch)
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, resultMatch.Effect)

		// No Match
		evalCtxNoMatch := &EvaluationContext{
			Principal: "web-identity-user",
			Action:    "sts:AssumeRoleWithWebIdentity",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"oidc:roles": []string{"OtherRole1", "OtherRole2"},
			},
		}
		resultNoMatch, err := engine.EvaluateTrustPolicy(context.Background(), trustPolicy, evalCtxNoMatch)
		require.NoError(t, err)
		assert.Equal(t, EffectDeny, resultNoMatch.Effect)
	})

	t.Run("ForAllValues:StringEquals", func(t *testing.T) {
		trustPolicyAll := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "AllowOIDCAll",
					Effect: "Allow",
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
					Condition: map[string]map[string]interface{}{
						"ForAllValues:StringEquals": {
							"oidc:roles": []string{"RoleA", "RoleB", "RoleC"},
						},
					},
				},
			},
		}

		// Match: All requested roles ARE in the allowed set
		evalCtxAllMatch := &EvaluationContext{
			Principal: "web-identity-user",
			Action:    "sts:AssumeRoleWithWebIdentity",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"oidc:roles": []string{"RoleA", "RoleB"},
			},
		}
		resultAllMatch, err := engine.EvaluateTrustPolicy(context.Background(), trustPolicyAll, evalCtxAllMatch)
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, resultAllMatch.Effect)

		// Fail: RoleD is NOT in the allowed set
		evalCtxAllFail := &EvaluationContext{
			Principal: "web-identity-user",
			Action:    "sts:AssumeRoleWithWebIdentity",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"oidc:roles": []string{"RoleA", "RoleD"},
			},
		}
		resultAllFail, err := engine.EvaluateTrustPolicy(context.Background(), trustPolicyAll, evalCtxAllFail)
		require.NoError(t, err)
		assert.Equal(t, EffectDeny, resultAllFail.Effect)

		// Vacuously true: Request has NO roles
		evalCtxEmpty := &EvaluationContext{
			Principal: "web-identity-user",
			Action:    "sts:AssumeRoleWithWebIdentity",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"oidc:roles": []string{},
			},
		}
		resultEmpty, err := engine.EvaluateTrustPolicy(context.Background(), trustPolicyAll, evalCtxEmpty)
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, resultEmpty.Effect)
	})
}
