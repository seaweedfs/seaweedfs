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

	t.Run("ForAllValues:NumericEqualsVacuouslyTrue", func(t *testing.T) {
		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "AllowNumericAll",
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Condition: map[string]map[string]interface{}{
						"ForAllValues:NumericEquals": {
							"aws:MultiFactorAuthAge": []string{"3600", "7200"},
						},
					},
				},
			},
		}

		// Vacuously true: Request has NO MFA age info
		evalCtxEmpty := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"aws:MultiFactorAuthAge": []string{},
			},
		}
		resultEmpty, err := engine.EvaluateTrustPolicy(context.Background(), policy, evalCtxEmpty)
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, resultEmpty.Effect, "Should allow when numeric context is empty for ForAllValues")
	})

	t.Run("ForAllValues:BoolVacuouslyTrue", func(t *testing.T) {
		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "AllowBoolAll",
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Condition: map[string]map[string]interface{}{
						"ForAllValues:Bool": {
							"aws:SecureTransport": "true",
						},
					},
				},
			},
		}

		// Vacuously true
		evalCtxEmpty := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"aws:SecureTransport": []interface{}{},
			},
		}
		resultEmpty, err := engine.EvaluateTrustPolicy(context.Background(), policy, evalCtxEmpty)
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, resultEmpty.Effect, "Should allow when bool context is empty for ForAllValues")
	})

	t.Run("ForAllValues:DateVacuouslyTrue", func(t *testing.T) {
		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "AllowDateAll",
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Condition: map[string]map[string]interface{}{
						"ForAllValues:DateGreaterThan": {
							"aws:CurrentTime": "2020-01-01T00:00:00Z",
						},
					},
				},
			},
		}

		// Vacuously true
		evalCtxEmpty := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"aws:CurrentTime": []interface{}{},
			},
		}
		resultEmpty, err := engine.EvaluateTrustPolicy(context.Background(), policy, evalCtxEmpty)
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, resultEmpty.Effect, "Should allow when date context is empty for ForAllValues")
	})

	t.Run("ForAllValues:DateWithLabelsAsStrings", func(t *testing.T) {
		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "AllowDateStrings",
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Condition: map[string]map[string]interface{}{
						"ForAllValues:DateGreaterThan": {
							"aws:CurrentTime": "2020-01-01T00:00:00Z",
						},
					},
				},
			},
		}

		evalCtx := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"aws:CurrentTime": []string{"2021-01-01T00:00:00Z", "2022-01-01T00:00:00Z"},
			},
		}
		result, err := engine.EvaluateTrustPolicy(context.Background(), policy, evalCtx)
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, result.Effect, "Should allow when date context is a slice of strings")
	})

	t.Run("ForAllValues:BoolWithLabelsAsStrings", func(t *testing.T) {
		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "AllowBoolStrings",
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Condition: map[string]map[string]interface{}{
						"ForAllValues:Bool": {
							"aws:SecureTransport": "true",
						},
					},
				},
			},
		}

		evalCtx := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"aws:SecureTransport": []string{"true", "true"},
			},
		}
		result, err := engine.EvaluateTrustPolicy(context.Background(), policy, evalCtx)
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, result.Effect, "Should allow when bool context is a slice of strings")
	})

	t.Run("StringEqualsIgnoreCaseWithVariable", func(t *testing.T) {
		policyDoc := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:      "AllowVar",
					Effect:   "Allow",
					Action:   []string{"s3:GetObject"},
					Resource: []string{"arn:aws:s3:::bucket/*"},
					Condition: map[string]map[string]interface{}{
						"StringEqualsIgnoreCase": {
							"s3:prefix": "${aws:username}/",
						},
					},
				},
			},
		}

		err := engine.AddPolicy("", "var-policy", policyDoc)
		require.NoError(t, err)

		evalCtx := &EvaluationContext{
			Principal: "user",
			Action:    "s3:GetObject",
			Resource:  "arn:aws:s3:::bucket/ALICE/file.txt",
			RequestContext: map[string]interface{}{
				"s3:prefix":    "ALICE/",
				"aws:username": "alice",
			},
		}

		result, err := engine.Evaluate(context.Background(), "", evalCtx, []string{"var-policy"})
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, result.Effect, "Should allow when variable expands and matches case-insensitively")
	})

	t.Run("StringLike:CaseSensitivity", func(t *testing.T) {
		policyDoc := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:      "AllowCaseSensitiveLike",
					Effect:   "Allow",
					Action:   []string{"s3:GetObject"},
					Resource: []string{"arn:aws:s3:::bucket/*"},
					Condition: map[string]map[string]interface{}{
						"StringLike": {
							"s3:prefix": "Project/*",
						},
					},
				},
			},
		}

		err := engine.AddPolicy("", "like-policy", policyDoc)
		require.NoError(t, err)

		// Match: Case sensitive match
		evalCtxMatch := &EvaluationContext{
			Principal: "user",
			Action:    "s3:GetObject",
			Resource:  "arn:aws:s3:::bucket/Project/file.txt",
			RequestContext: map[string]interface{}{
				"s3:prefix": "Project/data",
			},
		}
		resultMatch, err := engine.Evaluate(context.Background(), "", evalCtxMatch, []string{"like-policy"})
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, resultMatch.Effect, "Should allow when case matches exactly")

		// Fail: Case insensitive match (should fail for StringLike)
		evalCtxFail := &EvaluationContext{
			Principal: "user",
			Action:    "s3:GetObject",
			Resource:  "arn:aws:s3:::bucket/project/file.txt",
			RequestContext: map[string]interface{}{
				"s3:prefix": "project/data", // lowercase 'p'
			},
		}
		resultFail, err := engine.Evaluate(context.Background(), "", evalCtxFail, []string{"like-policy"})
		require.NoError(t, err)
		assert.Equal(t, EffectDeny, resultFail.Effect, "Should deny when case does not match for StringLike")
	})

	t.Run("NumericNotEquals:Logic", func(t *testing.T) {
		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "DenySpecificAges",
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Condition: map[string]map[string]interface{}{
						"ForAllValues:NumericNotEquals": {
							"aws:MultiFactorAuthAge": []string{"3600", "7200"},
						},
					},
				},
			},
		}

		err := engine.AddPolicy("", "numeric-not-equals-policy", policy)
		require.NoError(t, err)

		// Fail: One age matches an excluded value (3600)
		evalCtxFail := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"aws:MultiFactorAuthAge": []string{"3600", "1800"},
			},
		}
		resultFail, err := engine.Evaluate(context.Background(), "", evalCtxFail, []string{"numeric-not-equals-policy"})
		require.NoError(t, err)
		assert.Equal(t, EffectDeny, resultFail.Effect, "Should deny when one age matches an excluded value")

		// Pass: No age matches any excluded value
		evalCtxPass := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"aws:MultiFactorAuthAge": []string{"1800", "900"},
			},
		}
		resultPass, err := engine.Evaluate(context.Background(), "", evalCtxPass, []string{"numeric-not-equals-policy"})
		require.NoError(t, err)
		assert.Equal(t, EffectAllow, resultPass.Effect, "Should allow when no age matches excluded values")
	})

	t.Run("DateNotEquals:Logic", func(t *testing.T) {
		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:    "DenySpecificTimes",
					Effect: "Allow",
					Action: []string{"sts:AssumeRole"},
					Condition: map[string]map[string]interface{}{
						"ForAllValues:DateNotEquals": {
							"aws:CurrentTime": []string{"2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"},
						},
					},
				},
			},
		}

		err := engine.AddPolicy("", "date-not-equals-policy", policy)
		require.NoError(t, err)

		// Fail: One time matches an excluded value
		evalCtxFail := &EvaluationContext{
			Principal: "user",
			Action:    "sts:AssumeRole",
			Resource:  "arn:aws:iam::role/test-role",
			RequestContext: map[string]interface{}{
				"aws:CurrentTime": []string{"2024-01-01T00:00:00Z", "2024-01-03T00:00:00Z"},
			},
		}
		resultFail, err := engine.Evaluate(context.Background(), "", evalCtxFail, []string{"date-not-equals-policy"})
		require.NoError(t, err)
		assert.Equal(t, EffectDeny, resultFail.Effect, "Should deny when one date matches an excluded value")
	})
}
