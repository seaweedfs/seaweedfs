package integration

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPolicyVariableSubstitution tests dynamic policy variables like ${oidc:sub} in Resource fields
func TestPolicyVariableSubstitution(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	// Create a role with a policy that uses ${oidc:sub} variable
	// This allows users to access only their own folder
	err := iamManager.CreateRole(ctx, "", "DynamicUserRole", &RoleDefinition{
		RoleName: "DynamicUserRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "test-oidc",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"DynamicUserPolicy"},
	})
	require.NoError(t, err)

	// Create the policy with variable substitution
	userPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Effect: "Allow",
				Action: []string{"s3:GetObject", "s3:PutObject"},
				Resource: []string{
					"arn:aws:s3:::mybucket/${oidc:sub}/*",
				},
			},
		},
	}

	// Store the policy (in a real system this would be in the policy store)
	err = iamManager.policyEngine.AddPolicy("", "DynamicUserPolicy", userPolicy)
	require.NoError(t, err)

	// Create JWT for user "alice"
	aliceJWT := createTestJWT(t, "https://test-issuer.com", "alice", "test-signing-key")

	// Assume role as "alice"
	assumeRequest := &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/DynamicUserRole",
		WebIdentityToken: aliceJWT,
		RoleSessionName:  "alice-session",
	}

	response, err := iamManager.AssumeRoleWithWebIdentity(ctx, assumeRequest)
	require.NoError(t, err)
	require.NotNil(t, response)

	// Test that the policy engine correctly substitutes ${oidc:sub} with "alice"
	evalCtx := &policy.EvaluationContext{
		Principal: "arn:aws:sts::assumed-role/DynamicUserRole/alice-session",
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::mybucket/alice/file.txt",
		RequestContext: map[string]interface{}{
			"oidc:sub": "alice",
		},
	}

	result, err := iamManager.policyEngine.Evaluate(ctx, "", evalCtx, []string{"DynamicUserPolicy"})
	require.NoError(t, err)
	assert.Equal(t, policy.EffectAllow, result.Effect, "Alice should be allowed to access her own folder")

	// Test that alice cannot access bob's folder
	evalCtx.Resource = "arn:aws:s3:::mybucket/bob/file.txt"
	result, err = iamManager.policyEngine.Evaluate(ctx, "", evalCtx, []string{"DynamicUserPolicy"})
	require.NoError(t, err)
	assert.Equal(t, policy.EffectDeny, result.Effect, "Alice should NOT be allowed to access Bob's folder")
}

// TestConditionWithNumericComparison tests numeric conditions like DurationSeconds
func TestConditionWithNumericComparison(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	// Create role with trust policy enforcing DurationSeconds <= 3600
	err := iamManager.CreateRole(ctx, "", "LimitedDurationRole", &RoleDefinition{
		RoleName: "LimitedDurationRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "test-oidc",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
					Condition: map[string]map[string]interface{}{
						"NumericLessThanEquals": {
							"sts:DurationSeconds": 3600, // Max 1 hour
						},
					},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})
	require.NoError(t, err)

	validJWT := createTestJWT(t, "https://test-issuer.com", "user", "test-signing-key")

	tests := []struct {
		name        string
		duration    int64
		shouldAllow bool
	}{
		{
			name:        "duration within limit",
			duration:    1800, // 30 mins
			shouldAllow: true,
		},
		{
			name:        "duration at limit",
			duration:    3600, // 1 hour
			shouldAllow: true,
		},
		{
			name:        "duration exceeding limit",
			duration:    7200, // 2 hours
			shouldAllow: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &sts.AssumeRoleWithWebIdentityRequest{
				RoleArn:          "arn:aws:iam::role/LimitedDurationRole",
				WebIdentityToken: validJWT,
				RoleSessionName:  "test-session",
				DurationSeconds:  &tt.duration,
			}

			response, err := iamManager.AssumeRoleWithWebIdentity(ctx, req)

			if tt.shouldAllow {
				assert.NoError(t, err, "Expected role assumption to succeed for duration %d", tt.duration)
				assert.NotNil(t, response)
			} else {
				assert.Error(t, err, "Expected role assumption to fail for duration %d", tt.duration)
				assert.Nil(t, response)
			}
		})
	}
}

// TestMultipleConditionOperators tests policies with multiple condition operators
func TestMultipleConditionOperators(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	// Create a policy with multiple conditions
	complexPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Effect: "Allow",
				Action: []string{"s3:GetObject"},
				Resource: []string{
					"arn:aws:s3:::secure-bucket/*",
				},
				Condition: map[string]map[string]interface{}{
					"StringEquals": {
						"oidc:aud": "my-app-id",
					},
					"StringLike": {
						"oidc:sub": "user-*",
					},
				},
			},
		},
	}

	err := iamManager.policyEngine.AddPolicy("", "ComplexConditionPolicy", complexPolicy)
	require.NoError(t, err)

	tests := []struct {
		name           string
		aud            string
		sub            string
		expectedEffect policy.Effect
	}{
		{
			name:           "all conditions match",
			aud:            "my-app-id",
			sub:            "user-alice",
			expectedEffect: policy.EffectAllow,
		},
		{
			name:           "aud mismatch",
			aud:            "wrong-app-id",
			sub:            "user-alice",
			expectedEffect: policy.EffectDeny,
		},
		{
			name:           "sub pattern mismatch",
			aud:            "my-app-id",
			sub:            "admin-alice",
			expectedEffect: policy.EffectDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evalCtx := &policy.EvaluationContext{
				Principal: "arn:aws:sts::assumed-role/TestRole/session",
				Action:    "s3:GetObject",
				Resource:  "arn:aws:s3:::secure-bucket/file.txt",
				RequestContext: map[string]interface{}{
					"oidc:aud": tt.aud,
					"oidc:sub": tt.sub,
				},
			}

			result, err := iamManager.policyEngine.Evaluate(ctx, "", evalCtx, []string{"ComplexConditionPolicy"})
			require.NoError(t, err)
			assert.Equal(t, tt.expectedEffect, result.Effect)
		})
	}
}
