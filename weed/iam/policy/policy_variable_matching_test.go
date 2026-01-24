package policy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPolicyVariableMatchingInActionsAndResources tests that Actions and Resources
// now support policy variables like ${aws:username} just like string conditions do
func TestPolicyVariableMatchingInActionsAndResources(t *testing.T) {
	engine := NewPolicyEngine()
	config := &PolicyEngineConfig{
		DefaultEffect: "Deny",
		StoreType:     "memory",
	}

	err := engine.Initialize(config)
	require.NoError(t, err)

	ctx := context.Background()
	filerAddress := ""

	// Create a policy that uses policy variables in Action and Resource fields
	policyDoc := &PolicyDocument{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Sid:    "AllowUserSpecificActions",
				Effect: "Allow",
				Action: []string{
					"s3:Get*",                  // Regular wildcard
					"s3:${aws:principaltype}*", // Policy variable in action
				},
				Resource: []string{
					"arn:aws:s3:::user-${aws:username}/*",    // Policy variable in resource
					"arn:aws:s3:::shared/${saml:username}/*", // Different policy variable
				},
			},
		},
	}

	err = engine.AddPolicy(filerAddress, "user-specific-policy", policyDoc)
	require.NoError(t, err)

	tests := []struct {
		name           string
		principal      string
		action         string
		resource       string
		requestContext map[string]interface{}
		expectedEffect Effect
		description    string
	}{
		{
			name:      "policy_variable_in_action_matches",
			principal: "test-user",
			action:    "s3:AssumedRole", // Should match s3:${aws:principaltype}* when principaltype=AssumedRole
			resource:  "arn:aws:s3:::user-testuser/file.txt",
			requestContext: map[string]interface{}{
				"aws:username":      "testuser",
				"aws:principaltype": "AssumedRole",
			},
			expectedEffect: EffectAllow,
			description:    "Action with policy variable should match when variable is expanded",
		},
		{
			name:      "policy_variable_in_resource_matches",
			principal: "alice",
			action:    "s3:GetObject",
			resource:  "arn:aws:s3:::user-alice/document.pdf", // Should match user-${aws:username}/*
			requestContext: map[string]interface{}{
				"aws:username": "alice",
			},
			expectedEffect: EffectAllow,
			description:    "Resource with policy variable should match when variable is expanded",
		},
		{
			name:      "saml_username_variable_in_resource",
			principal: "bob",
			action:    "s3:GetObject",
			resource:  "arn:aws:s3:::shared/bob/data.json", // Should match shared/${saml:username}/*
			requestContext: map[string]interface{}{
				"saml:username": "bob",
			},
			expectedEffect: EffectAllow,
			description:    "SAML username variable should be expanded in resource patterns",
		},
		{
			name:      "policy_variable_no_match_wrong_user",
			principal: "charlie",
			action:    "s3:GetObject",
			resource:  "arn:aws:s3:::user-alice/file.txt", // charlie trying to access alice's files
			requestContext: map[string]interface{}{
				"aws:username": "charlie",
			},
			expectedEffect: EffectDeny,
			description:    "Policy variable should prevent access when username doesn't match",
		},
		{
			name:           "missing_policy_variable_context",
			principal:      "dave",
			action:         "s3:GetObject",
			resource:       "arn:aws:s3:::user-dave/file.txt",
			requestContext: map[string]interface{}{
				// Missing aws:username context
			},
			expectedEffect: EffectDeny,
			description:    "Missing policy variable context should result in no match",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evalCtx := &EvaluationContext{
				Principal:      tt.principal,
				Action:         tt.action,
				Resource:       tt.resource,
				RequestContext: tt.requestContext,
			}

			result, err := engine.Evaluate(ctx, filerAddress, evalCtx, []string{"user-specific-policy"})
			require.NoError(t, err, "Policy evaluation should not error")

			assert.Equal(t, tt.expectedEffect, result.Effect,
				"Test %s: %s. Expected %s but got %s",
				tt.name, tt.description, tt.expectedEffect, result.Effect)
		})
	}
}

// TestActionResourceConsistencyWithStringConditions verifies that Actions, Resources,
// and string conditions all use the same AWS IAM-compliant matching logic
func TestActionResourceConsistencyWithStringConditions(t *testing.T) {
	engine := NewPolicyEngine()
	config := &PolicyEngineConfig{
		DefaultEffect: "Deny",
		StoreType:     "memory",
	}

	err := engine.Initialize(config)
	require.NoError(t, err)

	ctx := context.Background()
	filerAddress := ""

	// Policy that uses case-insensitive matching in all three areas
	policyDoc := &PolicyDocument{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Sid:      "CaseInsensitiveMatching",
				Effect:   "Allow",
				Action:   []string{"S3:GET*"},                    // Uppercase action pattern
				Resource: []string{"arn:aws:s3:::TEST-BUCKET/*"}, // Uppercase resource pattern
				Condition: map[string]map[string]interface{}{
					"StringLikeIgnoreCase": {
						"s3:RequestedRegion": "US-*", // Uppercase condition pattern
					},
				},
			},
		},
	}

	err = engine.AddPolicy(filerAddress, "case-insensitive-policy", policyDoc)
	require.NoError(t, err)

	evalCtx := &EvaluationContext{
		Principal: "test-user",
		Action:    "s3:getobject",                      // lowercase action
		Resource:  "arn:aws:s3:::test-bucket/file.txt", // lowercase resource
		RequestContext: map[string]interface{}{
			"s3:RequestedRegion": "us-east-1", // lowercase condition value
		},
	}

	result, err := engine.Evaluate(ctx, filerAddress, evalCtx, []string{"case-insensitive-policy"})
	require.NoError(t, err)

	// All should match due to case-insensitive AWS IAM-compliant matching
	assert.Equal(t, EffectAllow, result.Effect,
		"Actions, Resources, and Conditions should all use case-insensitive AWS IAM matching")

	// Verify that matching statements were found
	require.Len(t, result.MatchingStatements, 1,
		"Should have exactly one matching statement")
	assert.Equal(t, "Allow", string(result.MatchingStatements[0].Effect),
		"Matching statement should have Allow effect")
}
