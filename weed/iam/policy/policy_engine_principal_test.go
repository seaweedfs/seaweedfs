package policy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrincipalMatching tests the matchesPrincipal method
func TestPrincipalMatching(t *testing.T) {
	engine := setupTestPolicyEngine(t)

	tests := []struct {
		name      string
		principal interface{}
		evalCtx   *EvaluationContext
		want      bool
	}{
		{
			name:      "plain wildcard principal",
			principal: "*",
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{},
			},
			want: true,
		},
		{
			name: "structured wildcard federated principal",
			principal: map[string]interface{}{
				"Federated": "*",
			},
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{},
			},
			want: true,
		},
		{
			name: "wildcard in array",
			principal: map[string]interface{}{
				"Federated": []interface{}{"specific-provider", "*"},
			},
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{},
			},
			want: true,
		},
		{
			name: "specific federated provider match",
			principal: map[string]interface{}{
				"Federated": "https://example.com/oidc",
			},
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://example.com/oidc",
				},
			},
			want: true,
		},
		{
			name: "specific federated provider no match",
			principal: map[string]interface{}{
				"Federated": "https://example.com/oidc",
			},
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://other.com/oidc",
				},
			},
			want: false,
		},
		{
			name: "array with specific provider match",
			principal: map[string]interface{}{
				"Federated": []string{"https://provider1.com", "https://provider2.com"},
			},
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://provider2.com",
				},
			},
			want: true,
		},
		{
			name: "AWS principal match",
			principal: map[string]interface{}{
				"AWS": "arn:aws:iam::123456789012:user/alice",
			},
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{
					"aws:PrincipalArn": "arn:aws:iam::123456789012:user/alice",
				},
			},
			want: true,
		},
		{
			name: "Service principal match",
			principal: map[string]interface{}{
				"Service": "s3.amazonaws.com",
			},
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{
					"aws:PrincipalServiceName": "s3.amazonaws.com",
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.matchesPrincipal(tt.principal, tt.evalCtx)
			assert.Equal(t, tt.want, result, "Principal matching failed for: %s", tt.name)
		})
	}
}

// TestEvaluatePrincipalValue tests the evaluatePrincipalValue method
func TestEvaluatePrincipalValue(t *testing.T) {
	engine := setupTestPolicyEngine(t)

	tests := []struct {
		name           string
		principalValue interface{}
		contextKey     string
		evalCtx        *EvaluationContext
		want           bool
	}{
		{
			name:           "wildcard string",
			principalValue: "*",
			contextKey:     "aws:FederatedProvider",
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{},
			},
			want: true,
		},
		{
			name:           "specific string match",
			principalValue: "https://example.com",
			contextKey:     "aws:FederatedProvider",
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://example.com",
				},
			},
			want: true,
		},
		{
			name:           "specific string no match",
			principalValue: "https://example.com",
			contextKey:     "aws:FederatedProvider",
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://other.com",
				},
			},
			want: false,
		},
		{
			name:           "wildcard in array",
			principalValue: []interface{}{"provider1", "*"},
			contextKey:     "aws:FederatedProvider",
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{},
			},
			want: true,
		},
		{
			name:           "array match",
			principalValue: []string{"provider1", "provider2", "provider3"},
			contextKey:     "aws:FederatedProvider",
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "provider2",
				},
			},
			want: true,
		},
		{
			name:           "array no match",
			principalValue: []string{"provider1", "provider2"},
			contextKey:     "aws:FederatedProvider",
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "provider3",
				},
			},
			want: false,
		},
		{
			name:           "missing context key",
			principalValue: "specific-value",
			contextKey:     "aws:FederatedProvider",
			evalCtx: &EvaluationContext{
				RequestContext: map[string]interface{}{},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.evaluatePrincipalValue(tt.principalValue, tt.evalCtx, tt.contextKey)
			assert.Equal(t, tt.want, result, "Principal value evaluation failed for: %s", tt.name)
		})
	}
}

// TestTrustPolicyEvaluation tests the EvaluateTrustPolicy method
func TestTrustPolicyEvaluation(t *testing.T) {
	engine := setupTestPolicyEngine(t)

	tests := []struct {
		name        string
		trustPolicy *PolicyDocument
		evalCtx     *EvaluationContext
		wantEffect  Effect
		wantErr     bool
	}{
		{
			name: "wildcard federated principal allows any provider",
			trustPolicy: &PolicyDocument{
				Version: "2012-10-17",
				Statement: []Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"Federated": "*",
						},
						Action: []string{"sts:AssumeRoleWithWebIdentity"},
					},
				},
			},
			evalCtx: &EvaluationContext{
				Action: "sts:AssumeRoleWithWebIdentity",
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://any-provider.com",
				},
			},
			wantEffect: EffectAllow,
			wantErr:    false,
		},
		{
			name: "specific federated principal matches",
			trustPolicy: &PolicyDocument{
				Version: "2012-10-17",
				Statement: []Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"Federated": "https://example.com/oidc",
						},
						Action: []string{"sts:AssumeRoleWithWebIdentity"},
					},
				},
			},
			evalCtx: &EvaluationContext{
				Action: "sts:AssumeRoleWithWebIdentity",
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://example.com/oidc",
				},
			},
			wantEffect: EffectAllow,
			wantErr:    false,
		},
		{
			name: "specific federated principal does not match",
			trustPolicy: &PolicyDocument{
				Version: "2012-10-17",
				Statement: []Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"Federated": "https://example.com/oidc",
						},
						Action: []string{"sts:AssumeRoleWithWebIdentity"},
					},
				},
			},
			evalCtx: &EvaluationContext{
				Action: "sts:AssumeRoleWithWebIdentity",
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://other.com/oidc",
				},
			},
			wantEffect: EffectDeny,
			wantErr:    false,
		},
		{
			name: "plain wildcard principal",
			trustPolicy: &PolicyDocument{
				Version: "2012-10-17",
				Statement: []Statement{
					{
						Effect:    "Allow",
						Principal: "*",
						Action:    []string{"sts:AssumeRoleWithWebIdentity"},
					},
				},
			},
			evalCtx: &EvaluationContext{
				Action: "sts:AssumeRoleWithWebIdentity",
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://any-provider.com",
				},
			},
			wantEffect: EffectAllow,
			wantErr:    false,
		},
		{
			name: "trust policy with conditions",
			trustPolicy: &PolicyDocument{
				Version: "2012-10-17",
				Statement: []Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"Federated": "*",
						},
						Action: []string{"sts:AssumeRoleWithWebIdentity"},
						Condition: map[string]map[string]interface{}{
							"StringEquals": {
								"oidc:aud": "my-app-id",
							},
						},
					},
				},
			},
			evalCtx: &EvaluationContext{
				Action: "sts:AssumeRoleWithWebIdentity",
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://provider.com",
					"oidc:aud":              "my-app-id",
				},
			},
			wantEffect: EffectAllow,
			wantErr:    false,
		},
		{
			name: "trust policy condition not met",
			trustPolicy: &PolicyDocument{
				Version: "2012-10-17",
				Statement: []Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"Federated": "*",
						},
						Action: []string{"sts:AssumeRoleWithWebIdentity"},
						Condition: map[string]map[string]interface{}{
							"StringEquals": {
								"oidc:aud": "my-app-id",
							},
						},
					},
				},
			},
			evalCtx: &EvaluationContext{
				Action: "sts:AssumeRoleWithWebIdentity",
				RequestContext: map[string]interface{}{
					"aws:FederatedProvider": "https://provider.com",
					"oidc:aud":              "wrong-app-id",
				},
			},
			wantEffect: EffectDeny,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.EvaluateTrustPolicy(context.Background(), tt.trustPolicy, tt.evalCtx)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantEffect, result.Effect, "Trust policy evaluation failed for: %s", tt.name)
			}
		})
	}
}

// TestGetPrincipalContextKey tests the context key mapping
func TestGetPrincipalContextKey(t *testing.T) {
	tests := []struct {
		name          string
		principalType string
		want          string
	}{
		{
			name:          "Federated principal",
			principalType: "Federated",
			want:          "aws:FederatedProvider",
		},
		{
			name:          "AWS principal",
			principalType: "AWS",
			want:          "aws:PrincipalArn",
		},
		{
			name:          "Service principal",
			principalType: "Service",
			want:          "aws:PrincipalServiceName",
		},
		{
			name:          "Custom principal type",
			principalType: "CustomType",
			want:          "aws:PrincipalCustomType",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getPrincipalContextKey(tt.principalType)
			assert.Equal(t, tt.want, result, "Context key mapping failed for: %s", tt.name)
		})
	}
}
