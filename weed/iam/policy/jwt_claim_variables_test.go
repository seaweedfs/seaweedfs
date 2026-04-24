package policy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExpandPolicyVariablesDynamicJWTClaims verifies that arbitrary JWT claims
// (not only the hardcoded jwt:sub/jwt:iss/jwt:aud/jwt:preferred_username)
// can be substituted in policy patterns when present in the request context.
// Reproduces the scenario in https://github.com/seaweedfs/seaweedfs/issues/9214
// where ${jwt:project_path} was left unsubstituted.
func TestExpandPolicyVariablesDynamicJWTClaims(t *testing.T) {
	evalCtx := &EvaluationContext{
		RequestContext: map[string]interface{}{
			"jwt:project_path":   "some/namespace/project",
			"jwt:namespace_path": "some/namespace",
			"jwt:user_login":     "mat",
			// JSON-decoded numeric claim
			"jwt:project_id": float64(301),
			// Dynamic SAML/OIDC claims
			"saml:department": "engineering",
			"oidc:tenant":     "acme",
		},
	}

	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{
			name:     "dynamic jwt claim substituted in resource arn",
			pattern:  "arn:aws:s3:::softs/${jwt:project_path}/*",
			expected: "arn:aws:s3:::softs/some/namespace/project/*",
		},
		{
			name:     "multiple dynamic jwt claims in one pattern",
			pattern:  "${jwt:namespace_path}/${jwt:user_login}/data",
			expected: "some/namespace/mat/data",
		},
		{
			name:     "dynamic numeric jwt claim is stringified",
			pattern:  "projects/${jwt:project_id}/uploads",
			expected: "projects/301/uploads",
		},
		{
			name:     "dynamic saml claim substituted",
			pattern:  "dept/${saml:department}/*",
			expected: "dept/engineering/*",
		},
		{
			name:     "dynamic oidc claim substituted",
			pattern:  "tenants/${oidc:tenant}/data",
			expected: "tenants/acme/data",
		},
		{
			name:     "missing jwt claim leaves placeholder intact",
			pattern:  "softs/${jwt:missing_claim}/*",
			expected: "softs/${jwt:missing_claim}/*",
		},
		{
			name:     "non-identity prefix is not substituted",
			pattern:  "foo/${custom:project_path}/*",
			expected: "foo/${custom:project_path}/*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := expandPolicyVariables(tt.pattern, evalCtx)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestGitLabProjectUploadPolicy reproduces the GitLab OIDC policy from issue
// #9214 end-to-end against the policy engine, proving that jwt:project_path
// (a non-hardcoded JWT claim) is now substituted in Resource patterns so that
// each project only gets access under its own prefix.
func TestGitLabProjectUploadPolicy(t *testing.T) {
	engine := NewPolicyEngine()
	require.NoError(t, engine.Initialize(&PolicyEngineConfig{
		DefaultEffect: "Deny",
		StoreType:     "memory",
	}))

	policyDoc := &PolicyDocument{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Sid:      "AllowPutInOwnFolder",
				Effect:   "Allow",
				Action:   []string{"s3:PutObject"},
				Resource: []string{"arn:aws:s3:::softs/${jwt:project_path}/*"},
			},
		},
	}

	require.NoError(t, engine.AddPolicy("", "GitlabProjectUploadPolicy", policyDoc))

	ctx := context.Background()

	mat := map[string]interface{}{
		"jwt:project_path": "some/namespace/project",
	}
	other := map[string]interface{}{
		"jwt:project_path": "other/namespace/project",
	}

	cases := []struct {
		name     string
		action   string
		resource string
		reqCtx   map[string]interface{}
		want     Effect
	}{
		{
			name:     "user writes into own project folder - allowed",
			action:   "s3:PutObject",
			resource: "arn:aws:s3:::softs/some/namespace/project/build.zip",
			reqCtx:   mat,
			want:     EffectAllow,
		},
		{
			name:     "user tries to write into another project - falls through to default Deny",
			action:   "s3:PutObject",
			resource: "arn:aws:s3:::softs/other/namespace/project/build.zip",
			reqCtx:   mat,
			want:     EffectDeny,
		},
		{
			name:     "different user writes into their own project - allowed",
			action:   "s3:PutObject",
			resource: "arn:aws:s3:::softs/other/namespace/project/build.zip",
			reqCtx:   other,
			want:     EffectAllow,
		},
		{
			name:     "user tries to GetObject - only PutObject allowed, default Deny",
			action:   "s3:GetObject",
			resource: "arn:aws:s3:::softs/some/namespace/project/build.zip",
			reqCtx:   mat,
			want:     EffectDeny,
		},
		{
			name:     "missing project_path claim - placeholder prevents match, default Deny",
			action:   "s3:PutObject",
			resource: "arn:aws:s3:::softs/some/namespace/project/build.zip",
			reqCtx:   map[string]interface{}{},
			want:     EffectDeny,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			evalCtx := &EvaluationContext{
				Principal:      "user",
				Action:         tc.action,
				Resource:       tc.resource,
				RequestContext: tc.reqCtx,
			}

			result, err := engine.Evaluate(ctx, "", evalCtx, []string{"GitlabProjectUploadPolicy"})
			require.NoError(t, err)
			assert.Equal(t, tc.want, result.Effect,
				"matching statements: %+v", result.MatchingStatements)
		})
	}
}
