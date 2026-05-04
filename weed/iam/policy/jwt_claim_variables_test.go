package policy

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStringifyClaimValue covers the scalar types that can appear in
// RequestContext so that both JSON-decoded (float64, json.Number) and typed
// (int8..int64, uint..uint64, bool) values substitute correctly.
func TestStringifyClaimValue(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		want     string
		wantOk   bool
	}{
		{name: "string", value: "alice", want: "alice", wantOk: true},
		{name: "bool true", value: true, want: "true", wantOk: true},
		{name: "bool false", value: false, want: "false", wantOk: true},
		{name: "float64 integer", value: float64(301), want: "301", wantOk: true},
		{name: "float64 fraction", value: 1.5, want: "1.5", wantOk: true},
		{name: "float32", value: float32(2.25), want: "2.25", wantOk: true},
		{name: "int", value: int(42), want: "42", wantOk: true},
		{name: "int8", value: int8(-5), want: "-5", wantOk: true},
		{name: "int16", value: int16(32000), want: "32000", wantOk: true},
		{name: "int32", value: int32(-123456), want: "-123456", wantOk: true},
		{name: "int64", value: int64(9999999999), want: "9999999999", wantOk: true},
		{name: "uint", value: uint(7), want: "7", wantOk: true},
		{name: "uint8", value: uint8(255), want: "255", wantOk: true},
		{name: "uint16", value: uint16(65535), want: "65535", wantOk: true},
		{name: "uint32", value: uint32(4000000000), want: "4000000000", wantOk: true},
		{name: "uint64", value: uint64(18000000000000000000), want: "18000000000000000000", wantOk: true},
		{name: "json.Number int", value: json.Number("301"), want: "301", wantOk: true},
		{name: "json.Number float", value: json.Number("1.5"), want: "1.5", wantOk: true},
		{name: "nil", value: nil, want: "", wantOk: false},
		{name: "slice unsupported", value: []string{"a", "b"}, want: "", wantOk: false},
		{name: "map unsupported", value: map[string]string{"a": "b"}, want: "", wantOk: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := stringifyClaimValue(tt.value)
			assert.Equal(t, tt.wantOk, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

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
