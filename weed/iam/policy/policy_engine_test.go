package policy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPolicyEngineInitialization tests policy engine initialization
func TestPolicyEngineInitialization(t *testing.T) {
	tests := []struct {
		name    string
		config  *PolicyEngineConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: &PolicyEngineConfig{
				DefaultEffect: "Deny",
				StoreType:     "memory",
			},
			wantErr: false,
		},
		{
			name: "invalid default effect",
			config: &PolicyEngineConfig{
				DefaultEffect: "Invalid",
				StoreType:     "memory",
			},
			wantErr: true,
		},
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewPolicyEngine()

			err := engine.Initialize(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.True(t, engine.IsInitialized())
			}
		})
	}
}

// TestPolicyDocumentValidation tests policy document structure validation
func TestPolicyDocumentValidation(t *testing.T) {
	tests := []struct {
		name     string
		policy   *PolicyDocument
		wantErr  bool
		errorMsg string
	}{
		{
			name: "valid policy document",
			policy: &PolicyDocument{
				Version: "2012-10-17",
				Statement: []Statement{
					{
						Sid:      "AllowS3Read",
						Effect:   "Allow",
						Action:   []string{"s3:GetObject", "s3:ListBucket"},
						Resource: []string{"arn:aws:s3:::mybucket/*"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "missing version",
			policy: &PolicyDocument{
				Statement: []Statement{
					{
						Effect:   "Allow",
						Action:   []string{"s3:GetObject"},
						Resource: []string{"arn:aws:s3:::mybucket/*"},
					},
				},
			},
			wantErr:  true,
			errorMsg: "version is required",
		},
		{
			name: "empty statements",
			policy: &PolicyDocument{
				Version:   "2012-10-17",
				Statement: []Statement{},
			},
			wantErr:  true,
			errorMsg: "at least one statement is required",
		},
		{
			name: "invalid effect",
			policy: &PolicyDocument{
				Version: "2012-10-17",
				Statement: []Statement{
					{
						Effect:   "Maybe",
						Action:   []string{"s3:GetObject"},
						Resource: []string{"arn:aws:s3:::mybucket/*"},
					},
				},
			},
			wantErr:  true,
			errorMsg: "invalid effect",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePolicyDocument(tt.policy)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestPolicyEvaluation tests policy evaluation logic
func TestPolicyEvaluation(t *testing.T) {
	engine := setupTestPolicyEngine(t)

	// Add test policies
	readPolicy := &PolicyDocument{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Sid:    "AllowS3Read",
				Effect: "Allow",
				Action: []string{"s3:GetObject", "s3:ListBucket"},
				Resource: []string{
					"arn:aws:s3:::public-bucket/*", // For object operations
					"arn:aws:s3:::public-bucket",   // For bucket operations
				},
			},
		},
	}

	err := engine.AddPolicy("", "read-policy", readPolicy)
	require.NoError(t, err)

	denyPolicy := &PolicyDocument{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Sid:      "DenyS3Delete",
				Effect:   "Deny",
				Action:   []string{"s3:DeleteObject"},
				Resource: []string{"arn:aws:s3:::*"},
			},
		},
	}

	err = engine.AddPolicy("", "deny-policy", denyPolicy)
	require.NoError(t, err)

	tests := []struct {
		name     string
		context  *EvaluationContext
		policies []string
		want     Effect
	}{
		{
			name: "allow read access",
			context: &EvaluationContext{
				Principal: "user:alice",
				Action:    "s3:GetObject",
				Resource:  "arn:aws:s3:::public-bucket/file.txt",
				RequestContext: map[string]interface{}{
					"aws:SourceIp": "192.168.1.100",
				},
			},
			policies: []string{"read-policy"},
			want:     EffectAllow,
		},
		{
			name: "deny delete access (explicit deny)",
			context: &EvaluationContext{
				Principal: "user:alice",
				Action:    "s3:DeleteObject",
				Resource:  "arn:aws:s3:::public-bucket/file.txt",
			},
			policies: []string{"read-policy", "deny-policy"},
			want:     EffectDeny,
		},
		{
			name: "deny by default (no matching policy)",
			context: &EvaluationContext{
				Principal: "user:alice",
				Action:    "s3:PutObject",
				Resource:  "arn:aws:s3:::public-bucket/file.txt",
			},
			policies: []string{"read-policy"},
			want:     EffectDeny,
		},
		{
			name: "allow with wildcard action",
			context: &EvaluationContext{
				Principal: "user:admin",
				Action:    "s3:ListBucket",
				Resource:  "arn:aws:s3:::public-bucket",
			},
			policies: []string{"read-policy"},
			want:     EffectAllow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Evaluate(context.Background(), "", tt.context, tt.policies)

			assert.NoError(t, err)
			assert.Equal(t, tt.want, result.Effect)

			// Verify evaluation details
			assert.NotNil(t, result.EvaluationDetails)
			assert.Equal(t, tt.context.Action, result.EvaluationDetails.Action)
			assert.Equal(t, tt.context.Resource, result.EvaluationDetails.Resource)
		})
	}
}

// TestConditionEvaluation tests policy conditions
func TestConditionEvaluation(t *testing.T) {
	engine := setupTestPolicyEngine(t)

	// Policy with IP address condition
	conditionalPolicy := &PolicyDocument{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Sid:      "AllowFromOfficeIP",
				Effect:   "Allow",
				Action:   []string{"s3:*"},
				Resource: []string{"arn:aws:s3:::*"},
				Condition: map[string]map[string]interface{}{
					"IpAddress": {
						"aws:SourceIp": []string{"192.168.1.0/24", "10.0.0.0/8"},
					},
				},
			},
		},
	}

	err := engine.AddPolicy("", "ip-conditional", conditionalPolicy)
	require.NoError(t, err)

	tests := []struct {
		name    string
		context *EvaluationContext
		want    Effect
	}{
		{
			name: "allow from office IP",
			context: &EvaluationContext{
				Principal: "user:alice",
				Action:    "s3:GetObject",
				Resource:  "arn:aws:s3:::mybucket/file.txt",
				RequestContext: map[string]interface{}{
					"aws:SourceIp": "192.168.1.100",
				},
			},
			want: EffectAllow,
		},
		{
			name: "deny from external IP",
			context: &EvaluationContext{
				Principal: "user:alice",
				Action:    "s3:GetObject",
				Resource:  "arn:aws:s3:::mybucket/file.txt",
				RequestContext: map[string]interface{}{
					"aws:SourceIp": "8.8.8.8",
				},
			},
			want: EffectDeny,
		},
		{
			name: "allow from internal IP",
			context: &EvaluationContext{
				Principal: "user:alice",
				Action:    "s3:PutObject",
				Resource:  "arn:aws:s3:::mybucket/newfile.txt",
				RequestContext: map[string]interface{}{
					"aws:SourceIp": "10.1.2.3",
				},
			},
			want: EffectAllow,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := engine.Evaluate(context.Background(), "", tt.context, []string{"ip-conditional"})

			assert.NoError(t, err)
			assert.Equal(t, tt.want, result.Effect)
		})
	}
}

// TestResourceMatching tests resource ARN matching
func TestResourceMatching(t *testing.T) {
	tests := []struct {
		name            string
		policyResource  string
		requestResource string
		want            bool
	}{
		{
			name:            "exact match",
			policyResource:  "arn:aws:s3:::mybucket/file.txt",
			requestResource: "arn:aws:s3:::mybucket/file.txt",
			want:            true,
		},
		{
			name:            "wildcard match",
			policyResource:  "arn:aws:s3:::mybucket/*",
			requestResource: "arn:aws:s3:::mybucket/folder/file.txt",
			want:            true,
		},
		{
			name:            "bucket wildcard",
			policyResource:  "arn:aws:s3:::*",
			requestResource: "arn:aws:s3:::anybucket/file.txt",
			want:            true,
		},
		{
			name:            "no match different bucket",
			policyResource:  "arn:aws:s3:::mybucket/*",
			requestResource: "arn:aws:s3:::otherbucket/file.txt",
			want:            false,
		},
		{
			name:            "prefix match",
			policyResource:  "arn:aws:s3:::mybucket/documents/*",
			requestResource: "arn:aws:s3:::mybucket/documents/secret.txt",
			want:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchResource(tt.policyResource, tt.requestResource)
			assert.Equal(t, tt.want, result)
		})
	}
}

// TestActionMatching tests action pattern matching
func TestActionMatching(t *testing.T) {
	tests := []struct {
		name          string
		policyAction  string
		requestAction string
		want          bool
	}{
		{
			name:          "exact match",
			policyAction:  "s3:GetObject",
			requestAction: "s3:GetObject",
			want:          true,
		},
		{
			name:          "wildcard service",
			policyAction:  "s3:*",
			requestAction: "s3:PutObject",
			want:          true,
		},
		{
			name:          "wildcard all",
			policyAction:  "*",
			requestAction: "filer:CreateEntry",
			want:          true,
		},
		{
			name:          "prefix match",
			policyAction:  "s3:Get*",
			requestAction: "s3:GetObject",
			want:          true,
		},
		{
			name:          "no match different service",
			policyAction:  "s3:GetObject",
			requestAction: "filer:GetEntry",
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchAction(tt.policyAction, tt.requestAction)
			assert.Equal(t, tt.want, result)
		})
	}
}

// Helper function to set up test policy engine
func setupTestPolicyEngine(t *testing.T) *PolicyEngine {
	engine := NewPolicyEngine()
	config := &PolicyEngineConfig{
		DefaultEffect: "Deny",
		StoreType:     "memory",
	}

	err := engine.Initialize(config)
	require.NoError(t, err)

	return engine
}
