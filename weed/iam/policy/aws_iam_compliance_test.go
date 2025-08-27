package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAWSIAMMatch(t *testing.T) {
	evalCtx := &EvaluationContext{
		RequestContext: map[string]interface{}{
			"aws:username":      "testuser",
			"saml:username":     "john.doe",
			"oidc:sub":          "user123",
			"aws:userid":        "AIDACKCEVSQ6C2EXAMPLE",
			"aws:principaltype": "User",
		},
	}

	tests := []struct {
		name     string
		pattern  string
		value    string
		evalCtx  *EvaluationContext
		expected bool
	}{
		// Case insensitivity tests
		{
			name:     "case insensitive exact match",
			pattern:  "S3:GetObject",
			value:    "s3:getobject",
			evalCtx:  evalCtx,
			expected: true,
		},
		{
			name:     "case insensitive wildcard match",
			pattern:  "S3:Get*",
			value:    "s3:getobject",
			evalCtx:  evalCtx,
			expected: true,
		},
		// Policy variable expansion tests
		{
			name:     "AWS username variable expansion",
			pattern:  "arn:aws:s3:::mybucket/${aws:username}/*",
			value:    "arn:aws:s3:::mybucket/testuser/document.pdf",
			evalCtx:  evalCtx,
			expected: true,
		},
		{
			name:     "SAML username variable expansion",
			pattern:  "home/${saml:username}/*",
			value:    "home/john.doe/private.txt",
			evalCtx:  evalCtx,
			expected: true,
		},
		{
			name:     "OIDC subject variable expansion",
			pattern:  "users/${oidc:sub}/data",
			value:    "users/user123/data",
			evalCtx:  evalCtx,
			expected: true,
		},
		// Mixed case and variable tests
		{
			name:     "case insensitive with variable",
			pattern:  "S3:GetObject/${aws:username}/*",
			value:    "s3:getobject/testuser/file.txt",
			evalCtx:  evalCtx,
			expected: true,
		},
		// Universal wildcard
		{
			name:     "universal wildcard",
			pattern:  "*",
			value:    "anything",
			evalCtx:  evalCtx,
			expected: true,
		},
		// Question mark wildcard
		{
			name:     "question mark wildcard",
			pattern:  "file?.txt",
			value:    "file1.txt",
			evalCtx:  evalCtx,
			expected: true,
		},
		// No match cases
		{
			name:     "no match different pattern",
			pattern:  "s3:PutObject",
			value:    "s3:GetObject",
			evalCtx:  evalCtx,
			expected: false,
		},
		{
			name:     "variable not expanded due to missing context",
			pattern:  "users/${aws:username}/data",
			value:    "users/${aws:username}/data",
			evalCtx:  nil,
			expected: true, // Should match literally when no context
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := awsIAMMatch(tt.pattern, tt.value, tt.evalCtx)
			assert.Equal(t, tt.expected, result, "AWS IAM match result should match expected")
		})
	}
}

func TestExpandPolicyVariables(t *testing.T) {
	evalCtx := &EvaluationContext{
		RequestContext: map[string]interface{}{
			"aws:username":  "alice",
			"saml:username": "alice.smith",
			"oidc:sub":      "sub123",
		},
	}

	tests := []struct {
		name     string
		pattern  string
		evalCtx  *EvaluationContext
		expected string
	}{
		{
			name:     "expand aws username",
			pattern:  "home/${aws:username}/documents/*",
			evalCtx:  evalCtx,
			expected: "home/alice/documents/*",
		},
		{
			name:     "expand multiple variables",
			pattern:  "${aws:username}/${oidc:sub}/data",
			evalCtx:  evalCtx,
			expected: "alice/sub123/data",
		},
		{
			name:     "no variables to expand",
			pattern:  "static/path/file.txt",
			evalCtx:  evalCtx,
			expected: "static/path/file.txt",
		},
		{
			name:     "nil context",
			pattern:  "home/${aws:username}/file",
			evalCtx:  nil,
			expected: "home/${aws:username}/file",
		},
		{
			name:     "missing variable in context",
			pattern:  "home/${aws:nonexistent}/file",
			evalCtx:  evalCtx,
			expected: "home/${aws:nonexistent}/file", // Should remain unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := expandPolicyVariables(tt.pattern, tt.evalCtx)
			assert.Equal(t, tt.expected, result, "Policy variable expansion should match expected")
		})
	}
}

func TestAWSWildcardMatch(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		value    string
		expected bool
	}{
		{
			name:     "case insensitive asterisk",
			pattern:  "S3:Get*",
			value:    "s3:getobject",
			expected: true,
		},
		{
			name:     "case insensitive question mark",
			pattern:  "file?.TXT",
			value:    "file1.txt",
			expected: true,
		},
		{
			name:     "mixed wildcards",
			pattern:  "S3:*Object?",
			value:    "s3:getobjects",
			expected: true,
		},
		{
			name:     "no match",
			pattern:  "s3:Put*",
			value:    "s3:GetObject",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AwsWildcardMatch(tt.pattern, tt.value)
			assert.Equal(t, tt.expected, result, "AWS wildcard match should match expected")
		})
	}
}
