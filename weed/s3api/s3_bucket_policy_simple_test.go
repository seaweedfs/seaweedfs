package s3api

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBucketPolicyValidationBasics tests the core validation logic
func TestBucketPolicyValidationBasics(t *testing.T) {
	s3Server := &S3ApiServer{}

	tests := []struct {
		name          string
		policy        *policy.PolicyDocument
		bucket        string
		expectedValid bool
		expectedError string
	}{
		{
			name: "Valid bucket policy",
			policy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{
					{
						Sid:    "TestStatement",
						Effect: "Allow",
						Principal: map[string]interface{}{
							"AWS": "*",
						},
						Action: []string{"s3:GetObject"},
						Resource: []string{
							"arn:seaweed:s3:::test-bucket/*",
						},
					},
				},
			},
			bucket:        "test-bucket",
			expectedValid: true,
		},
		{
			name: "Policy without Principal (invalid)",
			policy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{
					{
						Effect:   "Allow",
						Action:   []string{"s3:GetObject"},
						Resource: []string{"arn:seaweed:s3:::test-bucket/*"},
						// Principal is missing
					},
				},
			},
			bucket:        "test-bucket",
			expectedValid: false,
			expectedError: "bucket policies must specify a Principal",
		},
		{
			name: "Invalid version",
			policy: &policy.PolicyDocument{
				Version: "2008-10-17", // Wrong version
				Statement: []policy.Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"AWS": "*",
						},
						Action:   []string{"s3:GetObject"},
						Resource: []string{"arn:seaweed:s3:::test-bucket/*"},
					},
				},
			},
			bucket:        "test-bucket",
			expectedValid: false,
			expectedError: "unsupported policy version",
		},
		{
			name: "Resource not matching bucket",
			policy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"AWS": "*",
						},
						Action:   []string{"s3:GetObject"},
						Resource: []string{"arn:seaweed:s3:::other-bucket/*"}, // Wrong bucket
					},
				},
			},
			bucket:        "test-bucket",
			expectedValid: false,
			expectedError: "does not match bucket",
		},
		{
			name: "Non-S3 action",
			policy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"AWS": "*",
						},
						Action:   []string{"iam:GetUser"}, // Non-S3 action
						Resource: []string{"arn:seaweed:s3:::test-bucket/*"},
					},
				},
			},
			bucket:        "test-bucket",
			expectedValid: false,
			expectedError: "bucket policies only support S3 actions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s3Server.validateBucketPolicy(tt.policy, tt.bucket)

			if tt.expectedValid {
				assert.NoError(t, err, "Policy should be valid")
			} else {
				assert.Error(t, err, "Policy should be invalid")
				if tt.expectedError != "" {
					assert.Contains(t, err.Error(), tt.expectedError, "Error message should contain expected text")
				}
			}
		})
	}
}

// TestBucketResourceValidation tests the resource ARN validation
func TestBucketResourceValidation(t *testing.T) {
	s3Server := &S3ApiServer{}

	tests := []struct {
		name     string
		resource string
		bucket   string
		valid    bool
	}{
		// SeaweedFS ARN format
		{
			name:     "Exact bucket ARN (SeaweedFS)",
			resource: "arn:seaweed:s3:::test-bucket",
			bucket:   "test-bucket",
			valid:    true,
		},
		{
			name:     "Bucket wildcard ARN (SeaweedFS)",
			resource: "arn:seaweed:s3:::test-bucket/*",
			bucket:   "test-bucket",
			valid:    true,
		},
		{
			name:     "Specific object ARN (SeaweedFS)",
			resource: "arn:seaweed:s3:::test-bucket/path/to/object.txt",
			bucket:   "test-bucket",
			valid:    true,
		},
		// AWS ARN format (compatibility)
		{
			name:     "Exact bucket ARN (AWS)",
			resource: "arn:aws:s3:::test-bucket",
			bucket:   "test-bucket",
			valid:    true,
		},
		{
			name:     "Bucket wildcard ARN (AWS)",
			resource: "arn:aws:s3:::test-bucket/*",
			bucket:   "test-bucket",
			valid:    true,
		},
		{
			name:     "Specific object ARN (AWS)",
			resource: "arn:aws:s3:::test-bucket/path/to/object.txt",
			bucket:   "test-bucket",
			valid:    true,
		},
		// Simplified format (without ARN prefix)
		{
			name:     "Simplified bucket name",
			resource: "test-bucket",
			bucket:   "test-bucket",
			valid:    true,
		},
		{
			name:     "Simplified bucket wildcard",
			resource: "test-bucket/*",
			bucket:   "test-bucket",
			valid:    true,
		},
		{
			name:     "Simplified specific object",
			resource: "test-bucket/path/to/object.txt",
			bucket:   "test-bucket",
			valid:    true,
		},
		// Invalid cases
		{
			name:     "Different bucket ARN (SeaweedFS)",
			resource: "arn:seaweed:s3:::other-bucket/*",
			bucket:   "test-bucket",
			valid:    false,
		},
		{
			name:     "Different bucket ARN (AWS)",
			resource: "arn:aws:s3:::other-bucket/*",
			bucket:   "test-bucket",
			valid:    false,
		},
		{
			name:     "Different bucket simplified",
			resource: "other-bucket/*",
			bucket:   "test-bucket",
			valid:    false,
		},
		{
			name:     "Global S3 wildcard (SeaweedFS)",
			resource: "arn:seaweed:s3:::*",
			bucket:   "test-bucket",
			valid:    false,
		},
		{
			name:     "Global S3 wildcard (AWS)",
			resource: "arn:aws:s3:::*",
			bucket:   "test-bucket",
			valid:    false,
		},
		{
			name:     "Invalid ARN format",
			resource: "invalid-arn",
			bucket:   "test-bucket",
			valid:    false,
		},
		{
			name:     "Bucket name prefix match but different bucket",
			resource: "test-bucket-different/*",
			bucket:   "test-bucket",
			valid:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s3Server.validateResourceForBucket(tt.resource, tt.bucket)
			assert.Equal(t, tt.valid, result, "Resource validation result should match expected")
		})
	}
}

// TestBucketPolicyJSONSerialization tests policy JSON handling
func TestBucketPolicyJSONSerialization(t *testing.T) {
	policy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "PublicReadGetObject",
				Effect: "Allow",
				Principal: map[string]interface{}{
					"AWS": "*",
				},
				Action: []string{"s3:GetObject"},
				Resource: []string{
					"arn:seaweed:s3:::public-bucket/*",
				},
			},
		},
	}

	// Test that policy can be marshaled and unmarshaled correctly
	jsonData := marshalPolicy(t, policy)
	assert.NotEmpty(t, jsonData, "JSON data should not be empty")

	// Verify the JSON contains expected elements
	jsonStr := string(jsonData)
	assert.Contains(t, jsonStr, "2012-10-17", "JSON should contain version")
	assert.Contains(t, jsonStr, "s3:GetObject", "JSON should contain action")
	assert.Contains(t, jsonStr, "arn:seaweed:s3:::public-bucket/*", "JSON should contain resource")
	assert.Contains(t, jsonStr, "PublicReadGetObject", "JSON should contain statement ID")
}

// Helper function for marshaling policies
func marshalPolicy(t *testing.T, policyDoc *policy.PolicyDocument) []byte {
	data, err := json.Marshal(policyDoc)
	require.NoError(t, err)
	return data
}

// TestBucketPolicyAnonymousAccessHelpers tests the helper functions for anonymous access evaluation (Issue #7469)
func TestBucketPolicyAnonymousAccessHelpers(t *testing.T) {
	tests := []struct {
		name           string
		statement      policy.Statement
		s3Action       string
		resource       string
		expectedMatch  bool
		description    string
	}{
		{
			name: "Allow anonymous GetObject on specific path",
			statement: policy.Statement{
				Sid:    "PublicReadForTranscodedObjects",
				Effect: "Allow",
				Principal: map[string]interface{}{
					"AWS": "*",
				},
				Action:   []string{"s3:GetObject"},
				Resource: []string{"arn:aws:s3:::orbit/transcoded/*"},
			},
			s3Action:      "s3:GetObject",
			resource:      "arn:aws:s3:::orbit/transcoded/video.mp4",
			expectedMatch: true,
			description:   "Should match anonymous GetObject on transcoded/* path",
		},
		{
			name: "Deny anonymous GetObject on different path",
			statement: policy.Statement{
				Sid:    "PublicReadForTranscodedObjects",
				Effect: "Allow",
				Principal: map[string]interface{}{
					"AWS": "*",
				},
				Action:   []string{"s3:GetObject"},
				Resource: []string{"arn:aws:s3:::orbit/transcoded/*"},
			},
			s3Action:      "s3:GetObject",
			resource:      "arn:aws:s3:::orbit/private/document.pdf",
			expectedMatch: false,
			description:   "Should not match paths outside of policy resource pattern",
		},
		{
			name: "String principal wildcard",
			statement: policy.Statement{
				Effect:    "Allow",
				Principal: "*",
				Action:    []string{"s3:GetObject"},
				Resource:  []string{"arn:aws:s3:::public-bucket/*"},
			},
			s3Action:      "s3:GetObject",
			resource:      "arn:aws:s3:::public-bucket/any/file.txt",
			expectedMatch: true,
			description:   "Should match with string principal wildcard",
		},
		{
			name: "Wildcard action matching",
			statement: policy.Statement{
				Effect: "Allow",
				Principal: map[string]interface{}{
					"AWS": "*",
				},
				Action:   []string{"s3:*"},
				Resource: []string{"arn:aws:s3:::bucket/*"},
			},
			s3Action:      "s3:GetObject",
			resource:      "arn:aws:s3:::bucket/file.txt",
			expectedMatch: true,
			description:   "Wildcard action should match any S3 action",
		},
		{
			name: "ListBucket on bucket resource",
			statement: policy.Statement{
				Effect: "Allow",
				Principal: map[string]interface{}{
					"AWS": "*",
				},
				Action:   []string{"s3:ListBucket"},
				Resource: []string{"arn:aws:s3:::bucket"},
			},
			s3Action:      "s3:ListBucket",
			resource:      "arn:aws:s3:::bucket",
			expectedMatch: true,
			description:   "Should match ListBucket on exact bucket resource",
		},
		{
			name: "Non-anonymous principal",
			statement: policy.Statement{
				Effect: "Allow",
				Principal: map[string]interface{}{
					"AWS": "arn:aws:iam::123456789012:user/alice",
				},
				Action:   []string{"s3:GetObject"},
				Resource: []string{"arn:aws:s3:::bucket/*"},
			},
			s3Action:      "s3:GetObject",
			resource:      "arn:aws:s3:::bucket/file.txt",
			expectedMatch: false,
			description:   "Should not match when principal is not anonymous",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matched := statementMatchesAnonymousRequest(tt.statement, tt.s3Action, tt.resource)
			assert.Equal(t, tt.expectedMatch, matched, "Statement match result: %s", tt.description)
		})
	}
}

// TestPrincipalMatchesAnonymous tests the principal matching logic
func TestPrincipalMatchesAnonymous(t *testing.T) {
	tests := []struct {
		name      string
		principal interface{}
		expected  bool
	}{
		{"String wildcard", "*", true},
		{"AWS map with wildcard", map[string]interface{}{"AWS": "*"}, true},
		{"AWS map with array containing wildcard", map[string]interface{}{"AWS": []interface{}{"*"}}, true},
		{"AWS map with string array containing wildcard", map[string]interface{}{"AWS": []string{"*"}}, true},
		{"Specific ARN", map[string]interface{}{"AWS": "arn:aws:iam::123:user/alice"}, false},
		{"Empty principal", nil, false},
		{"Empty map", map[string]interface{}{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := principalMatchesAnonymous(tt.principal)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestActionToS3Action tests the action conversion
func TestActionToS3Action(t *testing.T) {
	tests := []struct {
		action   Action
		expected string
	}{
		{s3_constants.ACTION_READ, "s3:GetObject"},
		{s3_constants.ACTION_WRITE, "s3:PutObject"},
		{s3_constants.ACTION_LIST, "s3:ListBucket"},
		{s3_constants.ACTION_TAGGING, "s3:PutObjectTagging"},
		{s3_constants.ACTION_ADMIN, "s3:*"},
		{Action("s3:DeleteObject"), "s3:DeleteObject"},
		{Action("CustomAction"), "s3:CustomAction"},
	}

	for _, tt := range tests {
		t.Run(string(tt.action), func(t *testing.T) {
			result := actionToS3Action(tt.action)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBuildResourceARN tests the resource ARN building
func TestBuildResourceARN(t *testing.T) {
	tests := []struct {
		bucket   string
		object   string
		expected string
	}{
		{"bucket", "", "arn:aws:s3:::bucket"},
		{"bucket", "/", "arn:aws:s3:::bucket"},
		{"bucket", "file.txt", "arn:aws:s3:::bucket/file.txt"},
		{"bucket", "/file.txt", "arn:aws:s3:::bucket/file.txt"},
		{"bucket", "path/to/file.txt", "arn:aws:s3:::bucket/path/to/file.txt"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%s", tt.bucket, tt.object), func(t *testing.T) {
			result := buildResourceARN(tt.bucket, tt.object)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestResourceMatching tests the resource pattern matching
func TestResourceMatching(t *testing.T) {
	tests := []struct {
		pattern  string
		resource string
		expected bool
	}{
		{"*", "anything", true},
		{"arn:aws:s3:::bucket/*", "arn:aws:s3:::bucket/file.txt", true},
		{"arn:aws:s3:::bucket/*", "arn:aws:s3:::bucket/path/to/file.txt", true},
		{"arn:aws:s3:::bucket/*", "arn:aws:s3:::other-bucket/file.txt", false},
		{"bucket/*", "bucket/file.txt", true},
		{"bucket/prefix/*", "bucket/prefix/file.txt", true},
		{"bucket/prefix/*", "bucket/other/file.txt", false},
		{"arn:aws:s3:::bucket", "arn:aws:s3:::bucket", true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s matches %s", tt.pattern, tt.resource), func(t *testing.T) {
			result := matchesResourcePattern(tt.pattern, tt.resource)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIssue7252Examples tests the specific examples from GitHub issue #7252
func TestIssue7252Examples(t *testing.T) {
	s3Server := &S3ApiServer{}

	tests := []struct {
		name          string
		policy        *policy.PolicyDocument
		bucket        string
		expectedValid bool
		description   string
	}{
		{
			name: "Issue #7252 - Standard ARN with wildcard",
			policy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"AWS": "*",
						},
						Action:   []string{"s3:GetObject"},
						Resource: []string{"arn:aws:s3:::main-bucket/*"},
					},
				},
			},
			bucket:        "main-bucket",
			expectedValid: true,
			description:   "AWS ARN format should be accepted",
		},
		{
			name: "Issue #7252 - Simplified resource with wildcard",
			policy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"AWS": "*",
						},
						Action:   []string{"s3:GetObject"},
						Resource: []string{"main-bucket/*"},
					},
				},
			},
			bucket:        "main-bucket",
			expectedValid: true,
			description:   "Simplified format with wildcard should be accepted",
		},
		{
			name: "Issue #7252 - Resource as exact bucket name",
			policy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{
					{
						Effect: "Allow",
						Principal: map[string]interface{}{
							"AWS": "*",
						},
						Action:   []string{"s3:GetObject"},
						Resource: []string{"main-bucket"},
					},
				},
			},
			bucket:        "main-bucket",
			expectedValid: true,
			description:   "Exact bucket name should be accepted",
		},
		{
			name: "Public read policy with AWS ARN",
			policy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{
					{
						Sid:    "PublicReadGetObject",
						Effect: "Allow",
						Principal: map[string]interface{}{
							"AWS": "*",
						},
						Action:   []string{"s3:GetObject"},
						Resource: []string{"arn:aws:s3:::my-public-bucket/*"},
					},
				},
			},
			bucket:        "my-public-bucket",
			expectedValid: true,
			description:   "Standard public read policy with AWS ARN should work",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s3Server.validateBucketPolicy(tt.policy, tt.bucket)

			if tt.expectedValid {
				assert.NoError(t, err, "Policy should be valid: %s", tt.description)
			} else {
				assert.Error(t, err, "Policy should be invalid: %s", tt.description)
			}
		})
	}
}
