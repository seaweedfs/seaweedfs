package s3api

import (
	"encoding/json"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
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
		{
			name:     "Exact bucket ARN",
			resource: "arn:seaweed:s3:::test-bucket",
			bucket:   "test-bucket",
			valid:    true,
		},
		{
			name:     "Bucket wildcard ARN",
			resource: "arn:seaweed:s3:::test-bucket/*",
			bucket:   "test-bucket",
			valid:    true,
		},
		{
			name:     "Specific object ARN",
			resource: "arn:seaweed:s3:::test-bucket/path/to/object.txt",
			bucket:   "test-bucket",
			valid:    true,
		},
		{
			name:     "Different bucket ARN",
			resource: "arn:seaweed:s3:::other-bucket/*",
			bucket:   "test-bucket",
			valid:    false,
		},
		{
			name:     "Global S3 wildcard",
			resource: "arn:seaweed:s3:::*",
			bucket:   "test-bucket",
			valid:    false,
		},
		{
			name:     "Invalid ARN format",
			resource: "invalid-arn",
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
