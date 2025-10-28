package s3api

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// TestGranularActionMappingSecurity demonstrates how the new granular action mapping
// fixes critical security issues that existed with the previous coarse mapping
func TestGranularActionMappingSecurity(t *testing.T) {
	tests := []struct {
		name                  string
		method                string
		bucket                string
		objectKey             string
		queryParams           map[string]string
		description           string
		problemWithOldMapping string
		granularActionResult  string
	}{
		{
			name:        "delete_object_security_fix",
			method:      "DELETE",
			bucket:      "sensitive-bucket",
			objectKey:   "confidential-file.txt",
			queryParams: map[string]string{},
			description: "DELETE object operations should map to s3:DeleteObject, not s3:PutObject",
			problemWithOldMapping: "Old mapping incorrectly mapped DELETE object to s3:PutObject, " +
				"allowing users with only PUT permissions to delete objects - a critical security flaw",
			granularActionResult: "s3:DeleteObject",
		},
		{
			name:        "get_object_acl_precision",
			method:      "GET",
			bucket:      "secure-bucket",
			objectKey:   "private-file.pdf",
			queryParams: map[string]string{"acl": ""},
			description: "GET object ACL should map to s3:GetObjectAcl, not generic s3:GetObject",
			problemWithOldMapping: "Old mapping would allow users with s3:GetObject permission to " +
				"read ACLs, potentially exposing sensitive permission information",
			granularActionResult: "s3:GetObjectAcl",
		},
		{
			name:        "put_object_tagging_precision",
			method:      "PUT",
			bucket:      "data-bucket",
			objectKey:   "business-document.xlsx",
			queryParams: map[string]string{"tagging": ""},
			description: "PUT object tagging should map to s3:PutObjectTagging, not generic s3:PutObject",
			problemWithOldMapping: "Old mapping couldn't distinguish between actual object uploads and " +
				"metadata operations like tagging, making fine-grained permissions impossible",
			granularActionResult: "s3:PutObjectTagging",
		},
		{
			name:        "multipart_upload_precision",
			method:      "POST",
			bucket:      "large-files",
			objectKey:   "video.mp4",
			queryParams: map[string]string{"uploads": ""},
			description: "Multipart upload initiation should map to s3:CreateMultipartUpload",
			problemWithOldMapping: "Old mapping would treat multipart operations as generic s3:PutObject, " +
				"preventing policies that allow regular uploads but restrict large multipart operations",
			granularActionResult: "s3:CreateMultipartUpload",
		},
		{
			name:        "bucket_policy_vs_bucket_creation",
			method:      "PUT",
			bucket:      "corporate-bucket",
			objectKey:   "",
			queryParams: map[string]string{"policy": ""},
			description: "Bucket policy modifications should map to s3:PutBucketPolicy, not s3:CreateBucket",
			problemWithOldMapping: "Old mapping couldn't distinguish between creating buckets and " +
				"modifying bucket policies, potentially allowing unauthorized policy changes",
			granularActionResult: "s3:PutBucketPolicy",
		},
		{
			name:        "list_vs_read_distinction",
			method:      "GET",
			bucket:      "inventory-bucket",
			objectKey:   "",
			queryParams: map[string]string{"uploads": ""},
			description: "Listing multipart uploads should map to s3:ListMultipartUploads",
			problemWithOldMapping: "Old mapping would use generic s3:ListBucket for all bucket operations, " +
				"preventing fine-grained control over who can see ongoing multipart operations",
			granularActionResult: "s3:ListMultipartUploads",
		},
		{
			name:        "delete_object_tagging_precision",
			method:      "DELETE",
			bucket:      "metadata-bucket",
			objectKey:   "tagged-file.json",
			queryParams: map[string]string{"tagging": ""},
			description: "Delete object tagging should map to s3:DeleteObjectTagging, not s3:DeleteObject",
			problemWithOldMapping: "Old mapping couldn't distinguish between deleting objects and " +
				"deleting tags, preventing policies that allow tag management but not object deletion",
			granularActionResult: "s3:DeleteObjectTagging",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create HTTP request with query parameters
			req := &http.Request{
				Method: tt.method,
				URL:    &url.URL{Path: "/" + tt.bucket + "/" + tt.objectKey},
			}

			// Add query parameters
			query := req.URL.Query()
			for key, value := range tt.queryParams {
				query.Set(key, value)
			}
			req.URL.RawQuery = query.Encode()

			// Test the new granular action determination
			result := determineGranularS3Action(req, s3_constants.ACTION_WRITE, tt.bucket, tt.objectKey)

			assert.Equal(t, tt.granularActionResult, result,
				"Security Fix Test: %s\n"+
					"Description: %s\n"+
					"Problem with old mapping: %s\n"+
					"Expected: %s, Got: %s",
				tt.name, tt.description, tt.problemWithOldMapping, tt.granularActionResult, result)

			// Log the security improvement
			t.Logf("SECURITY IMPROVEMENT: %s", tt.description)
			t.Logf("   Problem Fixed: %s", tt.problemWithOldMapping)
			t.Logf("   Granular Action: %s", result)
		})
	}
}

// TestBackwardCompatibilityFallback tests that the new system maintains backward compatibility
// with existing generic actions while providing enhanced granularity
func TestBackwardCompatibilityFallback(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		bucket         string
		objectKey      string
		fallbackAction Action
		expectedResult string
		description    string
	}{
		{
			name:           "generic_read_fallback",
			method:         "GET", // Generic method without specific query params
			bucket:         "",    // Edge case: no bucket specified
			objectKey:      "",    // Edge case: no object specified
			fallbackAction: s3_constants.ACTION_READ,
			expectedResult: "s3:GetObject",
			description:    "Generic read operations should fall back to s3:GetObject for compatibility",
		},
		{
			name:           "generic_write_fallback",
			method:         "PUT", // Generic method without specific query params
			bucket:         "",    // Edge case: no bucket specified
			objectKey:      "",    // Edge case: no object specified
			fallbackAction: s3_constants.ACTION_WRITE,
			expectedResult: "s3:PutObject",
			description:    "Generic write operations should fall back to s3:PutObject for compatibility",
		},
		{
			name:           "already_granular_passthrough",
			method:         "GET",
			bucket:         "",
			objectKey:      "",
			fallbackAction: "s3:GetBucketLocation", // Already specific
			expectedResult: "s3:GetBucketLocation",
			description:    "Already granular actions should pass through unchanged",
		},
		{
			name:           "unknown_action_conversion",
			method:         "GET",
			bucket:         "",
			objectKey:      "",
			fallbackAction: "CustomAction", // Not S3-prefixed
			expectedResult: "s3:CustomAction",
			description:    "Unknown actions should be converted to S3 format for consistency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{
				Method: tt.method,
				URL:    &url.URL{Path: "/" + tt.bucket + "/" + tt.objectKey},
			}

			result := determineGranularS3Action(req, tt.fallbackAction, tt.bucket, tt.objectKey)

			assert.Equal(t, tt.expectedResult, result,
				"Backward Compatibility Test: %s\nDescription: %s\nExpected: %s, Got: %s",
				tt.name, tt.description, tt.expectedResult, result)

			t.Logf("COMPATIBILITY: %s - %s", tt.description, result)
		})
	}
}

// TestPolicyEnforcementScenarios demonstrates how granular actions enable
// more precise and secure IAM policy enforcement
func TestPolicyEnforcementScenarios(t *testing.T) {
	scenarios := []struct {
		name            string
		policyExample   string
		method          string
		bucket          string
		objectKey       string
		queryParams     map[string]string
		expectedAction  string
		securityBenefit string
	}{
		{
			name: "allow_read_deny_acl_access",
			policyExample: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Action": "s3:GetObject",
						"Resource": "arn:aws:s3:::sensitive-bucket/*"
					}
				]
			}`,
			method:          "GET",
			bucket:          "sensitive-bucket",
			objectKey:       "document.pdf",
			queryParams:     map[string]string{"acl": ""},
			expectedAction:  "s3:GetObjectAcl",
			securityBenefit: "Policy allows reading objects but denies ACL access - granular actions enable this distinction",
		},
		{
			name: "allow_tagging_deny_object_modification",
			policyExample: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow", 
						"Action": ["s3:PutObjectTagging", "s3:DeleteObjectTagging"],
						"Resource": "arn:aws:s3:::data-bucket/*"
					}
				]
			}`,
			method:          "PUT",
			bucket:          "data-bucket",
			objectKey:       "metadata-file.json",
			queryParams:     map[string]string{"tagging": ""},
			expectedAction:  "s3:PutObjectTagging",
			securityBenefit: "Policy allows tag management but prevents actual object uploads - critical for metadata-only roles",
		},
		{
			name: "restrict_multipart_uploads",
			policyExample: `{
				"Version": "2012-10-17",
				"Statement": [
					{
						"Effect": "Allow",
						"Action": "s3:PutObject",
						"Resource": "arn:aws:s3:::uploads/*"
					},
					{
						"Effect": "Deny",
						"Action": ["s3:CreateMultipartUpload", "s3:UploadPart"],
						"Resource": "arn:aws:s3:::uploads/*"
					}
				]
			}`,
			method:          "POST",
			bucket:          "uploads",
			objectKey:       "large-file.zip",
			queryParams:     map[string]string{"uploads": ""},
			expectedAction:  "s3:CreateMultipartUpload",
			securityBenefit: "Policy allows regular uploads but blocks large multipart uploads - prevents resource abuse",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			req := &http.Request{
				Method: scenario.method,
				URL:    &url.URL{Path: "/" + scenario.bucket + "/" + scenario.objectKey},
			}

			query := req.URL.Query()
			for key, value := range scenario.queryParams {
				query.Set(key, value)
			}
			req.URL.RawQuery = query.Encode()

			result := determineGranularS3Action(req, s3_constants.ACTION_WRITE, scenario.bucket, scenario.objectKey)

			assert.Equal(t, scenario.expectedAction, result,
				"Policy Enforcement Scenario: %s\nExpected Action: %s, Got: %s",
				scenario.name, scenario.expectedAction, result)

			t.Logf("🔒 SECURITY SCENARIO: %s", scenario.name)
			t.Logf("   Expected Action: %s", result)
			t.Logf("   Security Benefit: %s", scenario.securityBenefit)
			t.Logf("   Policy Example:\n%s", scenario.policyExample)
		})
	}
}
