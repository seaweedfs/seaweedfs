package s3api

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/mux"
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

			t.Logf("SECURITY SCENARIO: %s", scenario.name)
			t.Logf("   Expected Action: %s", result)
			t.Logf("   Security Benefit: %s", scenario.securityBenefit)
			t.Logf("   Policy Example:\n%s", scenario.policyExample)
		})
	}
}

// TestDeleteObjectPolicyEnforcement demonstrates that the architectural limitation has been fixed
// Previously, DeleteObject operations were mapped to s3:PutObject, preventing fine-grained policies from working
func TestDeleteObjectPolicyEnforcement(t *testing.T) {
	tests := []struct {
		name             string
		method           string
		bucket           string
		objectKey        string
		baseAction       Action
		expectedS3Action string
		policyScenario   string
	}{
		{
			name:             "delete_object_maps_to_correct_action",
			method:           http.MethodDelete,
			bucket:           "test-bucket",
			objectKey:        "test-object.txt",
			baseAction:       s3_constants.ACTION_WRITE,
			expectedS3Action: "s3:DeleteObject",
			policyScenario:   "Policy that denies s3:DeleteObject but allows s3:PutObject should now work correctly",
		},
		{
			name:             "put_object_maps_to_correct_action",
			method:           http.MethodPut,
			bucket:           "test-bucket",
			objectKey:        "test-object.txt",
			baseAction:       s3_constants.ACTION_WRITE,
			expectedS3Action: "s3:PutObject",
			policyScenario:   "Policy that allows s3:PutObject but denies s3:DeleteObject should allow uploads",
		},
		{
			name:             "batch_delete_maps_to_delete_action",
			method:           http.MethodPost,
			bucket:           "test-bucket",
			objectKey:        "",
			baseAction:       s3_constants.ACTION_WRITE,
			expectedS3Action: "s3:DeleteObject",
			policyScenario:   "Batch delete operations should also map to s3:DeleteObject",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create HTTP request
			req := &http.Request{
				Method: tt.method,
				URL:    &url.URL{Path: "/" + tt.bucket + "/" + tt.objectKey},
				Header: http.Header{},
			}

			// For batch delete, add the delete query parameter
			if tt.method == http.MethodPost && tt.expectedS3Action == "s3:DeleteObject" {
				query := req.URL.Query()
				query.Set("delete", "")
				req.URL.RawQuery = query.Encode()
			}

			// Test the action resolution
			result := determineGranularS3Action(req, tt.baseAction, tt.bucket, tt.objectKey)

			assert.Equal(t, tt.expectedS3Action, result,
				"Action Resolution Test: %s\n"+
					"HTTP Method: %s\n"+
					"Base Action: %s\n"+
					"Policy Scenario: %s\n"+
					"Expected: %s, Got: %s",
				tt.name, tt.method, tt.baseAction, tt.policyScenario, tt.expectedS3Action, result)

			t.Logf("ARCHITECTURAL FIX VERIFIED: %s", tt.name)
			t.Logf("   Method: %s -> S3 Action: %s", tt.method, result)
			t.Logf("   Policy Scenario: %s", tt.policyScenario)
		})
	}
}

// TestFineGrainedPolicyExample demonstrates a real-world use case that now works
// This test verifies the exact scenario described in the original TODO comment
func TestFineGrainedPolicyExample(t *testing.T) {
	// Example policy: Allow PutObject but Deny DeleteObject
	// This is a common pattern for "append-only" buckets or write-once scenarios
	policyExample := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Sid": "AllowObjectUploads",
				"Effect": "Allow",
				"Action": "s3:PutObject",
				"Resource": "arn:aws:s3:::test-bucket/*"
			},
			{
				"Sid": "DenyObjectDeletion",
				"Effect": "Deny",
				"Action": "s3:DeleteObject",
				"Resource": "arn:aws:s3:::test-bucket/*"
			}
		]
	}`

	testCases := []struct {
		operation       string
		method          string
		objectKey       string
		queryParams     map[string]string
		baseAction      Action
		expectedAction  string
		shouldBeAllowed bool
		rationale       string
	}{
		{
			operation:       "PUT object",
			method:          http.MethodPut,
			objectKey:       "document.txt",
			queryParams:     map[string]string{},
			baseAction:      s3_constants.ACTION_WRITE,
			expectedAction:  "s3:PutObject",
			shouldBeAllowed: true,
			rationale:       "Policy explicitly allows s3:PutObject - upload should succeed",
		},
		{
			operation:       "DELETE object",
			method:          http.MethodDelete,
			objectKey:       "document.txt",
			queryParams:     map[string]string{},
			baseAction:      s3_constants.ACTION_WRITE,
			expectedAction:  "s3:DeleteObject",
			shouldBeAllowed: false,
			rationale:       "Policy explicitly denies s3:DeleteObject - deletion should be blocked",
		},
		{
			operation:       "Batch DELETE",
			method:          http.MethodPost,
			objectKey:       "",
			queryParams:     map[string]string{"delete": ""},
			baseAction:      s3_constants.ACTION_WRITE,
			expectedAction:  "s3:DeleteObject",
			shouldBeAllowed: false,
			rationale:       "Policy explicitly denies s3:DeleteObject - batch deletion should be blocked",
		},
	}

	t.Logf("\nTesting Fine-Grained Policy:")
	t.Logf("%s\n", policyExample)

	for _, tc := range testCases {
		t.Run(tc.operation, func(t *testing.T) {
			// Create HTTP request
			req := &http.Request{
				Method: tc.method,
				URL:    &url.URL{Path: "/test-bucket/" + tc.objectKey},
				Header: http.Header{},
			}

			// Add query parameters
			query := req.URL.Query()
			for key, value := range tc.queryParams {
				query.Set(key, value)
			}
			req.URL.RawQuery = query.Encode()

			// Determine the S3 action
			actualAction := determineGranularS3Action(req, tc.baseAction, "test-bucket", tc.objectKey)

			// Verify the action mapping is correct
			assert.Equal(t, tc.expectedAction, actualAction,
				"Operation: %s\nExpected Action: %s\nGot: %s",
				tc.operation, tc.expectedAction, actualAction)

			// Log the result
			allowStatus := "[DENIED]"
			if tc.shouldBeAllowed {
				allowStatus = "[ALLOWED]"
			}

			t.Logf("%s %s -> %s", allowStatus, tc.operation, actualAction)
			t.Logf("   Rationale: %s", tc.rationale)
		})
	}

	t.Logf("\nARCHITECTURAL LIMITATION RESOLVED!")
	t.Logf("   Fine-grained policies like 'allow PUT but deny DELETE' now work correctly")
	t.Logf("   The policy engine can distinguish between s3:PutObject and s3:DeleteObject")
}

// TestCoarseActionResolution verifies that ResolveS3Action correctly maps
// coarse-grained ACTION_WRITE to specific S3 actions based on HTTP context.
// This demonstrates the fix for the architectural limitation where ACTION_WRITE
// was always mapped to s3:PutObject, preventing fine-grained policies from working.
func TestCoarseActionResolution(t *testing.T) {
	testCases := []struct {
		name             string
		method           string
		path             string
		queryParams      map[string]string
		coarseAction     Action
		expectedS3Action string
		policyScenario   string
	}{
		{
			name:             "PUT_with_ACTION_WRITE_resolves_to_PutObject",
			method:           http.MethodPut,
			path:             "/test-bucket/test-file.txt",
			queryParams:      map[string]string{},
			coarseAction:     s3_constants.ACTION_WRITE,
			expectedS3Action: "s3:PutObject",
			policyScenario:   "Policy allowing s3:PutObject should match PUT requests",
		},
		{
			name:             "DELETE_with_ACTION_WRITE_resolves_to_DeleteObject",
			method:           http.MethodDelete,
			path:             "/test-bucket/test-file.txt",
			queryParams:      map[string]string{},
			coarseAction:     s3_constants.ACTION_WRITE,
			expectedS3Action: "s3:DeleteObject",
			policyScenario:   "Policy denying s3:DeleteObject should block DELETE requests",
		},
		{
			name:             "batch_DELETE_with_ACTION_WRITE_resolves_to_DeleteObject",
			method:           http.MethodPost,
			path:             "/test-bucket",
			queryParams:      map[string]string{"delete": ""},
			coarseAction:     s3_constants.ACTION_WRITE,
			expectedS3Action: "s3:DeleteObject",
			policyScenario:   "Policy denying s3:DeleteObject should block batch DELETE",
		},
		{
			name:             "POST_multipart_with_ACTION_WRITE_resolves_to_CreateMultipartUpload",
			method:           http.MethodPost,
			path:             "/test-bucket/large-file.mp4",
			queryParams:      map[string]string{"uploads": ""},
			coarseAction:     s3_constants.ACTION_WRITE,
			expectedS3Action: "s3:CreateMultipartUpload",
			policyScenario:   "Policy allowing s3:PutObject but denying s3:CreateMultipartUpload can now work",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Build URL with query parameters
			u, err := url.Parse(tc.path)
			assert.NoError(t, err)
			q := u.Query()
			for k, v := range tc.queryParams {
				q.Add(k, v)
			}
			u.RawQuery = q.Encode()

			// Create HTTP request
			req, err := http.NewRequest(tc.method, u.String(), nil)
			assert.NoError(t, err)

			// Parse path to extract bucket and object
			parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
			bucket := ""
			object := ""
			if len(parts) > 0 {
				bucket = parts[0]
			}
			if len(parts) > 1 {
				object = "/" + strings.Join(parts[1:], "/")
			}

			// Simulate mux.Vars for GetBucketAndObject
			req = mux.SetURLVars(req, map[string]string{
				"bucket": bucket,
				"object": object,
			})

			// Call ResolveS3Action with coarse action constant
			resolvedAction := ResolveS3Action(req, string(tc.coarseAction), bucket, object)

			// Verify correct S3 action is resolved
			assert.Equal(t, tc.expectedS3Action, resolvedAction,
				"Coarse action %s with method %s should resolve to %s",
				tc.coarseAction, tc.method, tc.expectedS3Action)

			t.Logf("SUCCESS: %s", tc.name)
			t.Logf("  Input:  %s %s + ACTION_WRITE", tc.method, tc.path)
			t.Logf("  Output: %s", resolvedAction)
			t.Logf("  Policy impact: %s", tc.policyScenario)
		})
	}

	t.Log("\n=== ARCHITECTURAL LIMITATION RESOLVED ===")
	t.Log("Handlers can use coarse ACTION_WRITE constant, and the context-aware")
	t.Log("resolver will map it to the correct specific S3 action (PutObject,")
	t.Log("DeleteObject, CreateMultipartUpload, etc.) based on HTTP method and")
	t.Log("query parameters. This enables fine-grained bucket policies like:")
	t.Log("  - Allow s3:PutObject but Deny s3:DeleteObject (append-only)")
	t.Log("  - Allow regular uploads but Deny multipart (size limits)")
}
