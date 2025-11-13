package s3api

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// TestListPartsActionMapping tests the fix for the missing s3:ListParts action mapping
// when GET requests include an uploadId query parameter
func TestListPartsActionMapping(t *testing.T) {
	testCases := []struct {
		name           string
		method         string
		bucket         string
		objectKey      string
		queryParams    map[string]string
		fallbackAction Action
		expectedAction string
		description    string
	}{
		{
			name:           "get_object_without_uploadId",
			method:         "GET",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{},
			fallbackAction: s3_constants.ACTION_READ,
			expectedAction: "s3:GetObject",
			description:    "GET request without uploadId should map to s3:GetObject",
		},
		{
			name:           "get_object_with_uploadId",
			method:         "GET",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{"uploadId": "test-upload-id"},
			fallbackAction: s3_constants.ACTION_READ,
			expectedAction: "s3:ListMultipartUploadParts",
			description:    "GET request with uploadId should map to s3:ListParts (this was the missing mapping)",
		},
		{
			name:      "get_object_with_uploadId_and_other_params",
			method:    "GET",
			bucket:    "test-bucket",
			objectKey: "test-object.txt",
			queryParams: map[string]string{
				"uploadId":           "test-upload-id-123",
				"max-parts":          "100",
				"part-number-marker": "50",
			},
			fallbackAction: s3_constants.ACTION_READ,
			expectedAction: "s3:ListMultipartUploadParts",
			description:    "GET request with uploadId plus other multipart params should map to s3:ListParts",
		},
		{
			name:           "get_object_with_versionId",
			method:         "GET",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{"versionId": "version-123"},
			fallbackAction: s3_constants.ACTION_READ,
			expectedAction: "s3:GetObjectVersion",
			description:    "GET request with versionId should map to s3:GetObjectVersion",
		},
		{
			name:           "get_object_acl_without_uploadId",
			method:         "GET",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{"acl": ""},
			fallbackAction: s3_constants.ACTION_READ_ACP,
			expectedAction: "s3:GetObjectAcl",
			description:    "GET request with acl should map to s3:GetObjectAcl (not affected by uploadId fix)",
		},
		{
			name:           "post_multipart_upload_without_uploadId",
			method:         "POST",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{"uploads": ""},
			fallbackAction: s3_constants.ACTION_WRITE,
			expectedAction: "s3:CreateMultipartUpload",
			description:    "POST request to initiate multipart upload should not be affected by uploadId fix",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create HTTP request with query parameters
			req := &http.Request{
				Method: tc.method,
				URL:    &url.URL{Path: "/" + tc.bucket + "/" + tc.objectKey},
			}

			// Add query parameters
			query := req.URL.Query()
			for key, value := range tc.queryParams {
				query.Set(key, value)
			}
			req.URL.RawQuery = query.Encode()

			// Call the granular action determination function
			action := determineGranularS3Action(req, tc.fallbackAction, tc.bucket, tc.objectKey)

			// Verify the action mapping
			assert.Equal(t, tc.expectedAction, action,
				"Test case: %s - %s", tc.name, tc.description)
		})
	}
}

// TestListPartsActionMappingSecurityScenarios tests security scenarios for the ListParts fix
func TestListPartsActionMappingSecurityScenarios(t *testing.T) {
	t.Run("privilege_separation_listparts_vs_getobject", func(t *testing.T) {
		// Scenario: User has permission to list multipart upload parts but NOT to get the actual object content
		// This is a common enterprise pattern where users can manage uploads but not read final objects

		// Test request 1: List parts with uploadId
		req1 := &http.Request{
			Method: "GET",
			URL:    &url.URL{Path: "/secure-bucket/confidential-document.pdf"},
		}
		query1 := req1.URL.Query()
		query1.Set("uploadId", "active-upload-123")
		req1.URL.RawQuery = query1.Encode()
		action1 := determineGranularS3Action(req1, s3_constants.ACTION_READ, "secure-bucket", "confidential-document.pdf")

		// Test request 2: Get object without uploadId
		req2 := &http.Request{
			Method: "GET",
			URL:    &url.URL{Path: "/secure-bucket/confidential-document.pdf"},
		}
		action2 := determineGranularS3Action(req2, s3_constants.ACTION_READ, "secure-bucket", "confidential-document.pdf")

		// These should be different actions, allowing different permissions
		assert.Equal(t, "s3:ListMultipartUploadParts", action1, "Listing multipart parts should require s3:ListMultipartUploadParts permission")
		assert.Equal(t, "s3:GetObject", action2, "Reading object content should require s3:GetObject permission")
		assert.NotEqual(t, action1, action2, "ListParts and GetObject should be separate permissions for security")
	})

	t.Run("policy_enforcement_precision", func(t *testing.T) {
		// This test documents the security improvement - before the fix, both operations
		// would incorrectly map to s3:GetObject, preventing fine-grained access control

		testCases := []struct {
			description    string
			queryParams    map[string]string
			expectedAction string
			securityNote   string
		}{
			{
				description:    "List multipart upload parts",
				queryParams:    map[string]string{"uploadId": "upload-abc123"},
				expectedAction: "s3:ListMultipartUploadParts",
				securityNote:   "FIXED: Now correctly maps to s3:ListParts instead of s3:GetObject",
			},
			{
				description:    "Get actual object content",
				queryParams:    map[string]string{},
				expectedAction: "s3:GetObject",
				securityNote:   "UNCHANGED: Still correctly maps to s3:GetObject",
			},
			{
				description:    "Get object with complex upload ID",
				queryParams:    map[string]string{"uploadId": "complex-upload-id-with-hyphens-123-abc-def"},
				expectedAction: "s3:ListMultipartUploadParts",
				securityNote:   "FIXED: Complex upload IDs now correctly detected",
			},
		}

		for _, tc := range testCases {
			req := &http.Request{
				Method: "GET",
				URL:    &url.URL{Path: "/test-bucket/test-object"},
			}

			query := req.URL.Query()
			for key, value := range tc.queryParams {
				query.Set(key, value)
			}
			req.URL.RawQuery = query.Encode()

			action := determineGranularS3Action(req, s3_constants.ACTION_READ, "test-bucket", "test-object")

			assert.Equal(t, tc.expectedAction, action,
				"%s - %s", tc.description, tc.securityNote)
		}
	})
}

// TestListPartsActionRealWorldScenarios tests realistic enterprise multipart upload scenarios
func TestListPartsActionRealWorldScenarios(t *testing.T) {
	t.Run("large_file_upload_workflow", func(t *testing.T) {
		// Simulate a large file upload workflow where users need different permissions for each step

		// Step 1: Initiate multipart upload (POST with uploads query)
		req1 := &http.Request{
			Method: "POST",
			URL:    &url.URL{Path: "/data/large-dataset.csv"},
		}
		query1 := req1.URL.Query()
		query1.Set("uploads", "")
		req1.URL.RawQuery = query1.Encode()
		action1 := determineGranularS3Action(req1, s3_constants.ACTION_WRITE, "data", "large-dataset.csv")

		// Step 2: List existing parts (GET with uploadId query) - THIS WAS THE MISSING MAPPING
		req2 := &http.Request{
			Method: "GET",
			URL:    &url.URL{Path: "/data/large-dataset.csv"},
		}
		query2 := req2.URL.Query()
		query2.Set("uploadId", "dataset-upload-20240827-001")
		req2.URL.RawQuery = query2.Encode()
		action2 := determineGranularS3Action(req2, s3_constants.ACTION_READ, "data", "large-dataset.csv")

		// Step 3: Upload a part (PUT with uploadId and partNumber)
		req3 := &http.Request{
			Method: "PUT",
			URL:    &url.URL{Path: "/data/large-dataset.csv"},
		}
		query3 := req3.URL.Query()
		query3.Set("uploadId", "dataset-upload-20240827-001")
		query3.Set("partNumber", "5")
		req3.URL.RawQuery = query3.Encode()
		action3 := determineGranularS3Action(req3, s3_constants.ACTION_WRITE, "data", "large-dataset.csv")

		// Step 4: Complete multipart upload (POST with uploadId)
		req4 := &http.Request{
			Method: "POST",
			URL:    &url.URL{Path: "/data/large-dataset.csv"},
		}
		query4 := req4.URL.Query()
		query4.Set("uploadId", "dataset-upload-20240827-001")
		req4.URL.RawQuery = query4.Encode()
		action4 := determineGranularS3Action(req4, s3_constants.ACTION_WRITE, "data", "large-dataset.csv")

		// Verify each step has the correct action mapping
		assert.Equal(t, "s3:CreateMultipartUpload", action1, "Step 1: Initiate upload")
		assert.Equal(t, "s3:ListMultipartUploadParts", action2, "Step 2: List parts (FIXED by this PR)")
		assert.Equal(t, "s3:UploadPart", action3, "Step 3: Upload part")
		assert.Equal(t, "s3:CompleteMultipartUpload", action4, "Step 4: Complete upload")

		// Verify that each step requires different permissions (security principle)
		actions := []string{action1, action2, action3, action4}
		for i, action := range actions {
			for j, otherAction := range actions {
				if i != j {
					assert.NotEqual(t, action, otherAction,
						"Each multipart operation step should require different permissions for fine-grained control")
				}
			}
		}
	})

	t.Run("edge_case_upload_ids", func(t *testing.T) {
		// Test various upload ID formats to ensure the fix works with real AWS-compatible upload IDs

		testUploadIds := []string{
			"simple123",
			"complex-upload-id-with-hyphens",
			"upload_with_underscores_123",
			"2VmVGvGhqM0sXnVeBjMNCqtRvr.ygGz0pWPLKAj.YW3zK7VmpFHYuLKVR8OOXnHEhP3WfwlwLKMYJxoHgkGYYv",
			"very-long-upload-id-that-might-be-generated-by-aws-s3-or-compatible-services-abcd1234",
			"uploadId-with.dots.and-dashes_and_underscores123",
		}

		for _, uploadId := range testUploadIds {
			req := &http.Request{
				Method: "GET",
				URL:    &url.URL{Path: "/test-bucket/test-file.bin"},
			}
			query := req.URL.Query()
			query.Set("uploadId", uploadId)
			req.URL.RawQuery = query.Encode()

			action := determineGranularS3Action(req, s3_constants.ACTION_READ, "test-bucket", "test-file.bin")

			assert.Equal(t, "s3:ListMultipartUploadParts", action,
				"Upload ID format %s should be correctly detected and mapped to s3:ListMultipartUploadParts", uploadId)
		}
	})
}
