package s3api

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// TestPostPolicyKeyNormalization tests that object keys from presigned POST
// are properly normalized without leading slashes and with duplicate slashes removed.
// This ensures consistent key handling across the S3 API.
func TestPostPolicyKeyNormalization(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		expectedObject string // Expected normalized object key
	}{
		{
			name:           "key without leading slash",
			key:            "test_image.png",
			expectedObject: "test_image.png",
		},
		{
			name:           "key with leading slash",
			key:            "/test_image.png",
			expectedObject: "test_image.png",
		},
		{
			name:           "key with path without leading slash",
			key:            "folder/subfolder/test_image.png",
			expectedObject: "folder/subfolder/test_image.png",
		},
		{
			name:           "key with path with leading slash",
			key:            "/folder/subfolder/test_image.png",
			expectedObject: "folder/subfolder/test_image.png",
		},
		{
			name:           "simple filename",
			key:            "file.txt",
			expectedObject: "file.txt",
		},
		{
			name:           "key with duplicate slashes",
			key:            "folder//subfolder///file.txt",
			expectedObject: "folder/subfolder/file.txt",
		},
		{
			name:           "key with leading duplicate slashes",
			key:            "//folder/file.txt",
			expectedObject: "folder/file.txt",
		},
		{
			name:           "key with trailing slash",
			key:            "folder/",
			expectedObject: "folder/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use the actual NormalizeObjectKey function
			object := s3_constants.NormalizeObjectKey(tt.key)

			// Verify the normalized object matches expected
			assert.Equal(t, tt.expectedObject, object,
				"Key should be normalized correctly")

			// Verify path construction would be correct
			bucket := "my-bucket"
			bucketsPath := "/buckets"
			expectedPath := bucketsPath + "/" + bucket + "/" + tt.expectedObject
			actualPath := bucketsPath + "/" + bucket + "/" + object

			assert.Equal(t, expectedPath, actualPath,
				"File path should be correctly constructed with slash between bucket and key")

			// Verify we don't have double slashes (except at the start which is fine)
			assert.NotContains(t, actualPath[1:], "//",
				"Path should not contain double slashes")
		})
	}
}

// TestNormalizeObjectKey tests the NormalizeObjectKey function directly
func TestNormalizeObjectKey(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string", "", ""},
		{"simple file", "file.txt", "file.txt"},
		{"with leading slash", "/file.txt", "file.txt"},
		{"path without slash", "a/b/c.txt", "a/b/c.txt"},
		{"path with slash", "/a/b/c.txt", "a/b/c.txt"},
		{"duplicate slashes", "a//b///c.txt", "a/b/c.txt"},
		{"leading duplicates", "///a/b.txt", "a/b.txt"},
		{"all duplicates", "//a//b//", "a/b/"},
		{"just slashes", "///", ""},
		{"trailing slash", "folder/", "folder/"},
		{"backslash to forward slash", "folder\\file.txt", "folder/file.txt"},
		{"windows path", "folder\\subfolder\\file.txt", "folder/subfolder/file.txt"},
		{"mixed slashes", "a/b\\c/d", "a/b/c/d"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s3_constants.NormalizeObjectKey(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPostPolicyFilenameSubstitution tests the ${filename} substitution in keys
func TestPostPolicyFilenameSubstitution(t *testing.T) {
	tests := []struct {
		name             string
		keyTemplate      string
		uploadedFilename string
		expectedKey      string
	}{
		{
			name:             "filename at end",
			keyTemplate:      "uploads/${filename}",
			uploadedFilename: "photo.jpg",
			expectedKey:      "uploads/photo.jpg",
		},
		{
			name:             "filename in middle",
			keyTemplate:      "user/files/${filename}/original",
			uploadedFilename: "document.pdf",
			expectedKey:      "user/files/document.pdf/original",
		},
		{
			name:             "no substitution needed",
			keyTemplate:      "static/file.txt",
			uploadedFilename: "ignored.txt",
			expectedKey:      "static/file.txt",
		},
		{
			name:             "filename only",
			keyTemplate:      "${filename}",
			uploadedFilename: "myfile.png",
			expectedKey:      "myfile.png",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the substitution logic from PostPolicyBucketHandler
			key := tt.keyTemplate
			if tt.uploadedFilename != "" && strings.Contains(key, "${filename}") {
				key = strings.Replace(key, "${filename}", tt.uploadedFilename, -1)
			}

			// Normalize using the actual function
			object := s3_constants.NormalizeObjectKey(key)

			assert.Equal(t, tt.expectedKey, object,
				"Key should be correctly substituted and normalized")
		})
	}
}

// TestExtractPostPolicyFormValues tests the form value extraction
func TestExtractPostPolicyFormValues(t *testing.T) {
	tests := []struct {
		name          string
		key           string
		contentType   string
		fileContent   string
		fileName      string
		expectSuccess bool
	}{
		{
			name:          "basic upload",
			key:           "test.txt",
			contentType:   "text/plain",
			fileContent:   "hello world",
			fileName:      "upload.txt",
			expectSuccess: true,
		},
		{
			name:          "upload with path key",
			key:           "folder/subfolder/test.txt",
			contentType:   "application/octet-stream",
			fileContent:   "binary data",
			fileName:      "data.bin",
			expectSuccess: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create multipart form
			var buf bytes.Buffer
			writer := multipart.NewWriter(&buf)

			// Add form fields
			writer.WriteField("key", tt.key)
			writer.WriteField("Content-Type", tt.contentType)

			// Add file
			part, err := writer.CreateFormFile("file", tt.fileName)
			assert.NoError(t, err)
			_, err = io.WriteString(part, tt.fileContent)
			assert.NoError(t, err)

			err = writer.Close()
			assert.NoError(t, err)

			// Parse the form
			reader := multipart.NewReader(&buf, writer.Boundary())
			form, err := reader.ReadForm(5 * 1024 * 1024)
			assert.NoError(t, err)
			defer form.RemoveAll()

			// Extract values using the actual function
			filePart, fileName, fileContentType, fileSize, formValues, err := extractPostPolicyFormValues(form)

			if tt.expectSuccess {
				assert.NoError(t, err)
				assert.NotNil(t, filePart)
				assert.Equal(t, tt.fileName, fileName)
				assert.NotEmpty(t, fileContentType)
				assert.Greater(t, fileSize, int64(0))
				assert.Equal(t, tt.key, formValues.Get("Key"))

				filePart.Close()
			}
		})
	}
}

// TestPostPolicyPathConstruction is an integration-style test that verifies
// the complete path construction logic
func TestPostPolicyPathConstruction(t *testing.T) {
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}

	tests := []struct {
		name         string
		bucket       string
		formKey      string // Key as it would come from form (may not have leading slash)
		expectedPath string
	}{
		{
			name:         "simple key without slash - the bug case",
			bucket:       "my-bucket",
			formKey:      "test_image.png",
			expectedPath: "/buckets/my-bucket/test_image.png",
		},
		{
			name:         "simple key with slash",
			bucket:       "my-bucket",
			formKey:      "/test_image.png",
			expectedPath: "/buckets/my-bucket/test_image.png",
		},
		{
			name:         "nested path without leading slash",
			bucket:       "uploads",
			formKey:      "2024/01/photo.jpg",
			expectedPath: "/buckets/uploads/2024/01/photo.jpg",
		},
		{
			name:         "nested path with leading slash",
			bucket:       "uploads",
			formKey:      "/2024/01/photo.jpg",
			expectedPath: "/buckets/uploads/2024/01/photo.jpg",
		},
		{
			name:         "key with duplicate slashes",
			bucket:       "my-bucket",
			formKey:      "folder//file.txt",
			expectedPath: "/buckets/my-bucket/folder/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use the actual NormalizeObjectKey function
			object := s3_constants.NormalizeObjectKey(tt.formKey)

			// Construct path as done in PostPolicyBucketHandler
			filePath := s3a.option.BucketsPath + "/" + tt.bucket + "/" + object

			assert.Equal(t, tt.expectedPath, filePath,
				"File path should be correctly constructed")

			// Verify bucket and key are properly separated
			assert.Contains(t, filePath, tt.bucket+"/",
				"Bucket should be followed by a slash")
		})
	}
}

// canonicalFormValues builds an http.Header mirroring how
// extractPostPolicyFormValues canonicalizes the keys in the POST form.
func canonicalFormValues(pairs map[string]string) http.Header {
	h := make(http.Header)
	for k, v := range pairs {
		h[http.CanonicalHeaderKey(k)] = []string{v}
	}
	return h
}

// TestApplyPostPolicyFormHeaders_ForwardsAcl ensures the acl form field is
// promoted to the X-Amz-Acl header on the underlying PUT.
func TestApplyPostPolicyFormHeaders_ForwardsAcl(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/bucket", nil)
	formValues := canonicalFormValues(map[string]string{
		"acl": "public-read",
	})

	applyPostPolicyFormHeaders(r, formValues)

	assert.Equal(t, "public-read", r.Header.Get(s3_constants.AmzCannedAcl))
	assert.Equal(t, "public-read", r.Header.Get("X-Amz-Acl"))
	// The raw "Acl" form key must not be copied verbatim onto the request.
	assert.Empty(t, r.Header.Get("Acl"))
}

// TestApplyPostPolicyFormHeaders_ForwardsContentHeaders ensures the
// Content-Encoding and Content-Language form fields are copied to the
// request header so they reach the underlying PUT.
func TestApplyPostPolicyFormHeaders_ForwardsContentHeaders(t *testing.T) {
	tests := []struct {
		name   string
		header string
		value  string
	}{
		{
			name:   "Content-Encoding",
			header: "Content-Encoding",
			value:  "gzip",
		},
		{
			name:   "Content-Language",
			header: "Content-Language",
			value:  "en-US",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/bucket", nil)
			formValues := canonicalFormValues(map[string]string{
				tt.header: tt.value,
			})

			applyPostPolicyFormHeaders(r, formValues)

			assert.Equal(t, tt.value, r.Header.Get(tt.header))
		})
	}
}

// TestApplyPostPolicyFormHeaders_ForwardsXAmzHeaders ensures arbitrary
// x-amz-* form fields (storage class, tagging, SSE, object lock, website
// redirect, metadata) are all forwarded to the request.
func TestApplyPostPolicyFormHeaders_ForwardsXAmzHeaders(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/bucket", nil)
	formValues := canonicalFormValues(map[string]string{
		"x-amz-storage-class":             "STANDARD_IA",
		"x-amz-tagging":                   "project=alpha&env=prod",
		"x-amz-server-side-encryption":    "AES256",
		"x-amz-object-lock-mode":          "GOVERNANCE",
		"x-amz-website-redirect-location": "/redirected",
		"x-amz-meta-foo":                  "bar",
	})

	applyPostPolicyFormHeaders(r, formValues)

	assert.Equal(t, "STANDARD_IA", r.Header.Get("X-Amz-Storage-Class"))
	assert.Equal(t, "project=alpha&env=prod", r.Header.Get("X-Amz-Tagging"))
	assert.Equal(t, "AES256", r.Header.Get("X-Amz-Server-Side-Encryption"))
	assert.Equal(t, "GOVERNANCE", r.Header.Get("X-Amz-Object-Lock-Mode"))
	assert.Equal(t, "/redirected", r.Header.Get("X-Amz-Website-Redirect-Location"))
	assert.Equal(t, "bar", r.Header.Get("X-Amz-Meta-Foo"))
}

// TestApplyPostPolicyFormHeaders_SkipsReserved ensures POST-policy
// mechanism fields (signatures, key, bucket, success actions, etc.) are not
// leaked as headers on the forwarded PUT.
func TestApplyPostPolicyFormHeaders_SkipsReserved(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/bucket", nil)
	formValues := canonicalFormValues(map[string]string{
		"Policy":                  "base64-encoded-policy",
		"Signature":               "v2-signature",
		"AWSAccessKeyId":          "AKIAEXAMPLE",
		"x-amz-signature":         "v4-signature",
		"x-amz-credential":        "AKIAEXAMPLE/20260417/us-east-1/s3/aws4_request",
		"x-amz-algorithm":         "AWS4-HMAC-SHA256",
		"x-amz-date":              "20260417T000000Z",
		"x-amz-security-token":    "session-token",
		"key":                     "uploads/file.txt",
		"file":                    "ignored",
		"bucket":                  "my-bucket",
		"success_action_redirect": "https://example.com/ok",
		"success_action_status":   "201",
		"redirect":                "https://example.com/legacy",
	})

	applyPostPolicyFormHeaders(r, formValues)

	reserved := []string{
		"Policy",
		"Signature",
		"Awsaccesskeyid",
		"X-Amz-Signature",
		"X-Amz-Credential",
		"X-Amz-Algorithm",
		"X-Amz-Date",
		"X-Amz-Security-Token",
		"Key",
		"File",
		"Bucket",
		"Success_action_redirect",
		"Success_action_status",
		"Redirect",
	}
	for _, k := range reserved {
		assert.Empty(t, r.Header.Get(k), "reserved field %q must not be forwarded", k)
	}
}

// TestApplyPostPolicyFormHeaders_KeepsExistingCacheControl is a regression
// check that the headers previously forwarded (Cache-Control, Expires,
// Content-Disposition) remain forwarded by the refactored helper.
func TestApplyPostPolicyFormHeaders_KeepsExistingCacheControl(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/bucket", nil)
	formValues := canonicalFormValues(map[string]string{
		"Cache-Control":       "max-age=3600",
		"Expires":             "Wed, 21 Oct 2026 07:28:00 GMT",
		"Content-Disposition": `attachment; filename="report.pdf"`,
	})

	applyPostPolicyFormHeaders(r, formValues)

	assert.Equal(t, "max-age=3600", r.Header.Get("Cache-Control"))
	assert.Equal(t, "Wed, 21 Oct 2026 07:28:00 GMT", r.Header.Get("Expires"))
	assert.Equal(t, `attachment; filename="report.pdf"`, r.Header.Get("Content-Disposition"))
}

// TestApplyPostPolicyFormHeaders_IgnoresContentType ensures Content-Type is
// left alone by the helper (it is set by the caller from either the form
// value or the uploaded file part just before invoking the helper).
func TestApplyPostPolicyFormHeaders_IgnoresContentType(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/bucket", nil)
	r.Header.Set("Content-Type", "image/png")
	formValues := canonicalFormValues(map[string]string{
		"Content-Type": "text/plain",
	})

	applyPostPolicyFormHeaders(r, formValues)

	// The helper must not overwrite the Content-Type that the handler has
	// already resolved from the form or from the file part.
	assert.Equal(t, "image/png", r.Header.Get("Content-Type"))
}

// TestPostPolicyBucketHandlerKeyExtraction tests that the handler correctly
// extracts and normalizes the key from a POST request
func TestPostPolicyBucketHandlerKeyExtraction(t *testing.T) {
	// Create a minimal S3ApiServer for testing
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
		iam: &IdentityAccessManagement{},
	}

	tests := []struct {
		name        string
		bucket      string
		key         string
		wantPathHas string // substring that must be in the constructed path
	}{
		{
			name:        "key without leading slash",
			bucket:      "test-bucket",
			key:         "simple-file.txt",
			wantPathHas: "/test-bucket/simple-file.txt",
		},
		{
			name:        "key with leading slash",
			bucket:      "test-bucket",
			key:         "/prefixed-file.txt",
			wantPathHas: "/test-bucket/prefixed-file.txt",
		},
		{
			name:        "key with duplicate slashes",
			bucket:      "test-bucket",
			key:         "folder//nested///file.txt",
			wantPathHas: "/test-bucket/folder/nested/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create multipart form body
			var buf bytes.Buffer
			writer := multipart.NewWriter(&buf)

			// Add required fields
			writer.WriteField("key", tt.key)
			writer.WriteField("Policy", "") // Empty policy for this test

			// Add file
			part, _ := writer.CreateFormFile("file", "test.txt")
			part.Write([]byte("test content"))
			writer.Close()

			// Create request
			req := httptest.NewRequest(http.MethodPost, "/"+tt.bucket, &buf)
			req.Header.Set("Content-Type", writer.FormDataContentType())

			// Set up mux vars (simulating router)
			req = mux.SetURLVars(req, map[string]string{"bucket": tt.bucket})

			// Parse form to extract key
			reader, _ := req.MultipartReader()
			form, _ := reader.ReadForm(5 * 1024 * 1024)
			defer form.RemoveAll()

			_, _, _, _, formValues, _ := extractPostPolicyFormValues(form)

			// Apply the same normalization as PostPolicyBucketHandler
			object := s3_constants.NormalizeObjectKey(formValues.Get("Key"))

			// Construct path
			filePath := s3a.option.BucketsPath + "/" + tt.bucket + "/" + object

			assert.Contains(t, filePath, tt.wantPathHas,
				"Path should contain properly separated bucket and key")
		})
	}
}
