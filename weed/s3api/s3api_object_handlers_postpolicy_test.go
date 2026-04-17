package s3api

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
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

// TestPostPolicyBucketHandler_PolicyViolationReturns403 drives the handler
// end-to-end with a signed multipart POST whose policy conditions cannot be
// satisfied by the form fields. It verifies the handler responds 403
// AccessDenied with no Location redirect, rather than the old 307 Temporary
// Redirect that obscured the policy failure.
func TestPostPolicyBucketHandler_PolicyViolationReturns403(t *testing.T) {
	const (
		accessKey  = "AKIATESTTESTTEST"
		secretKey  = "secret-key-for-tests"
		region     = "us-east-1"
		service    = "s3"
		testBucket = "test-bucket"
	)

	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "tester",
			Credentials: []*iam_pb.Credential{{AccessKey: accessKey, SecretKey: secretKey}},
			Actions:     []string{"Admin", "Read", "Write"},
		}},
	})
	assert.NoError(t, err, "loadS3ApiConfiguration should succeed")

	s3a := &S3ApiServer{
		option: &S3ApiServerOption{BucketsPath: "/buckets"},
		iam:    iam,
	}
	// Pre-populate the bucket registry so validateTableBucketObjectPath sees
	// a non-table bucket without needing a live filer connection.
	s3a.bucketRegistry = &BucketRegistry{
		metadataCache: map[string]*BucketMetaData{
			testBucket: {Name: testBucket, IsTableBucket: false},
		},
		notFound: make(map[string]struct{}),
		s3a:      s3a,
	}

	now := time.Now().UTC()
	amzDate := now.Format(iso8601Format)
	yyyymmddStr := now.Format(yyyymmdd)
	credential := fmt.Sprintf("%s/%s/%s/%s/aws4_request", accessKey, yyyymmddStr, region, service)
	expiration := now.Add(1 * time.Hour).Format("2006-01-02T15:04:05.000Z")

	policyJSON := fmt.Sprintf(
		`{"expiration":"%s","conditions":[`+
			`["eq","$bucket","%s"],`+
			`["eq","$key","required.txt"],`+
			`["eq","$x-amz-credential","%s"],`+
			`["eq","$x-amz-algorithm","AWS4-HMAC-SHA256"],`+
			`["eq","$x-amz-date","%s"]`+
			`]}`,
		expiration, testBucket, credential, amzDate,
	)
	encodedPolicy := base64.StdEncoding.EncodeToString([]byte(policyJSON))

	signingKey := getSigningKey(secretKey, yyyymmddStr, region, service)
	signature := getSignature(signingKey, encodedPolicy)
	// Sanity-check: the signature must be valid hex of the expected length so
	// that doesPolicySignatureV4Match does not trip on formatting.
	_, decodeErr := hex.DecodeString(signature)
	assert.NoError(t, decodeErr, "computed signature should be hex-encoded")

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	assert.NoError(t, writer.WriteField("bucket", testBucket))
	// Deliberately mismatch the policy's required key to force a violation.
	assert.NoError(t, writer.WriteField("key", "wrong.txt"))
	assert.NoError(t, writer.WriteField("x-amz-credential", credential))
	assert.NoError(t, writer.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256"))
	assert.NoError(t, writer.WriteField("x-amz-date", amzDate))
	assert.NoError(t, writer.WriteField("policy", encodedPolicy))
	assert.NoError(t, writer.WriteField("x-amz-signature", signature))

	filePart, err := writer.CreateFormFile("file", "payload.txt")
	assert.NoError(t, err)
	_, err = filePart.Write([]byte("contents that should never be uploaded"))
	assert.NoError(t, err)
	assert.NoError(t, writer.Close())

	req := httptest.NewRequest(http.MethodPost, "/"+testBucket, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req = mux.SetURLVars(req, map[string]string{"bucket": testBucket})
	rec := httptest.NewRecorder()

	s3a.PostPolicyBucketHandler(rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code,
		"policy violation must return 403, actual body: %s", rec.Body.String())
	assert.NotEqual(t, http.StatusTemporaryRedirect, rec.Code,
		"must not return 307 Temporary Redirect on policy violation")
	assert.Empty(t, rec.Header().Get("Location"),
		"403 response must not set a redirect Location header")
	assert.Contains(t, rec.Body.String(), "AccessDenied",
		"response body should identify AccessDenied; actual body: %s", rec.Body.String())
	assert.Contains(t, rec.Body.String(), "Policy Condition failed",
		"response body should carry the specific policy-failure message; actual body: %s", rec.Body.String())
	// Guard against the signing setup silently failing before the policy
	// branch ever runs, which would make the 403 assertion meaningless.
	assert.NotContains(t, rec.Body.String(), "SignatureDoesNotMatch",
		"signature must match so the handler reaches the policy-check branch")
	assert.NotContains(t, rec.Body.String(), "InvalidAccessKeyId",
		"access key must be known so the handler reaches the policy-check branch")
}
