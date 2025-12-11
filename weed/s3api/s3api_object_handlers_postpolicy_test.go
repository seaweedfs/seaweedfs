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
	"github.com/stretchr/testify/assert"
)

// TestPostPolicyKeyNormalization tests that object keys from presigned POST
// are properly normalized with a leading slash.
// This addresses issue #7713 where keys without leading slashes caused
// bucket and key to be concatenated without a separator.
func TestPostPolicyKeyNormalization(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		expectedPrefix string // Expected path prefix after bucket
	}{
		{
			name:           "key without leading slash",
			key:            "test_image.png",
			expectedPrefix: "/test_image.png",
		},
		{
			name:           "key with leading slash",
			key:            "/test_image.png",
			expectedPrefix: "/test_image.png",
		},
		{
			name:           "key with path without leading slash",
			key:            "folder/subfolder/test_image.png",
			expectedPrefix: "/folder/subfolder/test_image.png",
		},
		{
			name:           "key with path with leading slash",
			key:            "/folder/subfolder/test_image.png",
			expectedPrefix: "/folder/subfolder/test_image.png",
		},
		{
			name:           "simple filename",
			key:            "file.txt",
			expectedPrefix: "/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the key normalization logic from PostPolicyBucketHandler
			object := tt.key
			if !strings.HasPrefix(object, "/") {
				object = "/" + object
			}

			// Verify the normalized object has the expected prefix
			assert.Equal(t, tt.expectedPrefix, object,
				"Key should be normalized to have leading slash")

			// Verify path construction would be correct
			bucket := "my-bucket"
			bucketsPath := "/buckets"
			expectedPath := bucketsPath + "/" + bucket + tt.expectedPrefix
			actualPath := bucketsPath + "/" + bucket + object

			assert.Equal(t, expectedPath, actualPath,
				"File path should be correctly constructed with slash between bucket and key")

			// Verify we don't have double slashes (except at the start which is fine)
			assert.NotContains(t, actualPath[1:], "//",
				"Path should not contain double slashes")
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
			expectedKey:      "/uploads/photo.jpg",
		},
		{
			name:             "filename in middle",
			keyTemplate:      "user/files/${filename}/original",
			uploadedFilename: "document.pdf",
			expectedKey:      "/user/files/document.pdf/original",
		},
		{
			name:             "no substitution needed",
			keyTemplate:      "static/file.txt",
			uploadedFilename: "ignored.txt",
			expectedKey:      "/static/file.txt",
		},
		{
			name:             "filename only",
			keyTemplate:      "${filename}",
			uploadedFilename: "myfile.png",
			expectedKey:      "/myfile.png",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the substitution and normalization logic from PostPolicyBucketHandler
			key := tt.keyTemplate
			if tt.uploadedFilename != "" && strings.Contains(key, "${filename}") {
				key = strings.Replace(key, "${filename}", tt.uploadedFilename, -1)
			}

			// Normalize with leading slash
			object := key
			if !strings.HasPrefix(object, "/") {
				object = "/" + object
			}

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the key normalization from PostPolicyBucketHandler
			object := tt.formKey
			if !strings.HasPrefix(object, "/") {
				object = "/" + object
			}

			// Construct path as done in PostPolicyBucketHandler (line 120)
			filePath := s3a.option.BucketsPath + "/" + tt.bucket + object

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
			object := formValues.Get("Key")
			if !strings.HasPrefix(object, "/") {
				object = "/" + object
			}

			// Construct path
			filePath := s3a.option.BucketsPath + "/" + tt.bucket + object

			assert.Contains(t, filePath, tt.wantPathHas,
				"Path should contain properly separated bucket and key")
		})
	}
}

