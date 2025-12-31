package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3IAMMiddleware tests the basic S3 IAM middleware functionality
func TestS3IAMMiddleware(t *testing.T) {
	// Create IAM manager
	iamManager := integration.NewIAMManager()

	// Initialize with test configuration
	config := &integration.IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: time.Hour * 12},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{
			DefaultEffect: "Deny",
			StoreType:     "memory",
		},
		Roles: &integration.RoleStoreConfig{
			StoreType: "memory",
		},
	}

	err := iamManager.Initialize(config, func() string {
		return "localhost:8888" // Mock filer address for testing
	})
	require.NoError(t, err)

	// Create S3 IAM integration
	s3IAMIntegration := NewS3IAMIntegration(iamManager, "localhost:8888")

	// Test that integration is created successfully
	assert.NotNil(t, s3IAMIntegration)
	assert.True(t, s3IAMIntegration.enabled)
}

// TestS3IAMMiddlewareJWTAuth tests JWT authentication
func TestS3IAMMiddlewareJWTAuth(t *testing.T) {
	// Skip for now since it requires full setup
	t.Skip("JWT authentication test requires full IAM setup")

	// Create IAM integration
	s3iam := NewS3IAMIntegration(nil, "localhost:8888") // Disabled integration

	// Create test request with JWT token
	req := httptest.NewRequest("GET", "/test-bucket/test-object", http.NoBody)
	req.Header.Set("Authorization", "Bearer test-token")

	// Test authentication (should return not implemented when disabled)
	ctx := context.Background()
	identity, errCode := s3iam.AuthenticateJWT(ctx, req)

	assert.Nil(t, identity)
	assert.NotEqual(t, errCode, 0) // Should return an error
}

// TestBuildS3ResourceArn tests resource ARN building
func TestBuildS3ResourceArn(t *testing.T) {
	tests := []struct {
		name     string
		bucket   string
		object   string
		expected string
	}{
		{
			name:     "empty bucket and object",
			bucket:   "",
			object:   "",
			expected: "arn:aws:s3:::*",
		},
		{
			name:     "bucket only",
			bucket:   "test-bucket",
			object:   "",
			expected: "arn:aws:s3:::test-bucket",
		},
		{
			name:     "bucket and object",
			bucket:   "test-bucket",
			object:   "test-object.txt",
			expected: "arn:aws:s3:::test-bucket/test-object.txt",
		},
		{
			name:     "bucket and object with leading slash",
			bucket:   "test-bucket",
			object:   "/test-object.txt",
			expected: "arn:aws:s3:::test-bucket/test-object.txt",
		},
		{
			name:     "bucket and nested object",
			bucket:   "test-bucket",
			object:   "folder/subfolder/test-object.txt",
			expected: "arn:aws:s3:::test-bucket/folder/subfolder/test-object.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildS3ResourceArn(tt.bucket, tt.object)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDetermineGranularS3Action tests granular S3 action determination from HTTP requests
func TestDetermineGranularS3Action(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		bucket         string
		objectKey      string
		queryParams    map[string]string
		fallbackAction Action
		expected       string
		description    string
	}{
		// Object-level operations
		{
			name:           "get_object",
			method:         "GET",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{},
			fallbackAction: s3_constants.ACTION_READ,
			expected:       "s3:GetObject",
			description:    "Basic object retrieval",
		},
		{
			name:           "get_object_acl",
			method:         "GET",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{"acl": ""},
			fallbackAction: s3_constants.ACTION_READ_ACP,
			expected:       "s3:GetObjectAcl",
			description:    "Object ACL retrieval",
		},
		{
			name:           "get_object_tagging",
			method:         "GET",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{"tagging": ""},
			fallbackAction: s3_constants.ACTION_TAGGING,
			expected:       "s3:GetObjectTagging",
			description:    "Object tagging retrieval",
		},
		{
			name:           "put_object",
			method:         "PUT",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{},
			fallbackAction: s3_constants.ACTION_WRITE,
			expected:       "s3:PutObject",
			description:    "Basic object upload",
		},
		{
			name:           "put_object_acl",
			method:         "PUT",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{"acl": ""},
			fallbackAction: s3_constants.ACTION_WRITE_ACP,
			expected:       "s3:PutObjectAcl",
			description:    "Object ACL modification",
		},
		{
			name:           "delete_object",
			method:         "DELETE",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{},
			fallbackAction: s3_constants.ACTION_WRITE, // DELETE object uses WRITE fallback
			expected:       "s3:DeleteObject",
			description:    "Object deletion - correctly mapped to DeleteObject (not PutObject)",
		},
		{
			name:           "delete_object_tagging",
			method:         "DELETE",
			bucket:         "test-bucket",
			objectKey:      "test-object.txt",
			queryParams:    map[string]string{"tagging": ""},
			fallbackAction: s3_constants.ACTION_TAGGING,
			expected:       "s3:DeleteObjectTagging",
			description:    "Object tag deletion",
		},

		// Multipart upload operations
		{
			name:           "create_multipart_upload",
			method:         "POST",
			bucket:         "test-bucket",
			objectKey:      "large-file.txt",
			queryParams:    map[string]string{"uploads": ""},
			fallbackAction: s3_constants.ACTION_WRITE,
			expected:       "s3:CreateMultipartUpload",
			description:    "Multipart upload initiation",
		},
		{
			name:           "upload_part",
			method:         "PUT",
			bucket:         "test-bucket",
			objectKey:      "large-file.txt",
			queryParams:    map[string]string{"uploadId": "12345", "partNumber": "1"},
			fallbackAction: s3_constants.ACTION_WRITE,
			expected:       "s3:UploadPart",
			description:    "Multipart part upload",
		},
		{
			name:           "complete_multipart_upload",
			method:         "POST",
			bucket:         "test-bucket",
			objectKey:      "large-file.txt",
			queryParams:    map[string]string{"uploadId": "12345"},
			fallbackAction: s3_constants.ACTION_WRITE,
			expected:       "s3:CompleteMultipartUpload",
			description:    "Multipart upload completion",
		},
		{
			name:           "abort_multipart_upload",
			method:         "DELETE",
			bucket:         "test-bucket",
			objectKey:      "large-file.txt",
			queryParams:    map[string]string{"uploadId": "12345"},
			fallbackAction: s3_constants.ACTION_WRITE,
			expected:       "s3:AbortMultipartUpload",
			description:    "Multipart upload abort",
		},

		// Bucket-level operations
		{
			name:           "list_bucket",
			method:         "GET",
			bucket:         "test-bucket",
			objectKey:      "",
			queryParams:    map[string]string{},
			fallbackAction: s3_constants.ACTION_LIST,
			expected:       "s3:ListBucket",
			description:    "Bucket listing",
		},
		{
			name:           "get_bucket_acl",
			method:         "GET",
			bucket:         "test-bucket",
			objectKey:      "",
			queryParams:    map[string]string{"acl": ""},
			fallbackAction: s3_constants.ACTION_READ_ACP,
			expected:       "s3:GetBucketAcl",
			description:    "Bucket ACL retrieval",
		},
		{
			name:           "put_bucket_policy",
			method:         "PUT",
			bucket:         "test-bucket",
			objectKey:      "",
			queryParams:    map[string]string{"policy": ""},
			fallbackAction: s3_constants.ACTION_WRITE,
			expected:       "s3:PutBucketPolicy",
			description:    "Bucket policy modification",
		},
		{
			name:           "delete_bucket",
			method:         "DELETE",
			bucket:         "test-bucket",
			objectKey:      "",
			queryParams:    map[string]string{},
			fallbackAction: s3_constants.ACTION_DELETE_BUCKET,
			expected:       "s3:DeleteBucket",
			description:    "Bucket deletion",
		},
		{
			name:           "list_multipart_uploads",
			method:         "GET",
			bucket:         "test-bucket",
			objectKey:      "",
			queryParams:    map[string]string{"uploads": ""},
			fallbackAction: s3_constants.ACTION_LIST,
			expected:       "s3:ListBucketMultipartUploads",
			description:    "List multipart uploads in bucket",
		},

		// Fallback scenarios
		{
			name:           "legacy_read_fallback",
			method:         "GET",
			bucket:         "",
			objectKey:      "",
			queryParams:    map[string]string{},
			fallbackAction: s3_constants.ACTION_READ,
			expected:       "s3:GetObject",
			description:    "Legacy read action fallback",
		},
		{
			name:           "already_granular_action",
			method:         "GET",
			bucket:         "",
			objectKey:      "",
			queryParams:    map[string]string{},
			fallbackAction: "s3:GetBucketLocation", // Already granular
			expected:       "s3:GetBucketLocation",
			description:    "Already granular action passed through",
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

			// Test the action determination
			result := ResolveS3Action(req, string(tt.fallbackAction), tt.bucket, tt.objectKey)

			assert.Equal(t, tt.expected, result,
				"Test %s failed: %s. Expected %s but got %s",
				tt.name, tt.description, tt.expected, result)
		})
	}
}

// TestMapLegacyActionToIAM tests the legacy action fallback mapping
func TestMapLegacyActionToIAM(t *testing.T) {
	tests := []struct {
		name         string
		legacyAction Action
		expected     string
	}{
		{
			name:         "read_action_fallback",
			legacyAction: s3_constants.ACTION_READ,
			expected:     "s3:GetObject",
		},
		{
			name:         "write_action_fallback",
			legacyAction: s3_constants.ACTION_WRITE,
			expected:     "s3:PutObject",
		},
		{
			name:         "admin_action_fallback",
			legacyAction: s3_constants.ACTION_ADMIN,
			expected:     "s3:*",
		},
		{
			name:         "granular_multipart_action",
			legacyAction: s3_constants.ACTION_CREATE_MULTIPART_UPLOAD,
			expected:     "s3:CreateMultipartUpload",
		},
		{
			name:         "unknown_action_with_s3_prefix",
			legacyAction: "s3:CustomAction",
			expected:     "s3:CustomAction",
		},
		{
			name:         "unknown_action_without_prefix",
			legacyAction: "CustomAction",
			expected:     "s3:CustomAction",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapLegacyActionToIAM(tt.legacyAction)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractSourceIP tests source IP extraction from requests
func TestExtractSourceIP(t *testing.T) {
	tests := []struct {
		name       string
		setupReq   func() *http.Request
		expectedIP string
	}{
		{
			name: "X-Forwarded-For header",
			setupReq: func() *http.Request {
				req := httptest.NewRequest("GET", "/test", http.NoBody)
				req.Header.Set("X-Forwarded-For", "192.168.1.100, 10.0.0.1")
				return req
			},
			expectedIP: "192.168.1.100",
		},
		{
			name: "X-Real-IP header",
			setupReq: func() *http.Request {
				req := httptest.NewRequest("GET", "/test", http.NoBody)
				req.Header.Set("X-Real-IP", "192.168.1.200")
				return req
			},
			expectedIP: "192.168.1.200",
		},
		{
			name: "RemoteAddr fallback",
			setupReq: func() *http.Request {
				req := httptest.NewRequest("GET", "/test", http.NoBody)
				req.RemoteAddr = "192.168.1.300:12345"
				return req
			},
			expectedIP: "192.168.1.300",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.setupReq()
			result := extractSourceIP(req)
			assert.Equal(t, tt.expectedIP, result)
		})
	}
}

// TestExtractRoleNameFromPrincipal tests role name extraction
func TestExtractRoleNameFromPrincipal(t *testing.T) {
	tests := []struct {
		name      string
		principal string
		expected  string
	}{
		{
			name:      "valid assumed role ARN",
			principal: "arn:aws:sts::assumed-role/S3ReadOnlyRole/session-123",
			expected:  "S3ReadOnlyRole",
		},
		{
			name:      "invalid format",
			principal: "invalid-principal",
			expected:  "", // Returns empty string to signal invalid format
		},
		{
			name:      "missing session name",
			principal: "arn:aws:sts::assumed-role/TestRole",
			expected:  "TestRole", // Extracts role name even without session name
		},
		{
			name:      "empty principal",
			principal: "",
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.ExtractRoleNameFromPrincipal(tt.principal)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIAMIdentityIsAdmin tests the IsAdmin method
func TestIAMIdentityIsAdmin(t *testing.T) {
	identity := &IAMIdentity{
		Name:         "test-identity",
		Principal:    "arn:aws:sts::assumed-role/TestRole/session",
		SessionToken: "test-token",
	}

	// In our implementation, IsAdmin always returns false since admin status
	// is determined by policies, not identity
	result := identity.IsAdmin()
	assert.False(t, result)
}
