package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
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
			TokenDuration:    time.Hour,
			MaxSessionLength: time.Hour * 12,
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

	err := iamManager.Initialize(config)
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
			expected: "arn:seaweed:s3:::*",
		},
		{
			name:     "bucket only",
			bucket:   "test-bucket",
			object:   "",
			expected: "arn:seaweed:s3:::test-bucket",
		},
		{
			name:     "bucket and object",
			bucket:   "test-bucket",
			object:   "test-object.txt",
			expected: "arn:seaweed:s3:::test-bucket/test-object.txt",
		},
		{
			name:     "bucket and object with leading slash",
			bucket:   "test-bucket",
			object:   "/test-object.txt",
			expected: "arn:seaweed:s3:::test-bucket/test-object.txt",
		},
		{
			name:     "bucket and nested object",
			bucket:   "test-bucket",
			object:   "folder/subfolder/test-object.txt",
			expected: "arn:seaweed:s3:::test-bucket/folder/subfolder/test-object.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildS3ResourceArn(tt.bucket, tt.object)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestMapS3ActionToIAMAction tests S3 to IAM action mapping
func TestMapS3ActionToIAMAction(t *testing.T) {
	tests := []struct {
		name     string
		s3Action Action
		expected string
	}{
		{
			name:     "read action",
			s3Action: "READ", // Assuming this is defined in s3_constants
			expected: "READ", // Will fallback to string representation
		},
		{
			name:     "write action",
			s3Action: "WRITE",
			expected: "WRITE",
		},
		{
			name:     "list action",
			s3Action: "LIST",
			expected: "LIST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapS3ActionToIAMAction(tt.s3Action)
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
			principal: "arn:seaweed:sts::assumed-role/S3ReadOnlyRole/session-123",
			expected:  "S3ReadOnlyRole",
		},
		{
			name:      "invalid format",
			principal: "invalid-principal",
			expected:  "invalid-principal", // Returns original on failure
		},
		{
			name:      "missing session name",
			principal: "arn:seaweed:sts::assumed-role/TestRole",
			expected:  "arn:seaweed:sts::assumed-role/TestRole", // Returns original on failure
		},
		{
			name:      "empty principal",
			principal: "",
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractRoleNameFromPrincipal(tt.principal)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIAMIdentityIsAdmin tests the IsAdmin method
func TestIAMIdentityIsAdmin(t *testing.T) {
	identity := &IAMIdentity{
		Name:         "test-identity",
		Principal:    "arn:seaweed:sts::assumed-role/TestRole/session",
		SessionToken: "test-token",
	}

	// In our implementation, IsAdmin always returns false since admin status
	// is determined by policies, not identity
	result := identity.IsAdmin()
	assert.False(t, result)
}
