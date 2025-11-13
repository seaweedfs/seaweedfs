package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/ldap"
	"github.com/seaweedfs/seaweedfs/weed/iam/oidc"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestJWTMultipart creates a test JWT token with the specified issuer, subject and signing key
func createTestJWTMultipart(t *testing.T, issuer, subject, signingKey string) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": issuer,
		"sub": subject,
		"aud": "test-client-id",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
		// Add claims that trust policy validation expects
		"idp": "test-oidc", // Identity provider claim for trust policy matching
	})

	tokenString, err := token.SignedString([]byte(signingKey))
	require.NoError(t, err)
	return tokenString
}

// TestMultipartIAMValidation tests IAM validation for multipart operations
func TestMultipartIAMValidation(t *testing.T) {
	// Set up IAM system
	iamManager := setupTestIAMManagerForMultipart(t)
	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")
	s3iam.enabled = true

	// Create IAM with integration
	iam := &IdentityAccessManagement{
		isAuthEnabled: true,
	}
	iam.SetIAMIntegration(s3iam)

	// Set up roles
	ctx := context.Background()
	setupTestRolesForMultipart(ctx, iamManager)

	// Create a valid JWT token for testing
	validJWTToken := createTestJWTMultipart(t, "https://test-issuer.com", "test-user-123", "test-signing-key")

	// Get session token
	response, err := iamManager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/S3WriteRole",
		WebIdentityToken: validJWTToken,
		RoleSessionName:  "multipart-test-session",
	})
	require.NoError(t, err)

	sessionToken := response.Credentials.SessionToken

	tests := []struct {
		name           string
		operation      MultipartOperation
		method         string
		path           string
		sessionToken   string
		expectedResult s3err.ErrorCode
	}{
		{
			name:           "Initiate multipart upload",
			operation:      MultipartOpInitiate,
			method:         "POST",
			path:           "/test-bucket/test-file.txt?uploads",
			sessionToken:   sessionToken,
			expectedResult: s3err.ErrNone,
		},
		{
			name:           "Upload part",
			operation:      MultipartOpUploadPart,
			method:         "PUT",
			path:           "/test-bucket/test-file.txt?partNumber=1&uploadId=test-upload-id",
			sessionToken:   sessionToken,
			expectedResult: s3err.ErrNone,
		},
		{
			name:           "Complete multipart upload",
			operation:      MultipartOpComplete,
			method:         "POST",
			path:           "/test-bucket/test-file.txt?uploadId=test-upload-id",
			sessionToken:   sessionToken,
			expectedResult: s3err.ErrNone,
		},
		{
			name:           "Abort multipart upload",
			operation:      MultipartOpAbort,
			method:         "DELETE",
			path:           "/test-bucket/test-file.txt?uploadId=test-upload-id",
			sessionToken:   sessionToken,
			expectedResult: s3err.ErrNone,
		},
		{
			name:           "List multipart uploads",
			operation:      MultipartOpList,
			method:         "GET",
			path:           "/test-bucket?uploads",
			sessionToken:   sessionToken,
			expectedResult: s3err.ErrNone,
		},
		{
			name:           "Upload part without session token",
			operation:      MultipartOpUploadPart,
			method:         "PUT",
			path:           "/test-bucket/test-file.txt?partNumber=1&uploadId=test-upload-id",
			sessionToken:   "",
			expectedResult: s3err.ErrNone, // Falls back to standard auth
		},
		{
			name:           "Upload part with invalid session token",
			operation:      MultipartOpUploadPart,
			method:         "PUT",
			path:           "/test-bucket/test-file.txt?partNumber=1&uploadId=test-upload-id",
			sessionToken:   "invalid-token",
			expectedResult: s3err.ErrAccessDenied,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request for multipart operation
			req := createMultipartRequest(t, tt.method, tt.path, tt.sessionToken)

			// Create identity for testing
			identity := &Identity{
				Name:    "test-user",
				Account: &AccountAdmin,
			}

			// Test validation
			result := iam.ValidateMultipartOperationWithIAM(req, identity, tt.operation)
			assert.Equal(t, tt.expectedResult, result, "Multipart IAM validation result should match expected")
		})
	}
}

// TestMultipartUploadPolicy tests multipart upload security policies
func TestMultipartUploadPolicy(t *testing.T) {
	policy := &MultipartUploadPolicy{
		MaxPartSize:         10 * 1024 * 1024, // 10MB for testing
		MinPartSize:         5 * 1024 * 1024,  // 5MB minimum
		MaxParts:            100,              // 100 parts max for testing
		AllowedContentTypes: []string{"application/json", "text/plain"},
		RequiredHeaders:     []string{"Content-Type"},
	}

	tests := []struct {
		name          string
		request       *MultipartUploadRequest
		expectedError string
	}{
		{
			name: "Valid upload part request",
			request: &MultipartUploadRequest{
				Bucket:      "test-bucket",
				ObjectKey:   "test-file.txt",
				PartNumber:  1,
				Operation:   string(MultipartOpUploadPart),
				ContentSize: 8 * 1024 * 1024, // 8MB
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			},
			expectedError: "",
		},
		{
			name: "Part size too large",
			request: &MultipartUploadRequest{
				Bucket:      "test-bucket",
				ObjectKey:   "test-file.txt",
				PartNumber:  1,
				Operation:   string(MultipartOpUploadPart),
				ContentSize: 15 * 1024 * 1024, // 15MB exceeds limit
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			},
			expectedError: "part size",
		},
		{
			name: "Invalid part number (too high)",
			request: &MultipartUploadRequest{
				Bucket:      "test-bucket",
				ObjectKey:   "test-file.txt",
				PartNumber:  150, // Exceeds max parts
				Operation:   string(MultipartOpUploadPart),
				ContentSize: 8 * 1024 * 1024,
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			},
			expectedError: "part number",
		},
		{
			name: "Invalid part number (too low)",
			request: &MultipartUploadRequest{
				Bucket:      "test-bucket",
				ObjectKey:   "test-file.txt",
				PartNumber:  0, // Must be >= 1
				Operation:   string(MultipartOpUploadPart),
				ContentSize: 8 * 1024 * 1024,
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			},
			expectedError: "part number",
		},
		{
			name: "Content type not allowed",
			request: &MultipartUploadRequest{
				Bucket:      "test-bucket",
				ObjectKey:   "test-file.txt",
				PartNumber:  1,
				Operation:   string(MultipartOpUploadPart),
				ContentSize: 8 * 1024 * 1024,
				Headers: map[string]string{
					"Content-Type": "video/mp4", // Not in allowed list
				},
			},
			expectedError: "content type video/mp4 is not allowed",
		},
		{
			name: "Missing required header",
			request: &MultipartUploadRequest{
				Bucket:      "test-bucket",
				ObjectKey:   "test-file.txt",
				PartNumber:  1,
				Operation:   string(MultipartOpUploadPart),
				ContentSize: 8 * 1024 * 1024,
				Headers:     map[string]string{}, // Missing Content-Type
			},
			expectedError: "required header Content-Type is missing",
		},
		{
			name: "Non-upload operation (should not validate size)",
			request: &MultipartUploadRequest{
				Bucket:    "test-bucket",
				ObjectKey: "test-file.txt",
				Operation: string(MultipartOpInitiate),
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
			},
			expectedError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := policy.ValidateMultipartRequestWithPolicy(tt.request)

			if tt.expectedError == "" {
				assert.NoError(t, err, "Policy validation should succeed")
			} else {
				assert.Error(t, err, "Policy validation should fail")
				assert.Contains(t, err.Error(), tt.expectedError, "Error message should contain expected text")
			}
		})
	}
}

// TestMultipartS3ActionMapping tests the mapping of multipart operations to S3 actions
func TestMultipartS3ActionMapping(t *testing.T) {
	tests := []struct {
		operation      MultipartOperation
		expectedAction Action
	}{
		{MultipartOpInitiate, s3_constants.ACTION_CREATE_MULTIPART_UPLOAD},
		{MultipartOpUploadPart, s3_constants.ACTION_UPLOAD_PART},
		{MultipartOpComplete, s3_constants.ACTION_COMPLETE_MULTIPART},
		{MultipartOpAbort, s3_constants.ACTION_ABORT_MULTIPART},
		{MultipartOpList, s3_constants.ACTION_LIST_MULTIPART_UPLOADS},
		{MultipartOpListParts, s3_constants.ACTION_LIST_PARTS},
		{MultipartOperation("unknown"), "s3:InternalErrorUnknownMultipartAction"}, // Fail-closed for security
	}

	for _, tt := range tests {
		t.Run(string(tt.operation), func(t *testing.T) {
			action := determineMultipartS3Action(tt.operation)
			assert.Equal(t, tt.expectedAction, action, "S3 action mapping should match expected")
		})
	}
}

// TestSessionTokenExtraction tests session token extraction from various sources
func TestSessionTokenExtraction(t *testing.T) {
	tests := []struct {
		name          string
		setupRequest  func() *http.Request
		expectedToken string
	}{
		{
			name: "Bearer token in Authorization header",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("PUT", "/test-bucket/test-file.txt", nil)
				req.Header.Set("Authorization", "Bearer test-session-token-123")
				return req
			},
			expectedToken: "test-session-token-123",
		},
		{
			name: "X-Amz-Security-Token header",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("PUT", "/test-bucket/test-file.txt", nil)
				req.Header.Set("X-Amz-Security-Token", "security-token-456")
				return req
			},
			expectedToken: "security-token-456",
		},
		{
			name: "X-Amz-Security-Token query parameter",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("PUT", "/test-bucket/test-file.txt?X-Amz-Security-Token=query-token-789", nil)
				return req
			},
			expectedToken: "query-token-789",
		},
		{
			name: "No token present",
			setupRequest: func() *http.Request {
				return httptest.NewRequest("PUT", "/test-bucket/test-file.txt", nil)
			},
			expectedToken: "",
		},
		{
			name: "Authorization header without Bearer",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("PUT", "/test-bucket/test-file.txt", nil)
				req.Header.Set("Authorization", "AWS access_key:signature")
				return req
			},
			expectedToken: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.setupRequest()
			token := extractSessionTokenFromRequest(req)
			assert.Equal(t, tt.expectedToken, token, "Extracted token should match expected")
		})
	}
}

// TestUploadPartValidation tests upload part request validation
func TestUploadPartValidation(t *testing.T) {
	s3Server := &S3ApiServer{}

	tests := []struct {
		name          string
		setupRequest  func() *http.Request
		expectedError string
	}{
		{
			name: "Valid upload part request",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("PUT", "/test-bucket/test-file.txt?partNumber=1&uploadId=test-123", nil)
				req.Header.Set("Content-Type", "application/octet-stream")
				req.ContentLength = 6 * 1024 * 1024 // 6MB
				return req
			},
			expectedError: "",
		},
		{
			name: "Missing partNumber parameter",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("PUT", "/test-bucket/test-file.txt?uploadId=test-123", nil)
				req.Header.Set("Content-Type", "application/octet-stream")
				req.ContentLength = 6 * 1024 * 1024
				return req
			},
			expectedError: "missing partNumber parameter",
		},
		{
			name: "Invalid partNumber format",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("PUT", "/test-bucket/test-file.txt?partNumber=abc&uploadId=test-123", nil)
				req.Header.Set("Content-Type", "application/octet-stream")
				req.ContentLength = 6 * 1024 * 1024
				return req
			},
			expectedError: "invalid partNumber",
		},
		{
			name: "Part size too large",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("PUT", "/test-bucket/test-file.txt?partNumber=1&uploadId=test-123", nil)
				req.Header.Set("Content-Type", "application/octet-stream")
				req.ContentLength = 6 * 1024 * 1024 * 1024 // 6GB exceeds 5GB limit
				return req
			},
			expectedError: "part size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.setupRequest()
			err := s3Server.validateUploadPartRequest(req)

			if tt.expectedError == "" {
				assert.NoError(t, err, "Upload part validation should succeed")
			} else {
				assert.Error(t, err, "Upload part validation should fail")
				assert.Contains(t, err.Error(), tt.expectedError, "Error message should contain expected text")
			}
		})
	}
}

// TestDefaultMultipartUploadPolicy tests the default policy configuration
func TestDefaultMultipartUploadPolicy(t *testing.T) {
	policy := DefaultMultipartUploadPolicy()

	assert.Equal(t, int64(5*1024*1024*1024), policy.MaxPartSize, "Max part size should be 5GB")
	assert.Equal(t, int64(5*1024*1024), policy.MinPartSize, "Min part size should be 5MB")
	assert.Equal(t, 10000, policy.MaxParts, "Max parts should be 10,000")
	assert.Equal(t, 7*24*time.Hour, policy.MaxUploadDuration, "Max upload duration should be 7 days")
	assert.Empty(t, policy.AllowedContentTypes, "Should allow all content types by default")
	assert.Empty(t, policy.RequiredHeaders, "Should have no required headers by default")
	assert.Empty(t, policy.IPWhitelist, "Should have no IP restrictions by default")
}

// TestMultipartUploadSession tests multipart upload session structure
func TestMultipartUploadSession(t *testing.T) {
	session := &MultipartUploadSession{
		UploadID:  "test-upload-123",
		Bucket:    "test-bucket",
		ObjectKey: "test-file.txt",
		Initiator: "arn:aws:iam::user/testuser",
		Owner:     "arn:aws:iam::user/testuser",
		CreatedAt: time.Now(),
		Parts: []MultipartUploadPart{
			{
				PartNumber:   1,
				Size:         5 * 1024 * 1024,
				ETag:         "abc123",
				LastModified: time.Now(),
				Checksum:     "sha256:def456",
			},
		},
		Metadata: map[string]string{
			"Content-Type":      "application/octet-stream",
			"x-amz-meta-custom": "value",
		},
		Policy:       DefaultMultipartUploadPolicy(),
		SessionToken: "session-token-789",
	}

	assert.NotEmpty(t, session.UploadID, "Upload ID should not be empty")
	assert.NotEmpty(t, session.Bucket, "Bucket should not be empty")
	assert.NotEmpty(t, session.ObjectKey, "Object key should not be empty")
	assert.Len(t, session.Parts, 1, "Should have one part")
	assert.Equal(t, 1, session.Parts[0].PartNumber, "Part number should be 1")
	assert.NotNil(t, session.Policy, "Policy should not be nil")
}

// Helper functions for tests

func setupTestIAMManagerForMultipart(t *testing.T) *integration.IAMManager {
	// Create IAM manager
	manager := integration.NewIAMManager()

	// Initialize with test configuration
	config := &integration.IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{time.Hour},
			MaxSessionLength: sts.FlexibleDuration{time.Hour * 12},
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

	err := manager.Initialize(config, func() string {
		return "localhost:8888" // Mock filer address for testing
	})
	require.NoError(t, err)

	// Set up test identity providers
	setupTestProvidersForMultipart(t, manager)

	return manager
}

func setupTestProvidersForMultipart(t *testing.T, manager *integration.IAMManager) {
	// Set up OIDC provider
	oidcProvider := oidc.NewMockOIDCProvider("test-oidc")
	oidcConfig := &oidc.OIDCConfig{
		Issuer:   "https://test-issuer.com",
		ClientID: "test-client-id",
	}
	err := oidcProvider.Initialize(oidcConfig)
	require.NoError(t, err)
	oidcProvider.SetupDefaultTestData()

	// Set up LDAP provider
	ldapProvider := ldap.NewMockLDAPProvider("test-ldap")
	err = ldapProvider.Initialize(nil) // Mock doesn't need real config
	require.NoError(t, err)
	ldapProvider.SetupDefaultTestData()

	// Register providers
	err = manager.RegisterIdentityProvider(oidcProvider)
	require.NoError(t, err)
	err = manager.RegisterIdentityProvider(ldapProvider)
	require.NoError(t, err)
}

func setupTestRolesForMultipart(ctx context.Context, manager *integration.IAMManager) {
	// Create write policy for multipart operations
	writePolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "AllowS3MultipartOperations",
				Effect: "Allow",
				Action: []string{
					"s3:PutObject",
					"s3:GetObject",
					"s3:ListBucket",
					"s3:DeleteObject",
					"s3:CreateMultipartUpload",
					"s3:UploadPart",
					"s3:CompleteMultipartUpload",
					"s3:AbortMultipartUpload",
					"s3:ListMultipartUploads",
					"s3:ListParts",
				},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
			},
		},
	}

	manager.CreatePolicy(ctx, "", "S3WritePolicy", writePolicy)

	// Create write role
	manager.CreateRole(ctx, "", "S3WriteRole", &integration.RoleDefinition{
		RoleName: "S3WriteRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "test-oidc",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"S3WritePolicy"},
	})

	// Create a role for multipart users
	manager.CreateRole(ctx, "", "MultipartUser", &integration.RoleDefinition{
		RoleName: "MultipartUser",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "test-oidc",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"S3WritePolicy"},
	})
}

func createMultipartRequest(t *testing.T, method, path, sessionToken string) *http.Request {
	req := httptest.NewRequest(method, path, nil)

	// Add session token if provided
	if sessionToken != "" {
		req.Header.Set("Authorization", "Bearer "+sessionToken)
		// Set the principal ARN header that matches the assumed role from the test setup
		// This corresponds to the role "arn:aws:iam::role/S3WriteRole" with session name "multipart-test-session"
		req.Header.Set("X-SeaweedFS-Principal", "arn:aws:sts::assumed-role/S3WriteRole/multipart-test-session")
	}

	// Add common headers
	req.Header.Set("Content-Type", "application/octet-stream")

	return req
}
