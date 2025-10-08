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

// createTestJWTPresigned creates a test JWT token with the specified issuer, subject and signing key
func createTestJWTPresigned(t *testing.T, issuer, subject, signingKey string) string {
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

// TestPresignedURLIAMValidation tests IAM validation for presigned URLs
func TestPresignedURLIAMValidation(t *testing.T) {
	// Set up IAM system
	iamManager := setupTestIAMManagerForPresigned(t)
	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")

	// Create IAM with integration
	iam := &IdentityAccessManagement{
		isAuthEnabled: true,
	}
	iam.SetIAMIntegration(s3iam)

	// Set up roles
	ctx := context.Background()
	setupTestRolesForPresigned(ctx, iamManager)

	// Create a valid JWT token for testing
	validJWTToken := createTestJWTPresigned(t, "https://test-issuer.com", "test-user-123", "test-signing-key")

	// Get session token
	response, err := iamManager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:seaweed:iam::role/S3ReadOnlyRole",
		WebIdentityToken: validJWTToken,
		RoleSessionName:  "presigned-test-session",
	})
	require.NoError(t, err)

	sessionToken := response.Credentials.SessionToken

	tests := []struct {
		name           string
		method         string
		path           string
		sessionToken   string
		expectedResult s3err.ErrorCode
	}{
		{
			name:           "GET object with read permissions",
			method:         "GET",
			path:           "/test-bucket/test-file.txt",
			sessionToken:   sessionToken,
			expectedResult: s3err.ErrNone,
		},
		{
			name:           "PUT object with read-only permissions (should fail)",
			method:         "PUT",
			path:           "/test-bucket/new-file.txt",
			sessionToken:   sessionToken,
			expectedResult: s3err.ErrAccessDenied,
		},
		{
			name:           "GET object without session token",
			method:         "GET",
			path:           "/test-bucket/test-file.txt",
			sessionToken:   "",
			expectedResult: s3err.ErrNone, // Falls back to standard auth
		},
		{
			name:           "Invalid session token",
			method:         "GET",
			path:           "/test-bucket/test-file.txt",
			sessionToken:   "invalid-token",
			expectedResult: s3err.ErrAccessDenied,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request with presigned URL parameters
			req := createPresignedURLRequest(t, tt.method, tt.path, tt.sessionToken)

			// Create identity for testing
			identity := &Identity{
				Name:    "test-user",
				Account: &AccountAdmin,
			}

			// Test validation
			result := iam.ValidatePresignedURLWithIAM(req, identity)
			assert.Equal(t, tt.expectedResult, result, "IAM validation result should match expected")
		})
	}
}

// TestPresignedURLGeneration tests IAM-aware presigned URL generation
func TestPresignedURLGeneration(t *testing.T) {
	// Set up IAM system
	iamManager := setupTestIAMManagerForPresigned(t)
	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")
	s3iam.enabled = true // Enable IAM integration
	presignedManager := NewS3PresignedURLManager(s3iam)

	ctx := context.Background()
	setupTestRolesForPresigned(ctx, iamManager)

	// Create a valid JWT token for testing
	validJWTToken := createTestJWTPresigned(t, "https://test-issuer.com", "test-user-123", "test-signing-key")

	// Get session token
	response, err := iamManager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:seaweed:iam::role/S3AdminRole",
		WebIdentityToken: validJWTToken,
		RoleSessionName:  "presigned-gen-test-session",
	})
	require.NoError(t, err)

	sessionToken := response.Credentials.SessionToken

	tests := []struct {
		name          string
		request       *PresignedURLRequest
		shouldSucceed bool
		expectedError string
	}{
		{
			name: "Generate valid presigned GET URL",
			request: &PresignedURLRequest{
				Method:       "GET",
				Bucket:       "test-bucket",
				ObjectKey:    "test-file.txt",
				Expiration:   time.Hour,
				SessionToken: sessionToken,
			},
			shouldSucceed: true,
		},
		{
			name: "Generate valid presigned PUT URL",
			request: &PresignedURLRequest{
				Method:       "PUT",
				Bucket:       "test-bucket",
				ObjectKey:    "new-file.txt",
				Expiration:   time.Hour,
				SessionToken: sessionToken,
			},
			shouldSucceed: true,
		},
		{
			name: "Generate URL with invalid session token",
			request: &PresignedURLRequest{
				Method:       "GET",
				Bucket:       "test-bucket",
				ObjectKey:    "test-file.txt",
				Expiration:   time.Hour,
				SessionToken: "invalid-token",
			},
			shouldSucceed: false,
			expectedError: "IAM authorization failed",
		},
		{
			name: "Generate URL without session token",
			request: &PresignedURLRequest{
				Method:     "GET",
				Bucket:     "test-bucket",
				ObjectKey:  "test-file.txt",
				Expiration: time.Hour,
			},
			shouldSucceed: false,
			expectedError: "IAM authorization failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := presignedManager.GeneratePresignedURLWithIAM(ctx, tt.request, "http://localhost:8333")

			if tt.shouldSucceed {
				assert.NoError(t, err, "Presigned URL generation should succeed")
				if response != nil {
					assert.NotEmpty(t, response.URL, "URL should not be empty")
					assert.Equal(t, tt.request.Method, response.Method, "Method should match")
					assert.True(t, response.ExpiresAt.After(time.Now()), "URL should not be expired")
				} else {
					t.Errorf("Response should not be nil when generation should succeed")
				}
			} else {
				assert.Error(t, err, "Presigned URL generation should fail")
				if tt.expectedError != "" {
					assert.Contains(t, err.Error(), tt.expectedError, "Error message should contain expected text")
				}
			}
		})
	}
}

// TestPresignedURLExpiration tests URL expiration validation
func TestPresignedURLExpiration(t *testing.T) {
	tests := []struct {
		name          string
		setupRequest  func() *http.Request
		expectedError string
	}{
		{
			name: "Valid non-expired URL",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "/test-bucket/test-file.txt", nil)
				q := req.URL.Query()
				// Set date to 30 minutes ago with 2 hours expiration for safe margin
				q.Set("X-Amz-Date", time.Now().UTC().Add(-30*time.Minute).Format("20060102T150405Z"))
				q.Set("X-Amz-Expires", "7200") // 2 hours
				req.URL.RawQuery = q.Encode()
				return req
			},
			expectedError: "",
		},
		{
			name: "Expired URL",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "/test-bucket/test-file.txt", nil)
				q := req.URL.Query()
				// Set date to 2 hours ago with 1 hour expiration
				q.Set("X-Amz-Date", time.Now().UTC().Add(-2*time.Hour).Format("20060102T150405Z"))
				q.Set("X-Amz-Expires", "3600") // 1 hour
				req.URL.RawQuery = q.Encode()
				return req
			},
			expectedError: "presigned URL has expired",
		},
		{
			name: "Missing date parameter",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "/test-bucket/test-file.txt", nil)
				q := req.URL.Query()
				q.Set("X-Amz-Expires", "3600")
				req.URL.RawQuery = q.Encode()
				return req
			},
			expectedError: "missing required presigned URL parameters",
		},
		{
			name: "Invalid date format",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "/test-bucket/test-file.txt", nil)
				q := req.URL.Query()
				q.Set("X-Amz-Date", "invalid-date")
				q.Set("X-Amz-Expires", "3600")
				req.URL.RawQuery = q.Encode()
				return req
			},
			expectedError: "invalid X-Amz-Date format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.setupRequest()
			err := ValidatePresignedURLExpiration(req)

			if tt.expectedError == "" {
				assert.NoError(t, err, "Validation should succeed")
			} else {
				assert.Error(t, err, "Validation should fail")
				assert.Contains(t, err.Error(), tt.expectedError, "Error message should contain expected text")
			}
		})
	}
}

// TestPresignedURLSecurityPolicy tests security policy enforcement
func TestPresignedURLSecurityPolicy(t *testing.T) {
	policy := &PresignedURLSecurityPolicy{
		MaxExpirationDuration: 24 * time.Hour,
		AllowedMethods:        []string{"GET", "PUT"},
		RequiredHeaders:       []string{"Content-Type"},
		MaxFileSize:           1024 * 1024, // 1MB
	}

	tests := []struct {
		name          string
		request       *PresignedURLRequest
		expectedError string
	}{
		{
			name: "Valid request",
			request: &PresignedURLRequest{
				Method:     "GET",
				Bucket:     "test-bucket",
				ObjectKey:  "test-file.txt",
				Expiration: 12 * time.Hour,
				Headers:    map[string]string{"Content-Type": "application/json"},
			},
			expectedError: "",
		},
		{
			name: "Expiration too long",
			request: &PresignedURLRequest{
				Method:     "GET",
				Bucket:     "test-bucket",
				ObjectKey:  "test-file.txt",
				Expiration: 48 * time.Hour, // Exceeds 24h limit
				Headers:    map[string]string{"Content-Type": "application/json"},
			},
			expectedError: "expiration duration",
		},
		{
			name: "Method not allowed",
			request: &PresignedURLRequest{
				Method:     "DELETE", // Not in allowed methods
				Bucket:     "test-bucket",
				ObjectKey:  "test-file.txt",
				Expiration: 12 * time.Hour,
				Headers:    map[string]string{"Content-Type": "application/json"},
			},
			expectedError: "HTTP method DELETE is not allowed",
		},
		{
			name: "Missing required header",
			request: &PresignedURLRequest{
				Method:     "GET",
				Bucket:     "test-bucket",
				ObjectKey:  "test-file.txt",
				Expiration: 12 * time.Hour,
				Headers:    map[string]string{}, // Missing Content-Type
			},
			expectedError: "required header Content-Type is missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := policy.ValidatePresignedURLRequest(tt.request)

			if tt.expectedError == "" {
				assert.NoError(t, err, "Policy validation should succeed")
			} else {
				assert.Error(t, err, "Policy validation should fail")
				assert.Contains(t, err.Error(), tt.expectedError, "Error message should contain expected text")
			}
		})
	}
}

// TestS3ActionDetermination tests action determination from HTTP methods
func TestS3ActionDetermination(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		bucket         string
		object         string
		expectedAction Action
	}{
		{
			name:           "GET object",
			method:         "GET",
			bucket:         "test-bucket",
			object:         "test-file.txt",
			expectedAction: s3_constants.ACTION_READ,
		},
		{
			name:           "GET bucket (list)",
			method:         "GET",
			bucket:         "test-bucket",
			object:         "",
			expectedAction: s3_constants.ACTION_LIST,
		},
		{
			name:           "PUT object",
			method:         "PUT",
			bucket:         "test-bucket",
			object:         "new-file.txt",
			expectedAction: s3_constants.ACTION_WRITE,
		},
		{
			name:           "DELETE object",
			method:         "DELETE",
			bucket:         "test-bucket",
			object:         "old-file.txt",
			expectedAction: s3_constants.ACTION_WRITE,
		},
		{
			name:           "DELETE bucket",
			method:         "DELETE",
			bucket:         "test-bucket",
			object:         "",
			expectedAction: s3_constants.ACTION_DELETE_BUCKET,
		},
		{
			name:           "HEAD object",
			method:         "HEAD",
			bucket:         "test-bucket",
			object:         "test-file.txt",
			expectedAction: s3_constants.ACTION_READ,
		},
		{
			name:           "POST object",
			method:         "POST",
			bucket:         "test-bucket",
			object:         "upload-file.txt",
			expectedAction: s3_constants.ACTION_WRITE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := determineS3ActionFromMethodAndPath(tt.method, tt.bucket, tt.object)
			assert.Equal(t, tt.expectedAction, action, "S3 action should match expected")
		})
	}
}

// Helper functions for tests

func setupTestIAMManagerForPresigned(t *testing.T) *integration.IAMManager {
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
	setupTestProvidersForPresigned(t, manager)

	return manager
}

func setupTestProvidersForPresigned(t *testing.T, manager *integration.IAMManager) {
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

func setupTestRolesForPresigned(ctx context.Context, manager *integration.IAMManager) {
	// Create read-only policy
	readOnlyPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "AllowS3ReadOperations",
				Effect: "Allow",
				Action: []string{"s3:GetObject", "s3:ListBucket", "s3:HeadObject"},
				Resource: []string{
					"arn:seaweed:s3:::*",
					"arn:seaweed:s3:::*/*",
				},
			},
		},
	}

	manager.CreatePolicy(ctx, "", "S3ReadOnlyPolicy", readOnlyPolicy)

	// Create read-only role
	manager.CreateRole(ctx, "", "S3ReadOnlyRole", &integration.RoleDefinition{
		RoleName: "S3ReadOnlyRole",
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
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})

	// Create admin policy
	adminPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "AllowAllS3Operations",
				Effect: "Allow",
				Action: []string{"s3:*"},
				Resource: []string{
					"arn:seaweed:s3:::*",
					"arn:seaweed:s3:::*/*",
				},
			},
		},
	}

	manager.CreatePolicy(ctx, "", "S3AdminPolicy", adminPolicy)

	// Create admin role
	manager.CreateRole(ctx, "", "S3AdminRole", &integration.RoleDefinition{
		RoleName: "S3AdminRole",
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
		AttachedPolicies: []string{"S3AdminPolicy"},
	})

	// Create a role for presigned URL users with admin permissions for testing
	manager.CreateRole(ctx, "", "PresignedUser", &integration.RoleDefinition{
		RoleName: "PresignedUser",
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
		AttachedPolicies: []string{"S3AdminPolicy"}, // Use admin policy for testing
	})
}

func createPresignedURLRequest(t *testing.T, method, path, sessionToken string) *http.Request {
	req := httptest.NewRequest(method, path, nil)

	// Add presigned URL parameters if session token is provided
	if sessionToken != "" {
		q := req.URL.Query()
		q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
		q.Set("X-Amz-Security-Token", sessionToken)
		q.Set("X-Amz-Date", time.Now().Format("20060102T150405Z"))
		q.Set("X-Amz-Expires", "3600")
		req.URL.RawQuery = q.Encode()
	}

	return req
}
