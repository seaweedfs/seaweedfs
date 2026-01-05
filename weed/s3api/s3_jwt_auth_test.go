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

// createTestJWTAuth creates a test JWT token with the specified issuer, subject and signing key
func createTestJWTAuth(t *testing.T, issuer, subject, signingKey string) string {
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

// TestJWTAuthenticationFlow tests the JWT authentication flow without full S3 server
func TestJWTAuthenticationFlow(t *testing.T) {
	// Set up IAM system
	iamManager := setupTestIAMManager(t)

	// Create IAM integration
	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")

	// Create IAM server with integration
	iamServer := setupIAMWithIntegration(t, iamManager, s3iam)

	// Test scenarios
	tests := []struct {
		name           string
		roleArn        string
		setupRole      func(ctx context.Context, mgr *integration.IAMManager)
		testOperations []JWTTestOperation
	}{
		{
			name:      "Read-Only JWT Authentication",
			roleArn:   "arn:aws:iam::role/S3ReadOnlyRole",
			setupRole: setupTestReadOnlyRole,
			testOperations: []JWTTestOperation{
				{Action: s3_constants.ACTION_READ, Bucket: "test-bucket", Object: "test-file.txt", ExpectedAllow: true},
				{Action: s3_constants.ACTION_WRITE, Bucket: "test-bucket", Object: "new-file.txt", ExpectedAllow: false},
				{Action: s3_constants.ACTION_LIST, Bucket: "test-bucket", Object: "", ExpectedAllow: true},
			},
		},
		{
			name:      "Admin JWT Authentication",
			roleArn:   "arn:aws:iam::role/S3AdminRole",
			setupRole: setupTestAdminRole,
			testOperations: []JWTTestOperation{
				{Action: s3_constants.ACTION_READ, Bucket: "admin-bucket", Object: "admin-file.txt", ExpectedAllow: true},
				{Action: s3_constants.ACTION_WRITE, Bucket: "admin-bucket", Object: "new-admin-file.txt", ExpectedAllow: true},
				{Action: s3_constants.ACTION_DELETE_BUCKET, Bucket: "admin-bucket", Object: "", ExpectedAllow: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Set up role
			tt.setupRole(ctx, iamManager)

			// Create a valid JWT token for testing
			validJWTToken := createTestJWTAuth(t, "https://test-issuer.com", "test-user-123", "test-signing-key")

			// Assume role to get JWT
			response, err := iamManager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
				RoleArn:          tt.roleArn,
				WebIdentityToken: validJWTToken,
				RoleSessionName:  "jwt-auth-test",
			})
			require.NoError(t, err)

			jwtToken := response.Credentials.SessionToken

			// Test each operation
			for _, op := range tt.testOperations {
				t.Run(string(op.Action), func(t *testing.T) {
					// Test JWT authentication
					identity, errCode := testJWTAuthentication(t, iamServer, jwtToken)
					require.Equal(t, s3err.ErrNone, errCode, "JWT authentication should succeed")
					require.NotNil(t, identity)

					// Test authorization with appropriate role based on test case
					var testRoleName string
					if tt.name == "Read-Only JWT Authentication" {
						testRoleName = "TestReadRole"
					} else {
						testRoleName = "TestAdminRole"
					}
					allowed := testJWTAuthorizationWithRole(t, iamServer, identity, op.Action, op.Bucket, op.Object, jwtToken, testRoleName)
					assert.Equal(t, op.ExpectedAllow, allowed, "Operation %s should have expected result", op.Action)
				})
			}
		})
	}
}

// TestJWTTokenValidation tests JWT token validation edge cases
func TestJWTTokenValidation(t *testing.T) {
	iamManager := setupTestIAMManager(t)
	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")
	iamServer := setupIAMWithIntegration(t, iamManager, s3iam)

	tests := []struct {
		name        string
		token       string
		expectedErr s3err.ErrorCode
	}{
		{
			name:        "Empty token",
			token:       "",
			expectedErr: s3err.ErrAccessDenied,
		},
		{
			name:        "Invalid token format",
			token:       "invalid-token",
			expectedErr: s3err.ErrAccessDenied,
		},
		{
			name:        "Expired token",
			token:       "expired-session-token",
			expectedErr: s3err.ErrAccessDenied,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity, errCode := testJWTAuthentication(t, iamServer, tt.token)

			assert.Equal(t, tt.expectedErr, errCode)
			assert.Nil(t, identity)
		})
	}
}

// TestRequestContextExtraction tests context extraction for policy conditions
func TestRequestContextExtraction(t *testing.T) {
	tests := []struct {
		name         string
		setupRequest func() *http.Request
		expectedIP   string
		expectedUA   string
	}{
		{
			name: "Standard request with IP",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "/test-bucket/test-file.txt", http.NoBody)
				req.Header.Set("X-Forwarded-For", "192.168.1.100")
				req.Header.Set("User-Agent", "aws-sdk-go/1.0")
				return req
			},
			expectedIP: "192.168.1.100",
			expectedUA: "aws-sdk-go/1.0",
		},
		{
			name: "Request with X-Real-IP",
			setupRequest: func() *http.Request {
				req := httptest.NewRequest("GET", "/test-bucket/test-file.txt", http.NoBody)
				req.Header.Set("X-Real-IP", "10.0.0.1")
				req.Header.Set("User-Agent", "boto3/1.0")
				return req
			},
			expectedIP: "10.0.0.1",
			expectedUA: "boto3/1.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tt.setupRequest()

			// Extract request context
			context := extractRequestContext(req)

			if tt.expectedIP != "" {
				assert.Equal(t, tt.expectedIP, context["sourceIP"])
			}

			if tt.expectedUA != "" {
				assert.Equal(t, tt.expectedUA, context["userAgent"])
			}
		})
	}
}

// TestIPBasedPolicyEnforcement tests IP-based conditional policies
func TestIPBasedPolicyEnforcement(t *testing.T) {
	iamManager := setupTestIAMManager(t)
	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")
	ctx := context.Background()

	// Set up IP-restricted role
	setupTestIPRestrictedRole(ctx, iamManager)

	// Create a valid JWT token for testing
	validJWTToken := createTestJWTAuth(t, "https://test-issuer.com", "test-user-123", "test-signing-key")

	// Assume role
	response, err := iamManager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/S3IPRestrictedRole",
		WebIdentityToken: validJWTToken,
		RoleSessionName:  "ip-test-session",
	})
	require.NoError(t, err)

	tests := []struct {
		name        string
		sourceIP    string
		shouldAllow bool
	}{
		{
			name:        "Allow from office IP",
			sourceIP:    "192.168.1.100",
			shouldAllow: true,
		},
		{
			name:        "Block from external IP",
			sourceIP:    "8.8.8.8",
			shouldAllow: false,
		},
		{
			name:        "Allow from internal range",
			sourceIP:    "10.0.0.1",
			shouldAllow: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request with specific IP
			req := httptest.NewRequest("GET", "/restricted-bucket/file.txt", http.NoBody)
			req.Header.Set("Authorization", "Bearer "+response.Credentials.SessionToken)
			req.Header.Set("X-Forwarded-For", tt.sourceIP)

			// Create IAM identity for testing
			identity := &IAMIdentity{
				Name:         "test-user",
				Principal:    response.AssumedRoleUser.Arn,
				SessionToken: response.Credentials.SessionToken,
			}

			// Test authorization with IP condition
			errCode := s3iam.AuthorizeAction(ctx, identity, s3_constants.ACTION_READ, "restricted-bucket", "file.txt", req)

			if tt.shouldAllow {
				assert.Equal(t, s3err.ErrNone, errCode, "Should allow access from IP %s", tt.sourceIP)
			} else {
				assert.Equal(t, s3err.ErrAccessDenied, errCode, "Should deny access from IP %s", tt.sourceIP)
			}
		})
	}
}

// JWTTestOperation represents a test operation for JWT testing
type JWTTestOperation struct {
	Action        Action
	Bucket        string
	Object        string
	ExpectedAllow bool
}

// Helper functions

func setupTestIAMManager(t *testing.T) *integration.IAMManager {
	// Create IAM manager
	manager := integration.NewIAMManager()

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

	err := manager.Initialize(config, func() string {
		return "localhost:8888" // Mock filer address for testing
	})
	require.NoError(t, err)

	// Set up test identity providers
	setupTestIdentityProviders(t, manager)

	return manager
}

func setupTestIdentityProviders(t *testing.T, manager *integration.IAMManager) {
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

func setupIAMWithIntegration(t *testing.T, iamManager *integration.IAMManager, s3iam *S3IAMIntegration) *IdentityAccessManagement {
	// Create a minimal IdentityAccessManagement for testing
	iam := &IdentityAccessManagement{
		isAuthEnabled: true,
	}

	// Set IAM integration
	iam.SetIAMIntegration(s3iam)

	return iam
}

func setupTestReadOnlyRole(ctx context.Context, manager *integration.IAMManager) {
	// Create read-only policy
	readPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "AllowS3Read",
				Effect: "Allow",
				Action: []string{"s3:GetObject", "s3:ListBucket"},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
			},
			{
				Sid:      "AllowSTSSessionValidation",
				Effect:   "Allow",
				Action:   []string{"sts:ValidateSession"},
				Resource: []string{"*"},
			},
		},
	}

	manager.CreatePolicy(ctx, "", "S3ReadOnlyPolicy", readPolicy)

	// Create role
	manager.CreateRole(ctx, "", "S3ReadOnlyRole", &integration.RoleDefinition{
		RoleName: "S3ReadOnlyRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "https://test-issuer.com",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})

	// Also create a TestReadRole for read-only authorization testing
	manager.CreateRole(ctx, "", "TestReadRole", &integration.RoleDefinition{
		RoleName: "TestReadRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "https://test-issuer.com",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})
}

func setupTestAdminRole(ctx context.Context, manager *integration.IAMManager) {
	// Create admin policy
	adminPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "AllowAllS3",
				Effect: "Allow",
				Action: []string{"s3:*"},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
			},
			{
				Sid:      "AllowSTSSessionValidation",
				Effect:   "Allow",
				Action:   []string{"sts:ValidateSession"},
				Resource: []string{"*"},
			},
		},
	}

	manager.CreatePolicy(ctx, "", "S3AdminPolicy", adminPolicy)

	// Create role
	manager.CreateRole(ctx, "", "S3AdminRole", &integration.RoleDefinition{
		RoleName: "S3AdminRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "https://test-issuer.com",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"S3AdminPolicy"},
	})

	// Also create a TestAdminRole with admin policy for authorization testing
	manager.CreateRole(ctx, "", "TestAdminRole", &integration.RoleDefinition{
		RoleName: "TestAdminRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "https://test-issuer.com",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"S3AdminPolicy"}, // Admin gets full access
	})
}

func setupTestIPRestrictedRole(ctx context.Context, manager *integration.IAMManager) {
	// Create IP-restricted policy
	restrictedPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "AllowFromOffice",
				Effect: "Allow",
				Action: []string{"s3:GetObject", "s3:ListBucket"},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
				Condition: map[string]map[string]interface{}{
					"IpAddress": {
						"aws:SourceIp": []string{"192.168.1.0/24", "10.0.0.0/8"},
					},
				},
			},
		},
	}

	manager.CreatePolicy(ctx, "", "S3IPRestrictedPolicy", restrictedPolicy)

	// Create role
	manager.CreateRole(ctx, "", "S3IPRestrictedRole", &integration.RoleDefinition{
		RoleName: "S3IPRestrictedRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "https://test-issuer.com",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"S3IPRestrictedPolicy"},
	})
}

func testJWTAuthentication(t *testing.T, iam *IdentityAccessManagement, token string) (*Identity, s3err.ErrorCode) {
	// Create test request with JWT
	req := httptest.NewRequest("GET", "/test-bucket/test-object", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)

	// Test authentication
	if iam.iamIntegration == nil {
		return nil, s3err.ErrNotImplemented
	}

	return iam.authenticateJWTWithIAM(req)
}

func testJWTAuthorization(t *testing.T, iam *IdentityAccessManagement, identity *Identity, action Action, bucket, object, token string) bool {
	return testJWTAuthorizationWithRole(t, iam, identity, action, bucket, object, token, "TestRole")
}

func testJWTAuthorizationWithRole(t *testing.T, iam *IdentityAccessManagement, identity *Identity, action Action, bucket, object, token, roleName string) bool {
	// Create test request
	req := httptest.NewRequest("GET", "/"+bucket+"/"+object, http.NoBody)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-SeaweedFS-Session-Token", token)

	// Use a proper principal ARN format that matches what STS would generate
	principalArn := "arn:aws:sts::assumed-role/" + roleName + "/test-session"
	req.Header.Set("X-SeaweedFS-Principal", principalArn)

	// Test authorization
	if iam.iamIntegration == nil {
		return false
	}

	errCode := iam.authorizeWithIAM(req, identity, action, bucket, object)
	return errCode == s3err.ErrNone
}
