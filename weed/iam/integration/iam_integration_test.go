package integration

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/iam/ldap"
	"github.com/seaweedfs/seaweedfs/weed/iam/oidc"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFullOIDCWorkflow tests the complete OIDC → STS → Policy workflow
func TestFullOIDCWorkflow(t *testing.T) {
	// Set up integrated IAM system
	iamManager := setupIntegratedIAMSystem(t)

	// Create JWT tokens for testing with the correct issuer
	validJWTToken := createTestJWT(t, "https://test-issuer.com", "test-user-123", "test-signing-key")
	invalidJWTToken := createTestJWT(t, "https://invalid-issuer.com", "test-user", "wrong-key")

	tests := []struct {
		name          string
		roleArn       string
		sessionName   string
		webToken      string
		expectedAllow bool
		testAction    string
		testResource  string
	}{
		{
			name:          "successful role assumption with policy validation",
			roleArn:       "arn:aws:iam::role/S3ReadOnlyRole",
			sessionName:   "oidc-session",
			webToken:      validJWTToken,
			expectedAllow: true,
			testAction:    "s3:GetObject",
			testResource:  "arn:aws:s3:::test-bucket/file.txt",
		},
		{
			name:          "role assumption denied by trust policy",
			roleArn:       "arn:aws:iam::role/RestrictedRole",
			sessionName:   "oidc-session",
			webToken:      validJWTToken,
			expectedAllow: false,
		},
		{
			name:          "invalid token rejected",
			roleArn:       "arn:aws:iam::role/S3ReadOnlyRole",
			sessionName:   "oidc-session",
			webToken:      invalidJWTToken,
			expectedAllow: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Step 1: Attempt role assumption
			assumeRequest := &sts.AssumeRoleWithWebIdentityRequest{
				RoleArn:          tt.roleArn,
				WebIdentityToken: tt.webToken,
				RoleSessionName:  tt.sessionName,
			}

			response, err := iamManager.AssumeRoleWithWebIdentity(ctx, assumeRequest)

			if !tt.expectedAllow {
				assert.Error(t, err)
				assert.Nil(t, response)
				return
			}

			// Should succeed if expectedAllow is true
			require.NoError(t, err)
			require.NotNil(t, response)
			require.NotNil(t, response.Credentials)

			// Step 2: Test policy enforcement with assumed credentials
			if tt.testAction != "" && tt.testResource != "" {
				allowed, err := iamManager.IsActionAllowed(ctx, &ActionRequest{
					Principal:    response.AssumedRoleUser.Arn,
					Action:       tt.testAction,
					Resource:     tt.testResource,
					SessionToken: response.Credentials.SessionToken,
				})

				require.NoError(t, err)
				assert.True(t, allowed, "Action should be allowed by role policy")
			}
		})
	}
}

// TestFullLDAPWorkflow tests the complete LDAP → STS → Policy workflow
func TestFullLDAPWorkflow(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)

	tests := []struct {
		name          string
		roleArn       string
		sessionName   string
		username      string
		password      string
		expectedAllow bool
		testAction    string
		testResource  string
	}{
		{
			name:          "successful LDAP role assumption",
			roleArn:       "arn:aws:iam::role/LDAPUserRole",
			sessionName:   "ldap-session",
			username:      "testuser",
			password:      "testpass",
			expectedAllow: true,
			testAction:    "filer:CreateEntry",
			testResource:  "arn:aws:filer::path/user-docs/*",
		},
		{
			name:          "invalid LDAP credentials",
			roleArn:       "arn:aws:iam::role/LDAPUserRole",
			sessionName:   "ldap-session",
			username:      "testuser",
			password:      "wrongpass",
			expectedAllow: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Step 1: Attempt role assumption with LDAP credentials
			assumeRequest := &sts.AssumeRoleWithCredentialsRequest{
				RoleArn:         tt.roleArn,
				Username:        tt.username,
				Password:        tt.password,
				RoleSessionName: tt.sessionName,
				ProviderName:    "test-ldap",
			}

			response, err := iamManager.AssumeRoleWithCredentials(ctx, assumeRequest)

			if !tt.expectedAllow {
				assert.Error(t, err)
				assert.Nil(t, response)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, response)

			// Step 2: Test policy enforcement
			if tt.testAction != "" && tt.testResource != "" {
				allowed, err := iamManager.IsActionAllowed(ctx, &ActionRequest{
					Principal:    response.AssumedRoleUser.Arn,
					Action:       tt.testAction,
					Resource:     tt.testResource,
					SessionToken: response.Credentials.SessionToken,
				})

				require.NoError(t, err)
				assert.True(t, allowed)
			}
		})
	}
}

// TestPolicyEnforcement tests policy evaluation for various scenarios
func TestPolicyEnforcement(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)

	// Create a valid JWT token for testing
	validJWTToken := createTestJWT(t, "https://test-issuer.com", "test-user-123", "test-signing-key")

	// Create a session for testing
	ctx := context.Background()
	assumeRequest := &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/S3ReadOnlyRole",
		WebIdentityToken: validJWTToken,
		RoleSessionName:  "policy-test-session",
	}

	response, err := iamManager.AssumeRoleWithWebIdentity(ctx, assumeRequest)
	require.NoError(t, err)

	sessionToken := response.Credentials.SessionToken
	principal := response.AssumedRoleUser.Arn

	tests := []struct {
		name        string
		action      string
		resource    string
		shouldAllow bool
		reason      string
	}{
		{
			name:        "allow read access",
			action:      "s3:GetObject",
			resource:    "arn:aws:s3:::test-bucket/file.txt",
			shouldAllow: true,
			reason:      "S3ReadOnlyRole should allow GetObject",
		},
		{
			name:        "allow list bucket",
			action:      "s3:ListBucket",
			resource:    "arn:aws:s3:::test-bucket",
			shouldAllow: true,
			reason:      "S3ReadOnlyRole should allow ListBucket",
		},
		{
			name:        "deny write access",
			action:      "s3:PutObject",
			resource:    "arn:aws:s3:::test-bucket/newfile.txt",
			shouldAllow: false,
			reason:      "S3ReadOnlyRole should deny write operations",
		},
		{
			name:        "deny delete access",
			action:      "s3:DeleteObject",
			resource:    "arn:aws:s3:::test-bucket/file.txt",
			shouldAllow: false,
			reason:      "S3ReadOnlyRole should deny delete operations",
		},
		{
			name:        "deny filer access",
			action:      "filer:CreateEntry",
			resource:    "arn:aws:filer::path/test",
			shouldAllow: false,
			reason:      "S3ReadOnlyRole should not allow filer operations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed, err := iamManager.IsActionAllowed(ctx, &ActionRequest{
				Principal:    principal,
				Action:       tt.action,
				Resource:     tt.resource,
				SessionToken: sessionToken,
			})

			require.NoError(t, err)
			assert.Equal(t, tt.shouldAllow, allowed, tt.reason)
		})
	}
}

// TestSessionExpiration tests session expiration and cleanup
func TestSessionExpiration(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	// Create a valid JWT token for testing
	validJWTToken := createTestJWT(t, "https://test-issuer.com", "test-user-123", "test-signing-key")

	// Create a short-lived session
	assumeRequest := &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/S3ReadOnlyRole",
		WebIdentityToken: validJWTToken,
		RoleSessionName:  "expiration-test",
		DurationSeconds:  int64Ptr(900), // 15 minutes
	}

	response, err := iamManager.AssumeRoleWithWebIdentity(ctx, assumeRequest)
	require.NoError(t, err)

	sessionToken := response.Credentials.SessionToken

	// Verify session is initially valid
	allowed, err := iamManager.IsActionAllowed(ctx, &ActionRequest{
		Principal:    response.AssumedRoleUser.Arn,
		Action:       "s3:GetObject",
		Resource:     "arn:aws:s3:::test-bucket/file.txt",
		SessionToken: sessionToken,
	})
	require.NoError(t, err)
	assert.True(t, allowed)

	// Verify the expiration time is set correctly
	assert.True(t, response.Credentials.Expiration.After(time.Now()))
	assert.True(t, response.Credentials.Expiration.Before(time.Now().Add(16*time.Minute)))

	// Test session expiration behavior in stateless JWT system
	// In a stateless system, manual expiration is not supported
	err = iamManager.ExpireSessionForTesting(ctx, sessionToken)
	require.Error(t, err, "Manual session expiration should not be supported in stateless system")
	assert.Contains(t, err.Error(), "manual session expiration not supported")

	// Verify session is still valid (since it hasn't naturally expired)
	allowed, err = iamManager.IsActionAllowed(ctx, &ActionRequest{
		Principal:    response.AssumedRoleUser.Arn,
		Action:       "s3:GetObject",
		Resource:     "arn:aws:s3:::test-bucket/file.txt",
		SessionToken: sessionToken,
	})
	require.NoError(t, err, "Session should still be valid in stateless system")
	assert.True(t, allowed, "Access should still be allowed since token hasn't naturally expired")
}

// TestTrustPolicyValidation tests role trust policy validation
func TestTrustPolicyValidation(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	tests := []struct {
		name        string
		roleArn     string
		provider    string
		userID      string
		shouldAllow bool
		reason      string
	}{
		{
			name:        "OIDC user allowed by trust policy",
			roleArn:     "arn:aws:iam::role/S3ReadOnlyRole",
			provider:    "oidc",
			userID:      "test-user-id",
			shouldAllow: true,
			reason:      "Trust policy should allow OIDC users",
		},
		{
			name:        "LDAP user allowed by different role",
			roleArn:     "arn:aws:iam::role/LDAPUserRole",
			provider:    "ldap",
			userID:      "testuser",
			shouldAllow: true,
			reason:      "Trust policy should allow LDAP users for LDAP role",
		},
		{
			name:        "Wrong provider for role",
			roleArn:     "arn:aws:iam::role/S3ReadOnlyRole",
			provider:    "ldap",
			userID:      "testuser",
			shouldAllow: false,
			reason:      "S3ReadOnlyRole trust policy should reject LDAP users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This would test trust policy evaluation
			// For now, we'll implement this as part of the IAM manager
			result := iamManager.ValidateTrustPolicy(ctx, tt.roleArn, tt.provider, tt.userID)
			assert.Equal(t, tt.shouldAllow, result, tt.reason)
		})
	}
}

// TestTrustPolicyWildcardPrincipal tests wildcard principal handling in trust policies
func TestTrustPolicyWildcardPrincipal(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	// Create a role with wildcard federated principal
	err := iamManager.CreateRole(ctx, "", "WildcardFederatedRole", &RoleDefinition{
		RoleName: "WildcardFederatedRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "*", // Wildcard should allow any federated provider
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})
	require.NoError(t, err)

	// Create a role with wildcard in array
	err = iamManager.CreateRole(ctx, "", "WildcardArrayRole", &RoleDefinition{
		RoleName: "WildcardArrayRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": []string{"specific-provider", "*"}, // Array with wildcard
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})
	require.NoError(t, err)

	// Create a role with plain wildcard principal (regression test)
	err = iamManager.CreateRole(ctx, "", "PlainWildcardRole", &RoleDefinition{
		RoleName: "PlainWildcardRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect:    "Allow",
					Principal: "*", // Plain wildcard
					Action:    []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})
	require.NoError(t, err)

	// Create JWT token for testing
	validJWTToken := createTestJWT(t, "https://test-issuer.com", "test-user-123", "test-signing-key")

	tests := []struct {
		name        string
		roleArn     string
		token       string
		shouldAllow bool
		reason      string
	}{
		{
			name:        "Wildcard federated principal allows any provider",
			roleArn:     "arn:aws:iam::role/WildcardFederatedRole",
			token:       validJWTToken,
			shouldAllow: true,
			reason:      "Wildcard federated principal should allow any provider",
		},
		{
			name:        "Wildcard in array allows any provider",
			roleArn:     "arn:aws:iam::role/WildcardArrayRole",
			token:       validJWTToken,
			shouldAllow: true,
			reason:      "Wildcard in principal array should allow any provider",
		},
		{
			name:        "Plain wildcard allows any provider (regression)",
			roleArn:     "arn:aws:iam::role/PlainWildcardRole",
			token:       validJWTToken,
			shouldAllow: true,
			reason:      "Plain wildcard principal should still work",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assumeRequest := &sts.AssumeRoleWithWebIdentityRequest{
				RoleArn:          tt.roleArn,
				WebIdentityToken: tt.token,
				RoleSessionName:  "wildcard-test-session",
			}

			response, err := iamManager.AssumeRoleWithWebIdentity(ctx, assumeRequest)

			if tt.shouldAllow {
				require.NoError(t, err, tt.reason)
				require.NotNil(t, response)
				require.NotNil(t, response.Credentials)
			} else {
				assert.Error(t, err, tt.reason)
				assert.Nil(t, response)
			}
		})
	}
}

// Helper functions and test setup

// createTestJWT creates a test JWT token with the specified issuer, subject and signing key
func createTestJWT(t *testing.T, issuer, subject, signingKey string) string {
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

func setupIntegratedIAMSystem(t *testing.T) *IAMManager {
	// Create IAM manager with all components
	manager := NewIAMManager()

	// Configure and initialize
	config := &IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: time.Hour * 12},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{
			DefaultEffect: "Deny",
			StoreType:     "memory", // Use memory for unit tests
		},
		Roles: &RoleStoreConfig{
			StoreType: "memory", // Use memory for unit tests
		},
	}

	err := manager.Initialize(config, func() string {
		return "localhost:8888" // Mock filer address for testing
	})
	require.NoError(t, err)

	// Set up test providers
	setupTestProviders(t, manager)

	// Set up test policies and roles
	setupTestPoliciesAndRoles(t, manager)

	return manager
}

func setupTestProviders(t *testing.T, manager *IAMManager) {
	// Set up OIDC provider
	oidcProvider := oidc.NewMockOIDCProvider("test-oidc")
	oidcConfig := &oidc.OIDCConfig{
		Issuer:   "https://test-issuer.com",
		ClientID: "test-client-id",
	}
	err := oidcProvider.Initialize(oidcConfig)
	require.NoError(t, err)
	oidcProvider.SetupDefaultTestData()

	// Set up LDAP mock provider (no config needed for mock)
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

func setupTestPoliciesAndRoles(t *testing.T, manager *IAMManager) {
	ctx := context.Background()

	// Create S3 read-only policy
	s3ReadPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "S3ReadAccess",
				Effect: "Allow",
				Action: []string{"s3:GetObject", "s3:ListBucket"},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
			},
		},
	}

	err := manager.CreatePolicy(ctx, "", "S3ReadOnlyPolicy", s3ReadPolicy)
	require.NoError(t, err)

	// Create LDAP user policy
	ldapUserPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "FilerAccess",
				Effect: "Allow",
				Action: []string{"filer:*"},
				Resource: []string{
					"arn:aws:filer::path/user-docs/*",
				},
			},
		},
	}

	err = manager.CreatePolicy(ctx, "", "LDAPUserPolicy", ldapUserPolicy)
	require.NoError(t, err)

	// Create roles with trust policies
	err = manager.CreateRole(ctx, "", "S3ReadOnlyRole", &RoleDefinition{
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
	require.NoError(t, err)

	err = manager.CreateRole(ctx, "", "LDAPUserRole", &RoleDefinition{
		RoleName: "LDAPUserRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "test-ldap",
					},
					Action: []string{"sts:AssumeRoleWithCredentials"},
				},
			},
		},
		AttachedPolicies: []string{"LDAPUserPolicy"},
	})
	require.NoError(t, err)
}

func int64Ptr(v int64) *int64 {
	return &v
}
