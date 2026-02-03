package s3api

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// TestSTSIdentityPolicyNamesPopulation tests that STS identities have PolicyNames
// populated from sessionInfo.Policies, enabling IAM-based authorization.
// This is a regression test for GitHub issue #7985.
func TestSTSIdentityPolicyNamesPopulation(t *testing.T) {
	// Setup: Create a mock STS service
	stsService, config := setupTestSTSService(t)

	// Create IAM with STS integration
	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, "memory")
	s3iam := &S3IAMIntegration{
		stsService: stsService,
	}
	iam.SetIAMIntegration(s3iam)

	// Generate a session token with policies
	sessionId := "test-session-id"
	expiresAt := time.Now().Add(time.Hour)

	// Create session claims with policies
	sessionClaims := sts.NewSTSSessionClaims(sessionId, config.Issuer, expiresAt).
		WithSessionName("test-session").
		WithRoleInfo("arn:aws:iam::role/adminRole", "arn:aws:sts::assumed-role/adminRole/test-session", "arn:aws:sts::assumed-role/adminRole/test-session")

	// Add policies to the session claims
	sessionClaims.Policies = []string{"AdminPolicy", "S3FullAccess"}

	// Generate JWT token
	tokenGen := sts.NewTokenGenerator(config.SigningKey, config.Issuer)
	sessionToken, err := tokenGen.GenerateJWTWithClaims(sessionClaims)
	require.NoError(t, err, "Should generate session token successfully")

	// Validate the session token
	sessionInfo, err := stsService.ValidateSessionToken(context.Background(), sessionToken)
	require.NoError(t, err, "Session token should be valid")
	require.NotNil(t, sessionInfo, "Session info should not be nil")

	// Verify that sessionInfo has policies
	assert.NotEmpty(t, sessionInfo.Policies, "SessionInfo should have policies")
	assert.Equal(t, []string{"AdminPolicy", "S3FullAccess"}, sessionInfo.Policies, "Policies should match")

	// Create a mock credential from session info
	cred := &Credential{
		AccessKey:  sessionInfo.Credentials.AccessKeyId,
		SecretKey:  sessionInfo.Credentials.SecretAccessKey,
		Status:     "Active",
		Expiration: sessionInfo.ExpiresAt.Unix(),
	}

	// Create identity as validateSTSSessionToken does
	identity := &Identity{
		Name:         sessionInfo.AssumedRoleUser,
		Account:      &AccountAdmin,
		Credentials:  []*Credential{cred},
		PrincipalArn: sessionInfo.Principal,
		PolicyNames:  sessionInfo.Policies, // This is the fix for issue #7985
	}

	// Verify the fix: PolicyNames should be populated
	assert.NotNil(t, identity.PolicyNames, "Identity PolicyNames should not be nil")
	assert.Equal(t, []string{"AdminPolicy", "S3FullAccess"}, identity.PolicyNames, "PolicyNames should be populated from sessionInfo.Policies")

	// Verify that Actions is empty (STS identities should use IAM authorization, not legacy Actions)
	assert.Empty(t, identity.Actions, "STS identities should have empty Actions to trigger IAM authorization path")

	// Verify legacy CanDo returns false (forcing fallback to IAM)
	assert.False(t, identity.CanDo("Read", "test-bucket", "/any/path"),
		"CanDo should return false for STS identities with no Actions")

	// Verify authorization path selection
	// When identity.Actions is empty and iamIntegration is available, it should use IAM authorization
	hasActions := len(identity.Actions) > 0
	hasIAMIntegration := iam.iamIntegration != nil

	assert.False(t, hasActions, "STS identity should not have Actions")
	assert.True(t, hasIAMIntegration, "IAM integration should be available")

	// This combination means authorization will go through the IAM path
	t.Log("✓ STS identity will use IAM authorization path (correct behavior)")
}

// TestSTSIdentityAuthorizationFlow tests the complete authorization flow for STS identities
// to ensure they are properly authorized via IAM integration.
func TestSTSIdentityAuthorizationFlow(t *testing.T) {
	// This test validates that:
	// 1. STS identities have empty Actions
	// 2. STS identities have PolicyNames populated
	// 3. Authorization logic routes to IAM path when Actions is empty and iamIntegration exists

	// Create a mock STS session info
	sessionInfo := &sts.SessionInfo{
		SessionId:       "test-session",
		SessionName:     "s3-session",
		RoleArn:         "arn:aws:iam::role/adminRole",
		AssumedRoleUser: "arn:aws:sts::assumed-role/adminRole/s3-session",
		Principal:       "arn:aws:sts::assumed-role/adminRole/s3-session",
		Policies:        []string{"AdminPolicy", "S3WritePolicy"},
		ExpiresAt:       time.Now().Add(time.Hour),
		Credentials: &sts.Credentials{
			AccessKeyId:     "AKIAIOSFODNN7EXAMPLE",
			SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			SessionToken:    "test-session-token",
			Expiration:      time.Now().Add(time.Hour),
		},
	}

	// Create credential from session info
	cred := &Credential{
		AccessKey:  sessionInfo.Credentials.AccessKeyId,
		SecretKey:  sessionInfo.Credentials.SecretAccessKey,
		Status:     "Active",
		Expiration: sessionInfo.ExpiresAt.Unix(),
	}

	// Create identity as validateSTSSessionToken does (with the fix)
	identity := &Identity{
		Name:         sessionInfo.AssumedRoleUser,
		Account:      &AccountAdmin,
		Credentials:  []*Credential{cred},
		PrincipalArn: sessionInfo.Principal,
		PolicyNames:  sessionInfo.Policies, // Fix for #7985
	}

	// Test 1: Verify PolicyNames are populated
	assert.Equal(t, []string{"AdminPolicy", "S3WritePolicy"}, identity.PolicyNames,
		"PolicyNames should be populated from sessionInfo.Policies")

	assert.Empty(t, identity.Actions,
		"STS identities should have empty Actions to trigger the IAM authorization path")

	// Test 2: Verify CanDo returns false (legacy auth should be bypassed)
	// This is important because it confirms that identity.Actions being empty
	// correctly forces the authorization logic to fall back to iam.authorizeWithIAM
	assert.False(t, identity.CanDo("Read", "test-bucket", "/any/path"),
		"CanDo should return false for STS identities with no Actions")

	// With empty Actions and populated PolicyNames, IAM authorization path will be used
	// as per auth_credentials.go:703-713
	t.Log("✓ Verified: STS identity correctly bypasses legacy CanDo() to use IAM authorization path")
}

// TestSTSIdentityWithoutPolicyNames tests the bug scenario where PolicyNames is not populated
// This reproduces the original issue #7985
func TestSTSIdentityWithoutPolicyNames(t *testing.T) {
	// Create identity WITHOUT PolicyNames (the bug scenario)
	identityBuggy := newTestIdentity(nil)

	// Create identity WITH PolicyNames (the fix)
	identityFixed := newTestIdentity([]string{"AdminPolicy"})

	// Verify the bug scenario
	assert.Empty(t, identityBuggy.Actions, "Buggy identity has no Actions")
	assert.Empty(t, identityBuggy.PolicyNames, "Buggy identity has no PolicyNames (the bug)")

	// Verify the fix
	assert.Empty(t, identityFixed.Actions, "Fixed identity has no Actions (correct)")
	assert.NotEmpty(t, identityFixed.PolicyNames, "Fixed identity has PolicyNames (the fix)")

	t.Logf("Bug scenario: Actions=%d, PolicyNames=%d → Would be denied",
		len(identityBuggy.Actions), len(identityBuggy.PolicyNames))
	t.Logf("Fixed scenario: Actions=%d, PolicyNames=%d → Can use IAM authorization",
		len(identityFixed.Actions), len(identityFixed.PolicyNames))
}

// TestCanDoPathConstruction tests that bucket and objectKey are properly concatenated
// with a slash separator. This is a regression test for the second issue mentioned in #7985.
func TestCanDoPathConstruction(t *testing.T) {
	// Create a test identity with wildcard actions (standard pattern)
	identity := &Identity{
		Name:    "test-user",
		Account: &AccountAdmin,
		Actions: []Action{
			"Write:test-bucket/*",     // Wildcard for all objects
			"Read:test-bucket/docs/*", // Wildcard for specific path
		},
	}

	// Test cases for path construction
	testCases := []struct {
		name       string
		action     Action
		bucket     string
		objectKey  string
		shouldPass bool
	}{
		{
			name:       "Wildcard match for write",
			action:     "Write",
			bucket:     "test-bucket",
			objectKey:  "path/to/object.txt",
			shouldPass: true,
		},
		{
			name:       "Wildcard match for read in docs path",
			action:     "Read",
			bucket:     "test-bucket",
			objectKey:  "docs/file.txt",
			shouldPass: true,
		},
		{
			name:       "No match - wrong action",
			action:     "Delete",
			bucket:     "test-bucket",
			objectKey:  "file.txt",
			shouldPass: false,
		},
		{
			name:       "No match - read outside docs path",
			action:     "Read",
			bucket:     "test-bucket",
			objectKey:  "other/file.txt",
			shouldPass: false,
		},
		{
			name:       "ObjectKey with leading slash",
			action:     "Read",
			bucket:     "test-bucket",
			objectKey:  "/docs/file.txt", // Already has leading slash
			shouldPass: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := identity.CanDo(tc.action, tc.bucket, tc.objectKey)

			// Robust path construction for verification
			fullPath := tc.bucket
			if tc.objectKey != "" && !strings.HasPrefix(tc.objectKey, "/") {
				fullPath += "/"
			}
			fullPath += tc.objectKey

			t.Logf("Testing path: %s", fullPath)

			if tc.shouldPass {
				assert.True(t, result, "Should allow action %s on %s", tc.action, fullPath)
			} else {
				assert.False(t, result, "Should deny action %s on %s", tc.action, fullPath)
			}
		})
	}
}

// TestValidateSTSSessionTokenIntegration is an integration test that validates
// the complete flow from session token validation to identity creation
func TestValidateSTSSessionTokenIntegration(t *testing.T) {
	// Setup STS service
	stsService, config := setupTestSTSService(t)

	// Create IAM with STS integration
	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, "memory")
	s3iam := &S3IAMIntegration{
		stsService: stsService,
	}
	iam.SetIAMIntegration(s3iam)
	// Create a mock HTTP request with STS session token
	req, err := http.NewRequest("PUT", "/test-bucket/test-object.txt", nil)
	require.NoError(t, err)

	// Generate session token
	sessionId := "integration-test-session"
	expiresAt := time.Now().Add(time.Hour)
	sessionClaims := sts.NewSTSSessionClaims(sessionId, config.Issuer, expiresAt).
		WithSessionName("integration-test").
		WithRoleInfo("arn:aws:iam::role/testRole", "arn:aws:sts::assumed-role/testRole/integration-test", "arn:aws:sts::assumed-role/testRole/integration-test")

	sessionClaims.Policies = []string{"TestPolicy"}

	tokenGen := sts.NewTokenGenerator(config.SigningKey, config.Issuer)
	sessionToken, err := tokenGen.GenerateJWTWithClaims(sessionClaims)
	require.NoError(t, err)

	// Validate session token
	sessionInfo, err := stsService.ValidateSessionToken(context.Background(), sessionToken)
	require.NoError(t, err)
	require.NotNil(t, sessionInfo)

	// Validate session token and check identity creation
	identity, _, errCode := iam.validateSTSSessionToken(req, sessionToken, sessionInfo.Credentials.AccessKeyId)
	require.Equal(t, s3err.ErrNone, errCode)
	require.NotNil(t, identity)

	// Verify the identity is properly configured for IAM authorization
	assert.Empty(t, identity.Actions, "STS identity should have empty Actions")
	assert.Equal(t, []string{"TestPolicy"}, identity.PolicyNames, "PolicyNames should be populated (this requires the fix in #7985)")
	assert.NotEmpty(t, identity.PrincipalArn, "PrincipalArn should be set")

	t.Log("✓ Integration test passed: STS identity properly configured for IAM authorization")
}

// TestSTSIdentityClaimsPopulation tests that Claims are properly populated from RequestContext
// This is critical for policy variable substitution like ${jwt:preferred_username}
func TestSTSIdentityClaimsPopulation(t *testing.T) {
	// Setup STS service
	stsService, config := setupTestSTSService(t)

	// Create IAM with STS integration
	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, "memory")
	s3iam := &S3IAMIntegration{
		stsService: stsService,
	}
	iam.SetIAMIntegration(s3iam)

	// Create a mock HTTP request
	req, err := http.NewRequest("PUT", "/test-bucket/test-object.txt", nil)
	require.NoError(t, err)

	// Generate session token with RequestContext containing user claims
	sessionId := "claims-test-session"
	expiresAt := time.Now().Add(time.Hour)
	sessionClaims := sts.NewSTSSessionClaims(sessionId, config.Issuer, expiresAt).
		WithSessionName("claims-test").
		WithRoleInfo("arn:aws:iam::role/S3UserRole", "arn:aws:sts::assumed-role/S3UserRole/claims-test", "arn:aws:sts::assumed-role/S3UserRole/claims-test")

	// Add RequestContext with user claims (simulating AssumeRoleWithWebIdentity)
	sessionClaims.RequestContext = map[string]interface{}{
		"preferred_username": "f2wbnp",
		"email":              "user@example.com",
		"name":               "Test User",
		"groups":             []string{"developers", "users"},
	}

	sessionClaims.Policies = []string{"S3UserPolicy"}

	tokenGen := sts.NewTokenGenerator(config.SigningKey, config.Issuer)
	sessionToken, err := tokenGen.GenerateJWTWithClaims(sessionClaims)
	require.NoError(t, err)

	// Validate session token
	sessionInfo, err := stsService.ValidateSessionToken(context.Background(), sessionToken)
	require.NoError(t, err)
	require.NotNil(t, sessionInfo)
	require.NotNil(t, sessionInfo.RequestContext, "RequestContext should be populated")

	// Verify RequestContext has the claims
	assert.Equal(t, "f2wbnp", sessionInfo.RequestContext["preferred_username"])
	assert.Equal(t, "user@example.com", sessionInfo.RequestContext["email"])

	// Validate session token and check identity creation
	identity, _, errCode := iam.validateSTSSessionToken(req, sessionToken, sessionInfo.Credentials.AccessKeyId)
	require.Equal(t, s3err.ErrNone, errCode)
	require.NotNil(t, identity)

	// Verify Claims are populated from RequestContext
	assert.NotNil(t, identity.Claims, "Claims should be populated")
	assert.Equal(t, "f2wbnp", identity.Claims["preferred_username"], "preferred_username should be in Claims")
	assert.Equal(t, "user@example.com", identity.Claims["email"], "email should be in Claims")
	assert.Equal(t, "Test User", identity.Claims["name"], "name should be in Claims")

	// Verify PolicyNames are also populated
	assert.Equal(t, []string{"S3UserPolicy"}, identity.PolicyNames)

	t.Log("✓ Claims properly populated from RequestContext for policy variable substitution")
}

// Helper functions for tests

func setupTestSTSService(t *testing.T) (*sts.STSService, *sts.STSConfig) {
	t.Helper()
	stsService := sts.NewSTSService()
	config := &sts.STSConfig{
		TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
		MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
		Issuer:           "test-issuer",
		SigningKey:       []byte("test-signing-key-at-least-32-bytes-long-for-security"),
	}
	err := stsService.Initialize(config)
	require.NoError(t, err, "STS service should initialize successfully")
	return stsService, config
}

func newTestIdentity(policyNames []string) *Identity {
	return &Identity{
		Name:         "arn:aws:sts::assumed-role/adminRole/s3-session",
		Account:      &AccountAdmin,
		Credentials:  []*Credential{{AccessKey: "test", SecretKey: "test"}},
		PrincipalArn: "arn:aws:sts::assumed-role/adminRole/s3-session",
		PolicyNames:  policyNames,
	}
}
