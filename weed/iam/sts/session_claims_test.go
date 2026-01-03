package sts

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSTSSessionClaimsToSessionInfo tests the ToSessionInfo conversion
func TestSTSSessionClaimsToSessionInfo(t *testing.T) {
	sessionId := "test-session-123"
	issuer := "test-issuer"
	expiresAt := time.Now().Add(time.Hour)

	claims := NewSTSSessionClaims(sessionId, issuer, expiresAt).
		WithSessionName("test-session-name").
		WithRoleInfo(
			"arn:aws:iam::123456789012:role/test-role",
			"arn:aws:iam::123456789012:assumed-role/test-role/session",
			"arn:aws:iam::123456789012:assumed-role/test-role/session",
		).
		WithIdentityProvider("oidc", "user-123", "https://issuer.example.com").
		WithMaxDuration(time.Hour)

	sessionInfo := claims.ToSessionInfo()

	// Verify basic claims are converted
	assert.Equal(t, sessionId, sessionInfo.SessionId)
	assert.Equal(t, "test-session-name", sessionInfo.SessionName)
	assert.Equal(t, "arn:aws:iam::123456789012:role/test-role", sessionInfo.RoleArn)
	assert.Equal(t, "arn:aws:iam::123456789012:assumed-role/test-role/session", sessionInfo.AssumedRoleUser)
	assert.Equal(t, "oidc", sessionInfo.IdentityProvider)
	assert.Equal(t, "user-123", sessionInfo.ExternalUserId)

	// Verify credentials are generated
	assert.NotNil(t, sessionInfo.Credentials, "credentials should be populated")
	assert.NotEmpty(t, sessionInfo.Credentials.AccessKeyId, "access key should be generated")
	assert.NotEmpty(t, sessionInfo.Credentials.SecretAccessKey, "secret key should be generated")
	// Credential expiration may have sub-second differences, so just check they're close
	assert.True(t, sessionInfo.Credentials.Expiration.Sub(expiresAt) < time.Second, "credential expiration should match session expiration")

	// Verify expiration is preserved (within 1 second tolerance for timing differences)
	assert.WithinDuration(t, expiresAt, sessionInfo.ExpiresAt, 1*time.Second)
}

// TestSTSSessionClaimsToSessionInfoCredentialGeneration tests that credentials are deterministically generated for access key and session token
func TestSTSSessionClaimsToSessionInfoCredentialGeneration(t *testing.T) {
	sessionId := "deterministic-session-id"
	issuer := "test-issuer"
	expiresAt := time.Now().Add(time.Hour).Truncate(time.Second)

	claims1 := NewSTSSessionClaims(sessionId, issuer, expiresAt)
	sessionInfo1 := claims1.ToSessionInfo()

	// Create another claims object with the same session ID and expiration
	claims2 := NewSTSSessionClaims(sessionId, issuer, expiresAt)
	sessionInfo2 := claims2.ToSessionInfo()

	// Verify that both have valid credentials
	assert.NotNil(t, sessionInfo1.Credentials, "credentials should be populated")
	assert.NotNil(t, sessionInfo2.Credentials, "credentials should be populated")

	// Verify deterministic generation: same SessionId should produce identical access key ID
	// (based on hash of session ID, not random)
	assert.Equal(t, sessionInfo1.Credentials.AccessKeyId, sessionInfo2.Credentials.AccessKeyId,
		"same session ID should produce identical access key ID (deterministic hash-based generation)")

	// Session token is also deterministic (hash-based on session ID)
	assert.Equal(t, sessionInfo1.Credentials.SessionToken, sessionInfo2.Credentials.SessionToken,
		"same session ID should produce identical session token (deterministic hash-based generation)")

	// Secret access key is NOW deterministic (hash-based on session ID, not random!)
	// This is critical for signature verification: the same session ID must regenerate
	// the same secret key so that signature verification succeeds.
	assert.Equal(t, sessionInfo1.Credentials.SecretAccessKey, sessionInfo2.Credentials.SecretAccessKey,
		"same session ID should produce identical secret access key (deterministic hash-based generation)")

	// Expiration should match
	assert.WithinDuration(t, sessionInfo1.Credentials.Expiration, sessionInfo2.Credentials.Expiration, 1*time.Second,
		"credentials expiration should match")
}

// TestSTSSessionClaimsToSessionInfoPreservesAllFields tests that all fields are preserved
func TestSTSSessionClaimsToSessionInfoPreservesAllFields(t *testing.T) {
	sessionId := "test-session-id"
	issuer := "test-issuer"
	expiresAt := time.Now().Add(2 * time.Hour)

	policies := []string{"policy1", "policy2"}
	requestContext := map[string]interface{}{
		"sourceIp":  "192.168.1.1",
		"userAgent": "test-agent",
	}

	claims := NewSTSSessionClaims(sessionId, issuer, expiresAt).
		WithSessionName("session-name").
		WithRoleInfo("role-arn", "assumed-role", "principal").
		WithIdentityProvider("provider", "external-id", "issuer").
		WithPolicies(policies).
		WithRequestContext(requestContext).
		WithMaxDuration(2 * time.Hour)

	sessionInfo := claims.ToSessionInfo()

	// Verify all fields are preserved
	assert.Equal(t, sessionId, sessionInfo.SessionId)
	assert.Equal(t, "session-name", sessionInfo.SessionName)
	assert.Equal(t, "role-arn", sessionInfo.RoleArn)
	assert.Equal(t, "assumed-role", sessionInfo.AssumedRoleUser)
	assert.Equal(t, "principal", sessionInfo.Principal)
	assert.Equal(t, "provider", sessionInfo.IdentityProvider)
	assert.Equal(t, "external-id", sessionInfo.ExternalUserId)
	assert.Equal(t, "issuer", sessionInfo.ProviderIssuer)
	assert.Equal(t, policies, sessionInfo.Policies)
	assert.Equal(t, requestContext, sessionInfo.RequestContext)
	assert.WithinDuration(t, expiresAt, sessionInfo.ExpiresAt, 1*time.Second)
}

// TestSTSSessionClaimsToSessionInfoEmptyFields tests handling of empty/nil fields
func TestSTSSessionClaimsToSessionInfoEmptyFields(t *testing.T) {
	sessionId := "minimal-session"
	issuer := "issuer"
	expiresAt := time.Now().Add(time.Hour)

	// Create claims with minimal fields
	claims := NewSTSSessionClaims(sessionId, issuer, expiresAt)

	sessionInfo := claims.ToSessionInfo()

	// Verify basic fields are preserved
	assert.Equal(t, sessionId, sessionInfo.SessionId)
	assert.Empty(t, sessionInfo.SessionName)
	assert.Empty(t, sessionInfo.RoleArn)

	// Verify credentials are still generated even with minimal fields
	assert.NotNil(t, sessionInfo.Credentials)
	assert.NotEmpty(t, sessionInfo.Credentials.AccessKeyId)
	assert.NotEmpty(t, sessionInfo.Credentials.SecretAccessKey)
}

// TestSTSSessionClaimsToSessionInfoCredentialExpiration tests credential expiration
func TestSTSSessionClaimsToSessionInfoCredentialExpiration(t *testing.T) {
	sessionId := "test-session"
	issuer := "issuer"

	tests := []struct {
		name             string
		expiresAt        time.Time
		expectNotExpired bool
		description      string
	}{
		{
			name:             "future_expiration",
			expiresAt:        time.Now().Add(time.Hour),
			expectNotExpired: true,
			description:      "Credentials should not be expired if ExpiresAt is in the future",
		},
		{
			name:             "past_expiration",
			expiresAt:        time.Now().Add(-time.Hour),
			expectNotExpired: false,
			description:      "Credentials should be expired if ExpiresAt is in the past",
		},
		{
			name:             "near_future_expiration",
			expiresAt:        time.Now().Add(time.Minute),
			expectNotExpired: true,
			description:      "Credentials should not be expired even if close to expiration",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			claims := NewSTSSessionClaims(sessionId, issuer, tc.expiresAt)
			sessionInfo := claims.ToSessionInfo()

			assert.NotNil(t, sessionInfo.Credentials)
			// Check expiration within 1 second due to timing precision
			assert.True(t, sessionInfo.Credentials.Expiration.Sub(tc.expiresAt) < time.Second)

			// We set tc.expiresAt to past/future values to exercise expiration handling.
			// Assert the credentials' expiration relative to now to exercise code behavior
			if tc.expectNotExpired {
				assert.True(t, time.Now().Before(sessionInfo.Credentials.Expiration), tc.description)
			} else {
				assert.True(t, time.Now().After(sessionInfo.Credentials.Expiration), tc.description)
			}
		})
	}
}

// TestSessionInfoIntegration tests the full integration of session info flow
func TestSessionInfoIntegration(t *testing.T) {
	// Create a session claim
	sessionId, err := GenerateSessionId()
	require.NoError(t, err)

	expiresAt := time.Now().Add(time.Hour)
	claims := NewSTSSessionClaims(sessionId, "test-issuer", expiresAt).
		WithSessionName("integration-test").
		WithRoleInfo(
			"arn:aws:iam::123456789012:role/integration",
			"arn:aws:iam::123456789012:assumed-role/integration/test",
			"arn:aws:iam::123456789012:assumed-role/integration/test",
		).
		WithIdentityProvider("test-provider", "user-id", "https://test.example.com")

	// Convert to SessionInfo
	sessionInfo := claims.ToSessionInfo()

	// Verify the session info has valid credentials
	assert.NotNil(t, sessionInfo.Credentials)
	assert.NotEmpty(t, sessionInfo.Credentials.AccessKeyId)
	assert.NotEmpty(t, sessionInfo.Credentials.SecretAccessKey)

	// Verify basic session properties
	assert.Equal(t, sessionId, sessionInfo.SessionId)
	assert.Equal(t, "integration-test", sessionInfo.SessionName)
	assert.False(t, sessionInfo.ExpiresAt.IsZero())

	// Verify that the session is valid
	assert.True(t, sessionInfo.ExpiresAt.After(time.Now()), "session should not be expired")
	assert.False(t, sessionInfo.Credentials.Expiration.Before(time.Now()), "credentials should not be expired")
}

// TestSecretAccessKeyDeterminism verifies that secret access keys are deterministically
// generated from the session ID. This is CRITICAL for STS signature verification:
// The client generates a secret key and signs the request. When the server receives
// the request, it must regenerate the exact same secret key from the JWT claims
// to verify the signature. If the secret key is random, verification will always fail.
func TestSecretAccessKeyDeterminism(t *testing.T) {
	sessionId := "critical-determinism-test"
	expiration := time.Now().Add(time.Hour)

	// Generate credentials multiple times with the same session ID
	credGen := NewCredentialGenerator()

	cred1, err := credGen.GenerateTemporaryCredentials(sessionId, expiration)
	assert.NoError(t, err)
	assert.NotNil(t, cred1)

	cred2, err := credGen.GenerateTemporaryCredentials(sessionId, expiration)
	assert.NoError(t, err)
	assert.NotNil(t, cred2)

	cred3, err := credGen.GenerateTemporaryCredentials(sessionId, expiration)
	assert.NoError(t, err)
	assert.NotNil(t, cred3)

	// All three should have IDENTICAL secret access keys
	assert.Equal(t, cred1.SecretAccessKey, cred2.SecretAccessKey,
		"same sessionId must produce identical secret key on first and second call")
	assert.Equal(t, cred2.SecretAccessKey, cred3.SecretAccessKey,
		"same sessionId must produce identical secret key on second and third call")

	// All three should have IDENTICAL access key IDs
	assert.Equal(t, cred1.AccessKeyId, cred2.AccessKeyId,
		"same sessionId must produce identical access key ID")
	assert.Equal(t, cred2.AccessKeyId, cred3.AccessKeyId,
		"same sessionId must produce identical access key ID")

	// All three should have IDENTICAL session tokens
	assert.Equal(t, cred1.SessionToken, cred2.SessionToken,
		"same sessionId must produce identical session token")
	assert.Equal(t, cred2.SessionToken, cred3.SessionToken,
		"same sessionId must produce identical session token")

	// Different session IDs should produce different secrets
	otherSessionId := "different-session"
	credOther, err := credGen.GenerateTemporaryCredentials(otherSessionId, expiration)
	assert.NoError(t, err)
	assert.NotNil(t, credOther)

	assert.NotEqual(t, cred1.SecretAccessKey, credOther.SecretAccessKey,
		"different session IDs must produce different secret keys")
	assert.NotEqual(t, cred1.AccessKeyId, credOther.AccessKeyId,
		"different session IDs must produce different access key IDs")
	assert.NotEqual(t, cred1.SessionToken, credOther.SessionToken,
		"different session IDs must produce different session tokens")
}
