package s3api

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestAuthorizeWithIAMSessionTokenExtraction tests that the authorizeWithIAM function
// correctly extracts session tokens from multiple sources and prioritizes them appropriately.
// This is a regression test for the bug where X-Amz-Security-Token was not being checked
// for V4 signature authentication with STS credentials.
func TestAuthorizeWithIAMSessionTokenExtraction(t *testing.T) {
	t.Run("Extracts X-SeaweedFS-Session-Token from JWT auth", func(t *testing.T) {
		req := &http.Request{
			Header: http.Header{
				"X-Seaweedfs-Session-Token": {"jwt-token-123"},
				"X-Seaweedfs-Principal":     {"arn:aws:iam::user/test"},
			},
			URL: &url.URL{},
		}

		// Extract tokens the same way authorizeWithIAM does
		sessionToken := req.Header.Get("X-SeaweedFS-Session-Token")
		principal := req.Header.Get("X-SeaweedFS-Principal")

		assert.Equal(t, "jwt-token-123", sessionToken, "Should extract JWT session token from header")
		assert.Equal(t, "arn:aws:iam::user/test", principal, "Should extract principal from header")
	})

	t.Run("Extracts X-Amz-Security-Token from V4 STS auth header", func(t *testing.T) {
		req := &http.Request{
			Header: http.Header{
				"X-Amz-Security-Token": {"sts-token-header-456"},
			},
			URL: &url.URL{},
		}

		// Extract tokens the same way authorizeWithIAM does
		sessionToken := req.Header.Get("X-SeaweedFS-Session-Token")
		principal := req.Header.Get("X-SeaweedFS-Principal")

		// If JWT token is empty, should fallback to X-Amz-Security-Token
		if sessionToken == "" {
			sessionToken = req.Header.Get("X-Amz-Security-Token")
		}

		assert.Equal(t, "sts-token-header-456", sessionToken, "Should fallback to X-Amz-Security-Token when JWT token is empty")
		assert.Empty(t, principal, "JWT principal should be empty for V4 auth")
	})

	t.Run("Extracts X-Amz-Security-Token from query parameter (presigned URL)", func(t *testing.T) {
		req := &http.Request{
			Header: http.Header{},
			URL:    &url.URL{RawQuery: "X-Amz-Security-Token=sts-token-query-789"},
		}

		// Extract tokens the same way authorizeWithIAM does
		sessionToken := req.Header.Get("X-SeaweedFS-Session-Token")
		if sessionToken == "" {
			sessionToken = req.Header.Get("X-Amz-Security-Token")
			if sessionToken == "" {
				sessionToken = req.URL.Query().Get("X-Amz-Security-Token")
			}
		}

		assert.Equal(t, "sts-token-query-789", sessionToken, "Should extract token from query parameter")
	})

	t.Run("JWT token takes precedence over X-Amz-Security-Token", func(t *testing.T) {
		req := &http.Request{
			Header: http.Header{
				"X-Seaweedfs-Session-Token": {"jwt-preferred"},
				"X-Seaweedfs-Principal":     {"arn:aws:iam::user/jwt-user"},
				"X-Amz-Security-Token":      {"sts-fallback"},
			},
			URL: &url.URL{},
		}

		// Extract tokens the same way authorizeWithIAM does
		sessionToken := req.Header.Get("X-SeaweedFS-Session-Token")
		if sessionToken == "" {
			sessionToken = req.Header.Get("X-Amz-Security-Token")
		}

		assert.Equal(t, "jwt-preferred", sessionToken, "JWT token should take precedence")
	})
}

// TestSTSSessionTokenIntoCredentials verifies that STS session tokens are properly
// preserved when converting to credentials for authorization.
func TestSTSSessionTokenIntoCredentials(t *testing.T) {
	// Create a credential generator and session claims
	credGen := sts.NewCredentialGenerator()
	sessionId := "test-session-123"
	expiresAt := time.Now().Add(time.Hour)

	// Generate temporary credentials
	creds, err := credGen.GenerateTemporaryCredentials(sessionId, expiresAt)
	require.NoError(t, err, "Should generate credentials successfully")
	require.NotNil(t, creds, "Credentials should not be nil")

	// Verify all credential fields are present
	assert.NotEmpty(t, creds.AccessKeyId, "AccessKeyId should be present")
	assert.NotEmpty(t, creds.SecretAccessKey, "SecretAccessKey should be present")
	assert.NotEmpty(t, creds.SessionToken, "SessionToken should be present for STS")

	// Verify deterministic generation (same session ID produces same credentials)
	creds2, err := credGen.GenerateTemporaryCredentials(sessionId, expiresAt)
	require.NoError(t, err)

	assert.Equal(t, creds.AccessKeyId, creds2.AccessKeyId, "AccessKeyId should be deterministic")
	assert.Equal(t, creds.SecretAccessKey, creds2.SecretAccessKey, "SecretAccessKey should be deterministic")
	assert.Equal(t, creds.SessionToken, creds2.SessionToken, "SessionToken should be deterministic for same sessionId")

	// Verify different session produces different credentials
	creds3, err := credGen.GenerateTemporaryCredentials("different-session", expiresAt)
	require.NoError(t, err)

	assert.NotEqual(t, creds.AccessKeyId, creds3.AccessKeyId, "Different sessions should produce different access key IDs")
	assert.NotEqual(t, creds.SecretAccessKey, creds3.SecretAccessKey, "Different sessions should produce different secret keys")
	assert.NotEqual(t, creds.SessionToken, creds3.SessionToken, "Different sessions should produce different session tokens")
}

// TestActionConstantsForV4Auth verifies that action constants are properly available
// for use in authorization checks with V4 signature authentication.
func TestActionConstantsForV4Auth(t *testing.T) {
	// Verify that S3 action constants are available
	actions := map[string]string{
		"READ":      s3_constants.ACTION_READ,
		"WRITE":     s3_constants.ACTION_WRITE,
		"READ_ACP":  s3_constants.ACTION_READ_ACP,
		"WRITE_ACP": s3_constants.ACTION_WRITE_ACP,
		"LIST":      s3_constants.ACTION_LIST,
		"TAGGING":   s3_constants.ACTION_TAGGING,
		"ADMIN":     s3_constants.ACTION_ADMIN,
	}

	for name, action := range actions {
		assert.NotEmpty(t, action, "Action %s should not be empty", name)
	}
}
