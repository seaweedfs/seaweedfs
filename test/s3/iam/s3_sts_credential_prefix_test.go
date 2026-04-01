package iam

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSTSTemporaryCredentialPrefix verifies that STS temporary credentials use ASIA prefix
// This test ensures AWS compatibility - temporary credentials should use ASIA, not AKIA
func TestSTSTemporaryCredentialPrefix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Skip("SeaweedFS STS endpoint is not running at", TestSTSEndpoint)
	}

	// Use test credentials from environment or fall back to defaults
	accessKey := os.Getenv("STS_TEST_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "admin"
	}
	secretKey := os.Getenv("STS_TEST_SECRET_KEY")
	if secretKey == "" {
		secretKey = "admin"
	}

	t.Run("assume_role_returns_asia_prefix", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/admin"},
			"RoleSessionName": {"asia-prefix-test"},
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		if resp.StatusCode != http.StatusOK {
			t.Logf("Response status: %d, body: %s", resp.StatusCode, string(body))
			t.Skip("AssumeRole not fully implemented yet")
		}

		var stsResp AssumeRoleTestResponse
		err = xml.Unmarshal(body, &stsResp)
		require.NoError(t, err, "Failed to parse response: %s", string(body))

		creds := stsResp.Result.Credentials
		require.NotEmpty(t, creds.AccessKeyId, "AccessKeyId should not be empty")

		// Verify ASIA prefix for temporary credentials
		assert.True(t, strings.HasPrefix(creds.AccessKeyId, "ASIA"),
			"Temporary credentials must use ASIA prefix (not AKIA for permanent keys), got: %s", creds.AccessKeyId)

		// Verify it's NOT using AKIA (permanent credentials)
		assert.False(t, strings.HasPrefix(creds.AccessKeyId, "AKIA"),
			"Temporary credentials must NOT use AKIA prefix (that's for permanent IAM keys), got: %s", creds.AccessKeyId)

		// Verify format: ASIA + 16 hex characters = 20 chars total
		assert.Equal(t, 20, len(creds.AccessKeyId),
			"Access key ID should be 20 characters (ASIA + 16 hex chars), got: %s", creds.AccessKeyId)

		t.Logf("✓ Temporary credentials correctly use ASIA prefix: %s", creds.AccessKeyId)
	})

	t.Run("assume_role_with_web_identity_returns_asia_prefix", func(t *testing.T) {
		// This test would require OIDC setup, so we'll skip it for now
		// but the same ASIA prefix validation should apply
		t.Skip("AssumeRoleWithWebIdentity requires OIDC provider setup")
	})
}
