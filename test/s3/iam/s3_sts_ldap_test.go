package iam

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssumeRoleWithLDAPIdentityResponse represents the STS response for LDAP identity
type AssumeRoleWithLDAPIdentityTestResponse struct {
	XMLName xml.Name `xml:"AssumeRoleWithLDAPIdentityResponse"`
	Result  struct {
		Credentials struct {
			AccessKeyId     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
			SessionToken    string `xml:"SessionToken"`
			Expiration      string `xml:"Expiration"`
		} `xml:"Credentials"`
	} `xml:"AssumeRoleWithLDAPIdentityResult"`
}

// TestSTSLDAPValidation tests input validation for AssumeRoleWithLDAPIdentity
func TestSTSLDAPValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Fatal("SeaweedFS STS endpoint is not running at", TestSTSEndpoint, "- please run 'make setup-all-tests' first")
	}

	// Check if AssumeRoleWithLDAPIdentity is implemented
	if !isLDAPIdentityActionImplemented(t) {
		t.Fatal("AssumeRoleWithLDAPIdentity action is not implemented in the running server - please rebuild weed binary with new code and restart the server")
	}

	t.Run("missing_ldap_username", func(t *testing.T) {
		resp, err := callSTSAPIForLDAP(t, url.Values{
			"Action":          {"AssumeRoleWithLDAPIdentity"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/test-role"},
			"RoleSessionName": {"test-session"},
			"LDAPPassword":    {"testpass"},
			// LDAPUsername is missing
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail without LDAPUsername")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var errResp STSErrorTestResponse
		err = xml.Unmarshal(body, &errResp)
		require.NoError(t, err, "Failed to parse error response: %s", string(body))
		// Expect either MissingParameter or InvalidAction (if not implemented)
		assert.Contains(t, []string{"MissingParameter", "InvalidAction"}, errResp.Error.Code)
	})

	t.Run("missing_ldap_password", func(t *testing.T) {
		resp, err := callSTSAPIForLDAP(t, url.Values{
			"Action":          {"AssumeRoleWithLDAPIdentity"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/test-role"},
			"RoleSessionName": {"test-session"},
			"LDAPUsername":    {"testuser"},
			// LDAPPassword is missing
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail without LDAPPassword")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var errResp STSErrorTestResponse
		err = xml.Unmarshal(body, &errResp)
		require.NoError(t, err, "Failed to parse error response: %s", string(body))
		assert.Contains(t, []string{"MissingParameter", "InvalidAction"}, errResp.Error.Code)
	})

	t.Run("missing_role_arn", func(t *testing.T) {
		resp, err := callSTSAPIForLDAP(t, url.Values{
			"Action":          {"AssumeRoleWithLDAPIdentity"},
			"Version":         {"2011-06-15"},
			"RoleSessionName": {"test-session"},
			"LDAPUsername":    {"testuser"},
			"LDAPPassword":    {"testpass"},
			// RoleArn is missing
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail without RoleArn")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var errResp STSErrorTestResponse
		err = xml.Unmarshal(body, &errResp)
		require.NoError(t, err, "Failed to parse error response: %s", string(body))
		assert.Contains(t, []string{"MissingParameter", "InvalidAction"}, errResp.Error.Code)
	})

	t.Run("invalid_duration_too_short", func(t *testing.T) {
		resp, err := callSTSAPIForLDAP(t, url.Values{
			"Action":          {"AssumeRoleWithLDAPIdentity"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/test-role"},
			"RoleSessionName": {"test-session"},
			"LDAPUsername":    {"testuser"},
			"LDAPPassword":    {"testpass"},
			"DurationSeconds": {"100"}, // Less than 900 seconds minimum
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		// If the action is implemented, it should reject invalid duration
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("Response for invalid duration: status=%d, body=%s", resp.StatusCode, string(body))
	})
}

// TestSTSLDAPWithValidCredentials tests LDAP authentication
// This test requires an LDAP server to be configured
func TestSTSLDAPWithValidCredentials(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Skip("SeaweedFS STS endpoint is not running at", TestSTSEndpoint)
	}

	// Check if LDAP is configured (skip if not)
	if !isLDAPConfigured() {
		t.Skip("LDAP is not configured - skipping LDAP integration tests")
	}

	t.Run("successful_ldap_auth", func(t *testing.T) {
		resp, err := callSTSAPIForLDAP(t, url.Values{
			"Action":          {"AssumeRoleWithLDAPIdentity"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/ldap-user"},
			"RoleSessionName": {"ldap-test-session"},
			"LDAPUsername":    {"testuser"},
			"LDAPPassword":    {"testpass"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("Response status: %d, body: %s", resp.StatusCode, string(body))

		if resp.StatusCode == http.StatusOK {
			var stsResp AssumeRoleWithLDAPIdentityTestResponse
			err = xml.Unmarshal(body, &stsResp)
			require.NoError(t, err, "Failed to parse response: %s", string(body))

			creds := stsResp.Result.Credentials
			assert.NotEmpty(t, creds.AccessKeyId, "AccessKeyId should not be empty")
			assert.NotEmpty(t, creds.SecretAccessKey, "SecretAccessKey should not be empty")
			assert.NotEmpty(t, creds.SessionToken, "SessionToken should not be empty")
			assert.NotEmpty(t, creds.Expiration, "Expiration should not be empty")
		}
	})
}

// TestSTSLDAPWithInvalidCredentials tests LDAP rejection with bad credentials
func TestSTSLDAPWithInvalidCredentials(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Skip("SeaweedFS STS endpoint is not running at", TestSTSEndpoint)
	}

	t.Run("invalid_ldap_password", func(t *testing.T) {
		resp, err := callSTSAPIForLDAP(t, url.Values{
			"Action":          {"AssumeRoleWithLDAPIdentity"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/ldap-user"},
			"RoleSessionName": {"ldap-test-session"},
			"LDAPUsername":    {"testuser"},
			"LDAPPassword":    {"wrong-password"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("Response for invalid LDAP credentials: status=%d, body=%s", resp.StatusCode, string(body))

		// Should fail (either AccessDenied or InvalidAction if not implemented)
		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail with invalid LDAP password")
	})

	t.Run("nonexistent_ldap_user", func(t *testing.T) {
		resp, err := callSTSAPIForLDAP(t, url.Values{
			"Action":          {"AssumeRoleWithLDAPIdentity"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/ldap-user"},
			"RoleSessionName": {"ldap-test-session"},
			"LDAPUsername":    {"nonexistent-user-12345"},
			"LDAPPassword":    {"somepassword"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("Response for nonexistent user: status=%d, body=%s", resp.StatusCode, string(body))

		// Should fail
		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail with nonexistent LDAP user")
	})
}

// callSTSAPIForLDAP makes an STS API call for LDAP operation
func callSTSAPIForLDAP(t *testing.T, params url.Values) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, TestSTSEndpoint+"/",
		strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}

// isLDAPConfigured checks if LDAP server is configured and available
func isLDAPConfigured() bool {
	// Check environment variable for LDAP URL
	ldapURL := os.Getenv("LDAP_URL")
	return ldapURL != ""
}

// isLDAPIdentityActionImplemented checks if the running server supports AssumeRoleWithLDAPIdentity
func isLDAPIdentityActionImplemented(t *testing.T) bool {
	resp, err := callSTSAPIForLDAP(t, url.Values{
		"Action":          {"AssumeRoleWithLDAPIdentity"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::role/test"},
		"RoleSessionName": {"test"},
		"LDAPUsername":    {"test"},
		"LDAPPassword":    {"test"},
	})
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	// If we get "NotImplemented" or empty response, the action isn't supported
	if len(body) == 0 {
		return false
	}

	var errResp STSErrorTestResponse
	if xml.Unmarshal(body, &errResp) == nil && errResp.Error.Code == "NotImplemented" {
		return false
	}

	// If we get InvalidAction, the action isn't routed
	if errResp.Error.Code == "InvalidAction" {
		return false
	}

	return true
}
