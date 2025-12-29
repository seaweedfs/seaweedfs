package iam

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// STS API test constants
const (
	TestSTSEndpoint = "http://localhost:8333"
)

// AssumeRoleWithWebIdentityResponse represents the STS response
type AssumeRoleWithWebIdentityTestResponse struct {
	XMLName xml.Name `xml:"AssumeRoleWithWebIdentityResponse"`
	Result  struct {
		Credentials struct {
			AccessKeyId     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
			SessionToken    string `xml:"SessionToken"`
			Expiration      string `xml:"Expiration"`
		} `xml:"Credentials"`
		SubjectFromWebIdentityToken string `xml:"SubjectFromWebIdentityToken,omitempty"`
	} `xml:"AssumeRoleWithWebIdentityResult"`
}

// STSErrorResponse represents an STS error response
type STSErrorTestResponse struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Error   struct {
		Type    string `xml:"Type"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
	RequestId string `xml:"RequestId"`
}

// TestAssumeRoleWithWebIdentityValidation tests input validation for the STS endpoint
func TestAssumeRoleWithWebIdentityValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Skip("SeaweedFS STS endpoint is not running at", TestSTSEndpoint)
	}

	t.Run("missing_web_identity_token", func(t *testing.T) {
		resp, err := callSTSAPI(t, url.Values{
			"Action":          {"AssumeRoleWithWebIdentity"},
			"RoleArn":         {"arn:aws:iam::role/test-role"},
			"RoleSessionName": {"test-session"},
			// WebIdentityToken is missing
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail without WebIdentityToken")

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		xml.Unmarshal(body, &errResp)
		assert.Equal(t, "MissingParameter", errResp.Error.Code)
	})

	t.Run("missing_role_arn", func(t *testing.T) {
		resp, err := callSTSAPI(t, url.Values{
			"Action":           {"AssumeRoleWithWebIdentity"},
			"WebIdentityToken": {"fake-jwt-token"},
			"RoleSessionName":  {"test-session"},
			// RoleArn is missing
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail without RoleArn")

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		xml.Unmarshal(body, &errResp)
		assert.Equal(t, "MissingParameter", errResp.Error.Code)
	})

	t.Run("missing_role_session_name", func(t *testing.T) {
		resp, err := callSTSAPI(t, url.Values{
			"Action":           {"AssumeRoleWithWebIdentity"},
			"WebIdentityToken": {"fake-jwt-token"},
			"RoleArn":          {"arn:aws:iam::role/test-role"},
			// RoleSessionName is missing
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail without RoleSessionName")

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		xml.Unmarshal(body, &errResp)
		assert.Equal(t, "MissingParameter", errResp.Error.Code)
	})

	t.Run("invalid_jwt_token", func(t *testing.T) {
		resp, err := callSTSAPI(t, url.Values{
			"Action":           {"AssumeRoleWithWebIdentity"},
			"WebIdentityToken": {"not-a-valid-jwt-token"},
			"RoleArn":          {"arn:aws:iam::role/test-role"},
			"RoleSessionName":  {"test-session"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should fail with AccessDenied since the JWT is invalid
		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail with invalid JWT token")

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		xml.Unmarshal(body, &errResp)
		assert.Contains(t, []string{"AccessDenied", "InvalidParameterValue"}, errResp.Error.Code)
	})
}

// TestAssumeRoleWithWebIdentityWithMockJWT tests the STS endpoint with mock JWTs
// This test requires the mock OIDC provider to be configured
func TestAssumeRoleWithWebIdentityWithMockJWT(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Skip("SeaweedFS STS endpoint is not running at", TestSTSEndpoint)
	}

	// Create a test framework to get valid JWT tokens
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	// Generate a test JWT using the framework
	testUsername := "sts-test-user"
	testRole := "readonly"

	token, err := framework.GenerateTestJWT(testUsername, testRole)
	if err != nil {
		t.Skipf("Unable to generate test JWT (requires mock OIDC or Keycloak): %v", err)
	}

	t.Run("valid_jwt_token", func(t *testing.T) {
		resp, err := callSTSAPI(t, url.Values{
			"Action":           {"AssumeRoleWithWebIdentity"},
			"WebIdentityToken": {token},
			"RoleArn":          {"arn:aws:iam::role/" + testRole},
			"RoleSessionName":  {"integration-test-session"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		t.Logf("Response status: %d, body: %s", resp.StatusCode, string(body))

		// Note: This may still fail if the role/trust policy is not configured
		// In that case, we just verify the error is about trust policy, not token validation
		if resp.StatusCode != http.StatusOK {
			var errResp STSErrorTestResponse
			xml.Unmarshal(body, &errResp)
			assert.NotEqual(t, "InvalidParameterValue", errResp.Error.Code,
				"Token validation should not fail - error should be about trust policy")
		} else {
			var stsResp AssumeRoleWithWebIdentityTestResponse
			err = xml.Unmarshal(body, &stsResp)
			require.NoError(t, err)

			creds := stsResp.Result.Credentials
			assert.NotEmpty(t, creds.AccessKeyId, "AccessKeyId should not be empty")
			assert.NotEmpty(t, creds.SecretAccessKey, "SecretAccessKey should not be empty")
			assert.NotEmpty(t, creds.SessionToken, "SessionToken should not be empty")
			assert.NotEmpty(t, creds.Expiration, "Expiration should not be empty")

			t.Logf("Successfully obtained temporary credentials: AccessKeyId=%s", creds.AccessKeyId)
		}
	})

	t.Run("with_duration_seconds", func(t *testing.T) {
		resp, err := callSTSAPI(t, url.Values{
			"Action":           {"AssumeRoleWithWebIdentity"},
			"WebIdentityToken": {token},
			"RoleArn":          {"arn:aws:iam::role/" + testRole},
			"RoleSessionName":  {"integration-test-session"},
			"DurationSeconds":  {"3600"}, // 1 hour
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		// Verify the request is accepted (even if trust policy causes rejection)
		body, _ := io.ReadAll(resp.Body)

		// Should not fail with InvalidParameterValue for DurationSeconds
		if resp.StatusCode != http.StatusOK {
			var errResp STSErrorTestResponse
			xml.Unmarshal(body, &errResp)
			assert.NotContains(t, errResp.Error.Message, "DurationSeconds",
				"DurationSeconds parameter should be accepted")
		}
	})
}

// callSTSAPI is a helper to make STS API calls
func callSTSAPI(t *testing.T, params url.Values) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, TestSTSEndpoint+"/",
		strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}

// isSTSEndpointRunning checks if SeaweedFS STS endpoint is running
func isSTSEndpointRunning(t *testing.T) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(TestSTSEndpoint + "/status")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
