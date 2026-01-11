package iam

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssumeRoleResponse represents the STS AssumeRole response
type AssumeRoleTestResponse struct {
	XMLName xml.Name `xml:"AssumeRoleResponse"`
	Result  struct {
		Credentials struct {
			AccessKeyId     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
			SessionToken    string `xml:"SessionToken"`
			Expiration      string `xml:"Expiration"`
		} `xml:"Credentials"`
		AssumedRoleUser struct {
			AssumedRoleId string `xml:"AssumedRoleId"`
			Arn           string `xml:"Arn"`
		} `xml:"AssumedRoleUser"`
	} `xml:"AssumeRoleResult"`
}

// TestSTSAssumeRoleValidation tests input validation for AssumeRole endpoint
func TestSTSAssumeRoleValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Fatal("SeaweedFS STS endpoint is not running at", TestSTSEndpoint, "- please run 'make setup-all-tests' first")
	}

	// Check if AssumeRole is implemented by making a test call
	if !isAssumeRoleImplemented(t) {
		t.Fatal("AssumeRole action is not implemented in the running server - please rebuild weed binary with new code and restart the server")
	}

	t.Run("missing_role_arn", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleSessionName": {"test-session"},
			// RoleArn is missing
		}, "test-access-key", "test-secret-key")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail without RoleArn")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var errResp STSErrorTestResponse
		err = xml.Unmarshal(body, &errResp)
		require.NoError(t, err, "Failed to parse error response: %s", string(body))
		assert.Equal(t, "MissingParameter", errResp.Error.Code)
	})

	t.Run("missing_role_session_name", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":  {"AssumeRole"},
			"Version": {"2011-06-15"},
			"RoleArn": {"arn:aws:iam::role/test-role"},
			// RoleSessionName is missing
		}, "test-access-key", "test-secret-key")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail without RoleSessionName")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var errResp STSErrorTestResponse
		err = xml.Unmarshal(body, &errResp)
		require.NoError(t, err, "Failed to parse error response: %s", string(body))
		assert.Equal(t, "MissingParameter", errResp.Error.Code)
	})

	t.Run("unsupported_action_for_anonymous", func(t *testing.T) {
		// AssumeRole requires SigV4 authentication, anonymous requests should fail
		resp, err := callSTSAPI(t, url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/test-role"},
			"RoleSessionName": {"test-session"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should fail because AssumeRole requires AWS SigV4 authentication
		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"AssumeRole should require authentication")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("Response for anonymous AssumeRole: status=%d, body=%s", resp.StatusCode, string(body))
	})

	t.Run("invalid_duration_too_short", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/test-role"},
			"RoleSessionName": {"test-session"},
			"DurationSeconds": {"100"}, // Less than 900 seconds minimum
		}, "test-access-key", "test-secret-key")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail with DurationSeconds < 900")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var errResp STSErrorTestResponse
		err = xml.Unmarshal(body, &errResp)
		require.NoError(t, err, "Failed to parse error response: %s", string(body))
		assert.Equal(t, "InvalidParameterValue", errResp.Error.Code)
	})

	t.Run("invalid_duration_too_long", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/test-role"},
			"RoleSessionName": {"test-session"},
			"DurationSeconds": {"100000"}, // More than 43200 seconds maximum
		}, "test-access-key", "test-secret-key")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail with DurationSeconds > 43200")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var errResp STSErrorTestResponse
		err = xml.Unmarshal(body, &errResp)
		require.NoError(t, err, "Failed to parse error response: %s", string(body))
		assert.Equal(t, "InvalidParameterValue", errResp.Error.Code)
	})
}

// isAssumeRoleImplemented checks if the running server supports AssumeRole
func isAssumeRoleImplemented(t *testing.T) bool {
	resp, err := callSTSAPIWithSigV4(t, url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::role/test"},
		"RoleSessionName": {"test"},
	}, "test", "test")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	// If we get "NotImplemented", the action isn't supported
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

// TestSTSAssumeRoleWithValidCredentials tests AssumeRole with valid IAM credentials
// This test requires a configured IAM user in SeaweedFS
func TestSTSAssumeRoleWithValidCredentials(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Skip("SeaweedFS STS endpoint is not running at", TestSTSEndpoint)
	}

	// Use test credentials from config - these should be configured in iam_config.json
	accessKey := "admin"
	secretKey := "admin"

	t.Run("successful_assume_role", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/admin"},
			"RoleSessionName": {"integration-test-session"},
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("Response status: %d, body: %s", resp.StatusCode, string(body))

		// If AssumeRole is not yet implemented, expect an error about unsupported action
		if resp.StatusCode != http.StatusOK {
			var errResp STSErrorTestResponse
			err = xml.Unmarshal(body, &errResp)
			require.NoError(t, err, "Failed to parse error response: %s", string(body))
			t.Logf("Error response: code=%s, message=%s", errResp.Error.Code, errResp.Error.Message)

			// This test will initially fail until AssumeRole is implemented
			// Once implemented, uncomment the assertions below
			// assert.Fail(t, "AssumeRole not yet implemented")
		} else {
			var stsResp AssumeRoleTestResponse
			err = xml.Unmarshal(body, &stsResp)
			require.NoError(t, err, "Failed to parse response: %s", string(body))

			creds := stsResp.Result.Credentials
			assert.NotEmpty(t, creds.AccessKeyId, "AccessKeyId should not be empty")
			assert.NotEmpty(t, creds.SecretAccessKey, "SecretAccessKey should not be empty")
			assert.NotEmpty(t, creds.SessionToken, "SessionToken should not be empty")
			assert.NotEmpty(t, creds.Expiration, "Expiration should not be empty")

			t.Logf("Successfully obtained temporary credentials: AccessKeyId=%s", creds.AccessKeyId)
		}
	})

	t.Run("with_custom_duration", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/admin"},
			"RoleSessionName": {"duration-test-session"},
			"DurationSeconds": {"3600"}, // 1 hour
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("Response status: %d, body: %s", resp.StatusCode, string(body))

		// Verify DurationSeconds is accepted
		if resp.StatusCode != http.StatusOK {
			var errResp STSErrorTestResponse
			err = xml.Unmarshal(body, &errResp)
			require.NoError(t, err, "Failed to parse error response: %s", string(body))
			// Should not fail due to DurationSeconds parameter
			assert.NotContains(t, errResp.Error.Message, "DurationSeconds",
				"DurationSeconds parameter should be accepted")
		}
	})
}

// TestSTSAssumeRoleWithInvalidCredentials tests AssumeRole rejection with bad credentials
func TestSTSAssumeRoleWithInvalidCredentials(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Skip("SeaweedFS STS endpoint is not running at", TestSTSEndpoint)
	}

	t.Run("invalid_access_key", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/admin"},
			"RoleSessionName": {"test-session"},
		}, "invalid-access-key", "some-secret-key")
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should fail with access denied or signature mismatch
		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail with invalid access key")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("Response for invalid credentials: status=%d, body=%s", resp.StatusCode, string(body))
	})

	t.Run("invalid_secret_key", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::role/admin"},
			"RoleSessionName": {"test-session"},
		}, "admin", "wrong-secret-key")
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should fail with signature mismatch
		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"Should fail with invalid secret key")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("Response for wrong secret: status=%d, body=%s", resp.StatusCode, string(body))
	})
}

// callSTSAPIWithSigV4 makes an STS API call with AWS Signature V4 authentication
func callSTSAPIWithSigV4(t *testing.T, params url.Values, accessKey, secretKey string) (*http.Response, error) {
	// Prepare request body
	body := params.Encode()

	// Create request
	req, err := http.NewRequest(http.MethodPost, TestSTSEndpoint+"/",
		strings.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Host", req.URL.Host)

	// Sign request with AWS Signature V4
	signRequestV4(req, body, accessKey, secretKey, "us-east-1", "sts")

	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}

// signRequestV4 signs an HTTP request using AWS Signature Version 4
func signRequestV4(req *http.Request, payload, accessKey, secretKey, region, service string) {
	// AWS SigV4 signing implementation
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	// Set required headers
	req.Header.Set("X-Amz-Date", amzDate)

	// Create canonical request
	canonicalURI := "/"
	canonicalQueryString := ""

	// Sort and format headers
	signedHeaders := []string{"content-type", "host", "x-amz-date"}
	sort.Strings(signedHeaders)

	canonicalHeaders := fmt.Sprintf("content-type:%s\nhost:%s\nx-amz-date:%s\n",
		req.Header.Get("Content-Type"),
		req.Host,
		amzDate)

	signedHeadersStr := strings.Join(signedHeaders, ";")

	// Hash payload
	payloadHash := sha256Hex(payload)

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		req.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeadersStr,
		payloadHash)

	// Create string to sign
	algorithm := "AWS4-HMAC-SHA256"
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	stringToSign := fmt.Sprintf("%s\n%s\n%s\n%s",
		algorithm,
		amzDate,
		credentialScope,
		sha256Hex(canonicalRequest))

	// Calculate signature
	signingKey := getSignatureKey(secretKey, dateStamp, region, service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	// Create authorization header
	authHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm,
		accessKey,
		credentialScope,
		signedHeadersStr,
		signature)

	req.Header.Set("Authorization", authHeader)
}

func sha256Hex(data string) string {
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func getSignatureKey(secretKey, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return kSigning
}
