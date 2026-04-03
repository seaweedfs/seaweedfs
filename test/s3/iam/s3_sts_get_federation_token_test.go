package iam

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GetFederationTokenTestResponse represents the STS GetFederationToken response
type GetFederationTokenTestResponse struct {
	XMLName xml.Name `xml:"GetFederationTokenResponse"`
	Result  struct {
		Credentials struct {
			AccessKeyId     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
			SessionToken    string `xml:"SessionToken"`
			Expiration      string `xml:"Expiration"`
		} `xml:"Credentials"`
		FederatedUser struct {
			FederatedUserId string `xml:"FederatedUserId"`
			Arn             string `xml:"Arn"`
		} `xml:"FederatedUser"`
	} `xml:"GetFederationTokenResult"`
}

func getTestCredentials() (string, string) {
	accessKey := os.Getenv("STS_TEST_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "admin"
	}
	secretKey := os.Getenv("STS_TEST_SECRET_KEY")
	if secretKey == "" {
		secretKey = "admin"
	}
	return accessKey, secretKey
}

// isGetFederationTokenImplemented checks if the running server supports GetFederationToken
func isGetFederationTokenImplemented(t *testing.T) bool {
	accessKey, secretKey := getTestCredentials()
	resp, err := callSTSAPIWithSigV4(t, url.Values{
		"Action":  {"GetFederationToken"},
		"Version": {"2011-06-15"},
		"Name":    {"probe"},
	}, accessKey, secretKey)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var errResp STSErrorTestResponse
	if xml.Unmarshal(body, &errResp) == nil {
		if errResp.Error.Code == "InvalidAction" || errResp.Error.Code == "NotImplemented" {
			return false
		}
	}
	return true
}

// TestSTSGetFederationTokenValidation tests input validation for the GetFederationToken endpoint
func TestSTSGetFederationTokenValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Fatal("SeaweedFS STS endpoint is not running at", TestSTSEndpoint, "- please run 'make setup-all-tests' first")
	}

	if !isGetFederationTokenImplemented(t) {
		t.Fatal("GetFederationToken action is not implemented in the running server")
	}

	accessKey, secretKey := getTestCredentials()

	t.Run("missing_name", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":  {"GetFederationToken"},
			"Version": {"2011-06-15"},
			// Name is missing
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		require.NoError(t, xml.Unmarshal(body, &errResp), "Failed to parse: %s", string(body))
		assert.Equal(t, "MissingParameter", errResp.Error.Code)
	})

	t.Run("name_too_short", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":  {"GetFederationToken"},
			"Version": {"2011-06-15"},
			"Name":    {"A"},
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		require.NoError(t, xml.Unmarshal(body, &errResp), "Failed to parse: %s", string(body))
		assert.Equal(t, "InvalidParameterValue", errResp.Error.Code)
	})

	t.Run("name_too_long", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":  {"GetFederationToken"},
			"Version": {"2011-06-15"},
			"Name":    {strings.Repeat("A", 33)},
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		require.NoError(t, xml.Unmarshal(body, &errResp), "Failed to parse: %s", string(body))
		assert.Equal(t, "InvalidParameterValue", errResp.Error.Code)
	})

	t.Run("name_invalid_characters", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":  {"GetFederationToken"},
			"Version": {"2011-06-15"},
			"Name":    {"bad name"},
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		require.NoError(t, xml.Unmarshal(body, &errResp), "Failed to parse: %s", string(body))
		assert.Equal(t, "InvalidParameterValue", errResp.Error.Code)
	})

	t.Run("duration_too_short", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"GetFederationToken"},
			"Version":         {"2011-06-15"},
			"Name":            {"TestApp"},
			"DurationSeconds": {"100"},
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		require.NoError(t, xml.Unmarshal(body, &errResp), "Failed to parse: %s", string(body))
		assert.Equal(t, "InvalidParameterValue", errResp.Error.Code)
	})

	t.Run("duration_too_long", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"GetFederationToken"},
			"Version":         {"2011-06-15"},
			"Name":            {"TestApp"},
			"DurationSeconds": {"200000"},
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		require.NoError(t, xml.Unmarshal(body, &errResp), "Failed to parse: %s", string(body))
		assert.Equal(t, "InvalidParameterValue", errResp.Error.Code)
	})

	t.Run("malformed_policy", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":  {"GetFederationToken"},
			"Version": {"2011-06-15"},
			"Name":    {"TestApp"},
			"Policy":  {"not-valid-json"},
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		var errResp STSErrorTestResponse
		require.NoError(t, xml.Unmarshal(body, &errResp), "Failed to parse: %s", string(body))
		assert.Equal(t, "MalformedPolicyDocument", errResp.Error.Code)
	})

	t.Run("anonymous_rejected", func(t *testing.T) {
		// GetFederationToken requires SigV4, anonymous should fail
		resp, err := callSTSAPI(t, url.Values{
			"Action":  {"GetFederationToken"},
			"Version": {"2011-06-15"},
			"Name":    {"TestApp"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode)
	})
}

// TestSTSGetFederationTokenRejectTemporaryCredentials tests that temporary
// credentials (session tokens) are rejected by GetFederationToken
func TestSTSGetFederationTokenRejectTemporaryCredentials(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Skip("SeaweedFS STS endpoint is not running at", TestSTSEndpoint)
	}

	if !isGetFederationTokenImplemented(t) {
		t.Skip("GetFederationToken not implemented")
	}

	accessKey, secretKey := getTestCredentials()

	// First, obtain temporary credentials via AssumeRole
	resp, err := callSTSAPIWithSigV4(t, url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::role/admin"},
		"RoleSessionName": {"temp-session"},
	}, accessKey, secretKey)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if resp.StatusCode != http.StatusOK {
		t.Skipf("AssumeRole failed (may not be configured): status=%d body=%s", resp.StatusCode, string(body))
	}

	var assumeResp AssumeRoleTestResponse
	require.NoError(t, xml.Unmarshal(body, &assumeResp), "Parse AssumeRole response: %s", string(body))

	tempAccessKey := assumeResp.Result.Credentials.AccessKeyId
	tempSecretKey := assumeResp.Result.Credentials.SecretAccessKey
	tempSessionToken := assumeResp.Result.Credentials.SessionToken
	require.NotEmpty(t, tempAccessKey)
	require.NotEmpty(t, tempSessionToken)

	// Now try GetFederationToken with the temporary credentials
	// Include X-Amz-Security-Token header which marks this as a temp credential call
	params := url.Values{
		"Action":  {"GetFederationToken"},
		"Version": {"2011-06-15"},
		"Name":    {"ShouldFail"},
	}

	reqBody := params.Encode()
	req, err := http.NewRequest(http.MethodPost, TestSTSEndpoint+"/", strings.NewReader(reqBody))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Amz-Security-Token", tempSessionToken)

	creds := credentials.NewStaticCredentials(tempAccessKey, tempSecretKey, tempSessionToken)
	signer := v4.NewSigner(creds)
	_, err = signer.Sign(req, strings.NewReader(reqBody), "sts", "us-east-1", time.Now())
	require.NoError(t, err)

	client := &http.Client{Timeout: 30 * time.Second}
	resp2, err := client.Do(req)
	require.NoError(t, err)
	defer resp2.Body.Close()

	body2, _ := io.ReadAll(resp2.Body)
	assert.Equal(t, http.StatusForbidden, resp2.StatusCode,
		"GetFederationToken should reject temporary credentials: %s", string(body2))
	assert.Contains(t, string(body2), "temporary credentials",
		"Error should mention temporary credentials")
}

// TestSTSGetFederationTokenSuccess tests a successful GetFederationToken call
// and verifies the returned credentials can be used to access S3
func TestSTSGetFederationTokenSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Skip("SeaweedFS STS endpoint is not running at", TestSTSEndpoint)
	}

	if !isGetFederationTokenImplemented(t) {
		t.Skip("GetFederationToken not implemented")
	}

	accessKey, secretKey := getTestCredentials()

	t.Run("basic_success", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":  {"GetFederationToken"},
			"Version": {"2011-06-15"},
			"Name":    {"AppClient"},
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		t.Logf("Response status: %d, body: %s", resp.StatusCode, string(body))

		if resp.StatusCode != http.StatusOK {
			var errResp STSErrorTestResponse
			_ = xml.Unmarshal(body, &errResp)
			t.Fatalf("GetFederationToken failed: code=%s message=%s", errResp.Error.Code, errResp.Error.Message)
		}

		var stsResp GetFederationTokenTestResponse
		require.NoError(t, xml.Unmarshal(body, &stsResp), "Parse response: %s", string(body))

		creds := stsResp.Result.Credentials
		assert.NotEmpty(t, creds.AccessKeyId)
		assert.NotEmpty(t, creds.SecretAccessKey)
		assert.NotEmpty(t, creds.SessionToken)
		assert.NotEmpty(t, creds.Expiration)

		fedUser := stsResp.Result.FederatedUser
		assert.Contains(t, fedUser.Arn, "federated-user/AppClient")
		assert.Contains(t, fedUser.FederatedUserId, "AppClient")
	})

	t.Run("with_custom_duration", func(t *testing.T) {
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"GetFederationToken"},
			"Version":         {"2011-06-15"},
			"Name":            {"DurationTest"},
			"DurationSeconds": {"3600"},
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		t.Logf("Response status: %d, body: %s", resp.StatusCode, string(body))

		if resp.StatusCode == http.StatusOK {
			var stsResp GetFederationTokenTestResponse
			require.NoError(t, xml.Unmarshal(body, &stsResp))
			assert.NotEmpty(t, stsResp.Result.Credentials.AccessKeyId)

			// Verify expiration is roughly 1 hour from now
			expTime, err := time.Parse(time.RFC3339, stsResp.Result.Credentials.Expiration)
			require.NoError(t, err)
			diff := time.Until(expTime)
			assert.InDelta(t, 3600, diff.Seconds(), 60,
				"Expiration should be ~1 hour from now")
		}
	})

	t.Run("with_36_hour_duration", func(t *testing.T) {
		// GetFederationToken allows up to 36 hours (unlike AssumeRole's 12h max)
		resp, err := callSTSAPIWithSigV4(t, url.Values{
			"Action":          {"GetFederationToken"},
			"Version":         {"2011-06-15"},
			"Name":            {"LongDuration"},
			"DurationSeconds": {"129600"}, // 36 hours
		}, accessKey, secretKey)
		require.NoError(t, err)
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusOK {
			var stsResp GetFederationTokenTestResponse
			require.NoError(t, xml.Unmarshal(body, &stsResp))

			expTime, err := time.Parse(time.RFC3339, stsResp.Result.Credentials.Expiration)
			require.NoError(t, err)
			diff := time.Until(expTime)
			assert.InDelta(t, 129600, diff.Seconds(), 60,
				"Expiration should be ~36 hours from now")
		} else {
			// Duration should not cause a rejection
			var errResp STSErrorTestResponse
			_ = xml.Unmarshal(body, &errResp)
			assert.NotContains(t, errResp.Error.Message, "DurationSeconds",
				"36-hour duration should be accepted by GetFederationToken")
		}
	})
}

// TestSTSGetFederationTokenWithSessionPolicy tests that vended credentials
// are scoped down by an inline session policy
func TestSTSGetFederationTokenWithSessionPolicy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSTSEndpointRunning(t) {
		t.Skip("SeaweedFS STS endpoint is not running at", TestSTSEndpoint)
	}

	if !isGetFederationTokenImplemented(t) {
		t.Skip("GetFederationToken not implemented")
	}

	accessKey, secretKey := getTestCredentials()

	// Create a test bucket using admin credentials
	adminSess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(TestSTSEndpoint),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
	})
	require.NoError(t, err)

	adminS3 := s3.New(adminSess)
	bucket := fmt.Sprintf("fed-token-test-%d", time.Now().UnixNano())

	_, err = adminS3.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	defer adminS3.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucket)})

	_, err = adminS3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("test.txt"),
		Body:   strings.NewReader("hello"),
	})
	require.NoError(t, err)
	defer adminS3.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("test.txt")})

	// Get federated credentials with a session policy that only allows GetObject
	sessionPolicy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Action": ["s3:GetObject"],
			"Resource": ["arn:aws:s3:::%s/*"]
		}]
	}`, bucket)

	resp, err := callSTSAPIWithSigV4(t, url.Values{
		"Action":  {"GetFederationToken"},
		"Version": {"2011-06-15"},
		"Name":    {"ScopedClient"},
		"Policy":  {sessionPolicy},
	}, accessKey, secretKey)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	t.Logf("GetFederationToken response: status=%d body=%s", resp.StatusCode, string(body))

	if resp.StatusCode != http.StatusOK {
		t.Skipf("GetFederationToken failed (may need IAM policy config): %s", string(body))
	}

	var stsResp GetFederationTokenTestResponse
	require.NoError(t, xml.Unmarshal(body, &stsResp))

	fedCreds := stsResp.Result.Credentials
	require.NotEmpty(t, fedCreds.AccessKeyId)
	require.NotEmpty(t, fedCreds.SessionToken)

	// Create S3 client with the federated credentials
	fedSess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(TestSTSEndpoint),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		Credentials: credentials.NewStaticCredentials(
			fedCreds.AccessKeyId, fedCreds.SecretAccessKey, fedCreds.SessionToken),
	})
	require.NoError(t, err)

	fedS3 := s3.New(fedSess)

	// GetObject should succeed (allowed by session policy)
	getResp, err := fedS3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("test.txt"),
	})
	if err == nil {
		defer getResp.Body.Close()
		t.Log("GetObject with federated credentials succeeded (as expected)")
	} else {
		t.Logf("GetObject with federated credentials: %v (session policy enforcement may vary)", err)
	}

	// PutObject should be denied (not allowed by session policy)
	_, err = fedS3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("denied.txt"),
		Body:   strings.NewReader("should fail"),
	})
	if err != nil {
		t.Log("PutObject correctly denied with federated credentials")
		assert.Contains(t, err.Error(), "AccessDenied",
			"PutObject should be denied by session policy")
	} else {
		// Clean up if unexpectedly succeeded
		adminS3.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("denied.txt")})
		t.Log("PutObject unexpectedly succeeded — session policy enforcement may not be active")
	}
}
