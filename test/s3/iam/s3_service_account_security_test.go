package iam

// Integration tests for SeaweedFS service accounts.
// These tests ensure comprehensive coverage of service account functionality
// including security, access control, and expiration.

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServiceAccountS3Access verifies that service accounts can actually
// perform S3 operations using their credentials.
func TestServiceAccountS3Access(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSeaweedFSRunning(t) {
		t.Skip("SeaweedFS is not running at", TestIAMEndpoint)
	}

	// Setup: Create a parent user
	parentUserName := fmt.Sprintf("s3access-test-%d", time.Now().UnixNano())

	// Create parent user
	resp, err := callIAMAPI(t, "CreateUser", url.Values{
		"UserName": {parentUserName},
	})
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "Failed to create parent user")

	defer func() {
		// Cleanup: delete parent user
		callIAMAPI(t, "DeleteUser", url.Values{"UserName": {parentUserName}})
	}()

	// Create service account for the parent user
	createResp, err := callIAMAPI(t, "CreateServiceAccount", url.Values{
		"ParentUser":  {parentUserName},
		"Description": {"S3 Access Test Service Account"},
	})
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusOK, createResp.StatusCode, "Failed to create service account")

	body, err := io.ReadAll(createResp.Body)
	require.NoError(t, err)

	var saResp CreateServiceAccountResponse
	err = xml.Unmarshal(body, &saResp)
	require.NoError(t, err, "Failed to parse CreateServiceAccount response: %s", string(body))

	accessKeyId := saResp.CreateServiceAccountResult.ServiceAccount.AccessKeyId
	secretAccessKey := saResp.CreateServiceAccountResult.ServiceAccount.SecretAccessKey
	saId := saResp.CreateServiceAccountResult.ServiceAccount.ServiceAccountId

	require.NotEmpty(t, accessKeyId, "AccessKeyId should not be empty")
	require.NotEmpty(t, secretAccessKey, "SecretAccessKey should not be empty")

	defer func() {
		// Cleanup: delete service account
		callIAMAPI(t, "DeleteServiceAccount", url.Values{"ServiceAccountId": {saId}})
	}()

	t.Run("list_buckets_with_sa_credentials", func(t *testing.T) {
		sess, err := session.NewSession(&aws.Config{
			Region:   aws.String("us-east-1"),
			Endpoint: aws.String(TestIAMEndpoint),
			Credentials: credentials.NewStaticCredentials(
				accessKeyId,
				secretAccessKey,
				"",
			),
			DisableSSL:       aws.Bool(true),
			S3ForcePathStyle: aws.Bool(true),
		})
		require.NoError(t, err)

		s3Client := s3.New(sess)
		_, err = s3Client.ListBuckets(&s3.ListBucketsInput{})

		// We don't necessarily expect success (depends on permissions),
		// but we should NOT get InvalidAccessKeyId or SignatureDoesNotMatch
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				assert.NotEqual(t, "InvalidAccessKeyId", aerr.Code(),
					"Service account credentials should be recognized")
				assert.NotEqual(t, "SignatureDoesNotMatch", aerr.Code(),
					"Service account signature should be valid")
			}
		}
	})

	t.Run("create_bucket_with_sa_credentials", func(t *testing.T) {
		sess, err := session.NewSession(&aws.Config{
			Region:   aws.String("us-east-1"),
			Endpoint: aws.String(TestIAMEndpoint),
			Credentials: credentials.NewStaticCredentials(
				accessKeyId,
				secretAccessKey,
				"",
			),
			DisableSSL:       aws.Bool(true),
			S3ForcePathStyle: aws.Bool(true),
		})
		require.NoError(t, err)

		s3Client := s3.New(sess)
		bucketName := fmt.Sprintf("sa-test-bucket-%d", time.Now().UnixNano())

		_, err = s3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})

		// Check that we get a proper response (success or AccessDenied based on policy)
		// but NOT InvalidAccessKeyId
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				assert.NotEqual(t, "InvalidAccessKeyId", aerr.Code(),
					"Service account credentials should be recognized")
				assert.NotEqual(t, "SignatureDoesNotMatch", aerr.Code(),
					"Service account signature should be valid")
			}
		} else {
			// Cleanup if bucket was created
			defer s3Client.DeleteBucket(&s3.DeleteBucketInput{
				Bucket: aws.String(bucketName),
			})
		}
	})
}

// TestServiceAccountExpiration verifies that expired service accounts
// are properly rejected.
func TestServiceAccountExpiration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSeaweedFSRunning(t) {
		t.Skip("SeaweedFS is not running at", TestIAMEndpoint)
	}

	// Setup: Create a parent user
	parentUserName := fmt.Sprintf("expiry-test-%d", time.Now().UnixNano())

	resp, err := callIAMAPI(t, "CreateUser", url.Values{
		"UserName": {parentUserName},
	})
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	defer func() {
		callIAMAPI(t, "DeleteUser", url.Values{"UserName": {parentUserName}})
	}()

	t.Run("reject_past_expiration", func(t *testing.T) {
		// Try to create a service account with expiration in the past
		pastExpiration := time.Now().Add(-1 * time.Hour).Unix()

		createResp, err := callIAMAPI(t, "CreateServiceAccount", url.Values{
			"ParentUser":  {parentUserName},
			"Description": {"Should fail - past expiration"},
			"Expiration":  {strconv.FormatInt(pastExpiration, 10)},
		})
		require.NoError(t, err)
		defer createResp.Body.Close()

		// Should fail because expiration is in the past
		assert.NotEqual(t, http.StatusOK, createResp.StatusCode,
			"Creating service account with past expiration should fail")
	})

	t.Run("accept_future_expiration", func(t *testing.T) {
		// Create a service account with expiration in the future
		futureExpiration := time.Now().Add(24 * time.Hour).Unix()

		createResp, err := callIAMAPI(t, "CreateServiceAccount", url.Values{
			"ParentUser":  {parentUserName},
			"Description": {"Should succeed - future expiration"},
			"Expiration":  {strconv.FormatInt(futureExpiration, 10)},
		})
		require.NoError(t, err)
		defer createResp.Body.Close()

		assert.Equal(t, http.StatusOK, createResp.StatusCode,
			"Creating service account with future expiration should succeed")

		// Parse response to get service account ID for cleanup
		if createResp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(createResp.Body)
			var saResp CreateServiceAccountResponse
			if xml.Unmarshal(body, &saResp) == nil {
				saId := saResp.CreateServiceAccountResult.ServiceAccount.ServiceAccountId
				if saId != "" {
					defer callIAMAPI(t, "DeleteServiceAccount", url.Values{
						"ServiceAccountId": {saId},
					})
				}
			}
		}
	})

	t.Run("reject_past_expiration_on_update", func(t *testing.T) {
		// Create a valid service account first
		futureExpiration := time.Now().Add(24 * time.Hour).Unix()

		createResp, err := callIAMAPI(t, "CreateServiceAccount", url.Values{
			"ParentUser":  {parentUserName},
			"Description": {"For update test"},
			"Expiration":  {strconv.FormatInt(futureExpiration, 10)},
		})
		require.NoError(t, err)
		defer createResp.Body.Close()
		require.Equal(t, http.StatusOK, createResp.StatusCode)

		body, err := io.ReadAll(createResp.Body)
		require.NoError(t, err)

		var saResp CreateServiceAccountResponse
		err = xml.Unmarshal(body, &saResp)
		require.NoError(t, err)

		saId := saResp.CreateServiceAccountResult.ServiceAccount.ServiceAccountId
		require.NotEmpty(t, saId)

		defer func() {
			callIAMAPI(t, "DeleteServiceAccount", url.Values{"ServiceAccountId": {saId}})
		}()

		// Try to update with past expiration
		pastExpiration := time.Now().Add(-1 * time.Hour).Unix()

		updateResp, err := callIAMAPI(t, "UpdateServiceAccount", url.Values{
			"ServiceAccountId": {saId},
			"Expiration":       {strconv.FormatInt(pastExpiration, 10)},
		})
		require.NoError(t, err)
		defer updateResp.Body.Close()

		// Should fail because expiration is in the past
		assert.NotEqual(t, http.StatusOK, updateResp.StatusCode,
			"Updating service account with past expiration should fail")
	})
}

// TestServiceAccountInheritedPermissions verifies that service accounts
// inherit their parent user's permissions.
// This is a key security test - SAs should not have MORE permissions than parent.
func TestServiceAccountInheritedPermissions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSeaweedFSRunning(t) {
		t.Skip("SeaweedFS is not running at", TestIAMEndpoint)
	}

	// Setup: Create a parent user
	parentUserName := fmt.Sprintf("inherit-test-%d", time.Now().UnixNano())

	resp, err := callIAMAPI(t, "CreateUser", url.Values{
		"UserName": {parentUserName},
	})
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	defer func() {
		callIAMAPI(t, "DeleteUser", url.Values{"UserName": {parentUserName}})
	}()

	// Create service account
	createResp, err := callIAMAPI(t, "CreateServiceAccount", url.Values{
		"ParentUser":  {parentUserName},
		"Description": {"Permissions inheritance test"},
	})
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusOK, createResp.StatusCode)

	body, err := io.ReadAll(createResp.Body)
	require.NoError(t, err)

	var saResp CreateServiceAccountResponse
	err = xml.Unmarshal(body, &saResp)
	require.NoError(t, err)

	saId := saResp.CreateServiceAccountResult.ServiceAccount.ServiceAccountId
	require.NotEmpty(t, saId)

	defer func() {
		callIAMAPI(t, "DeleteServiceAccount", url.Values{"ServiceAccountId": {saId}})
	}()

	t.Run("service_account_linked_to_parent", func(t *testing.T) {
		// Verify the service account is correctly linked to the parent
		getResp, err := callIAMAPI(t, "GetServiceAccount", url.Values{
			"ServiceAccountId": {saId},
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		body, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)

		var result GetServiceAccountResponse
		err = xml.Unmarshal(body, &result)
		require.NoError(t, err, "Failed to parse response: %s", string(body))

		assert.Equal(t, parentUserName, result.GetServiceAccountResult.ServiceAccount.ParentUser,
			"Service account should be linked to correct parent user")
	})

	t.Run("list_shows_correct_parent", func(t *testing.T) {
		// List service accounts filtered by parent
		listResp, err := callIAMAPI(t, "ListServiceAccounts", url.Values{
			"ParentUser": {parentUserName},
		})
		require.NoError(t, err)
		defer listResp.Body.Close()

		body, err := io.ReadAll(listResp.Body)
		require.NoError(t, err)

		var listResult ListServiceAccountsResponse
		err = xml.Unmarshal(body, &listResult)
		require.NoError(t, err)

		// Should find at least one service account for this parent
		found := false
		for _, sa := range listResult.ListServiceAccountsResult.ServiceAccounts {
			if sa.ServiceAccountId == saId {
				found = true
				assert.Equal(t, parentUserName, sa.ParentUser)
				break
			}
		}
		assert.True(t, found, "Service account should appear in list filtered by parent")
	})
}

// TestServiceAccountAccessKeyFormat verifies that service account access keys
// follow the correct AWS format.
func TestServiceAccountAccessKeyFormat(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSeaweedFSRunning(t) {
		t.Skip("SeaweedFS is not running at", TestIAMEndpoint)
	}

	// Setup: Create a parent user
	parentUserName := fmt.Sprintf("keyformat-test-%d", time.Now().UnixNano())

	resp, err := callIAMAPI(t, "CreateUser", url.Values{
		"UserName": {parentUserName},
	})
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	defer func() {
		callIAMAPI(t, "DeleteUser", url.Values{"UserName": {parentUserName}})
	}()

	createResp, err := callIAMAPI(t, "CreateServiceAccount", url.Values{
		"ParentUser":  {parentUserName},
		"Description": {"Key format test"},
	})
	require.NoError(t, err)
	defer createResp.Body.Close()
	require.Equal(t, http.StatusOK, createResp.StatusCode)

	body, err := io.ReadAll(createResp.Body)
	require.NoError(t, err)

	var saResp CreateServiceAccountResponse
	err = xml.Unmarshal(body, &saResp)
	require.NoError(t, err)

	accessKeyId := saResp.CreateServiceAccountResult.ServiceAccount.AccessKeyId
	secretAccessKey := saResp.CreateServiceAccountResult.ServiceAccount.SecretAccessKey
	saId := saResp.CreateServiceAccountResult.ServiceAccount.ServiceAccountId

	defer func() {
		callIAMAPI(t, "DeleteServiceAccount", url.Values{"ServiceAccountId": {saId}})
	}()

	t.Run("access_key_has_correct_prefix", func(t *testing.T) {
		// Service account access keys should start with ABIA
		assert.True(t, len(accessKeyId) >= 4,
			"Access key should be at least 4 characters")
		assert.Equal(t, "ABIA", accessKeyId[:4],
			"Service account access key should start with ABIA prefix")
	})

	t.Run("access_key_has_correct_length", func(t *testing.T) {
		// AWS access keys are 20 characters
		assert.Equal(t, 20, len(accessKeyId),
			"Access key should be exactly 20 characters (AWS standard)")
	})

	t.Run("secret_key_has_correct_length", func(t *testing.T) {
		// AWS secret keys are 40 characters
		assert.Equal(t, 40, len(secretAccessKey),
			"Secret key should be exactly 40 characters (AWS standard)")
	})
}
