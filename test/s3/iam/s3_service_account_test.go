package iam

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Service Account API test constants
const (
	TestIAMEndpoint = "http://localhost:8333"
)

// ServiceAccountInfo represents the response structure for service account operations
type ServiceAccountInfo struct {
	ServiceAccountId string `xml:"ServiceAccountId"`
	ParentUser       string `xml:"ParentUser"`
	Description      string `xml:"Description,omitempty"`
	AccessKeyId      string `xml:"AccessKeyId"`
	SecretAccessKey  string `xml:"SecretAccessKey,omitempty"`
	Status           string `xml:"Status"`
	Expiration       string `xml:"Expiration,omitempty"`
	CreateDate       string `xml:"CreateDate"`
}

// CreateServiceAccountResponse represents the response for CreateServiceAccount
type CreateServiceAccountResponse struct {
	XMLName                    xml.Name `xml:"CreateServiceAccountResponse"`
	CreateServiceAccountResult struct {
		ServiceAccount ServiceAccountInfo `xml:"ServiceAccount"`
	} `xml:"CreateServiceAccountResult"`
}

// ListServiceAccountsResponse represents the response for ListServiceAccounts
type ListServiceAccountsResponse struct {
	XMLName                   xml.Name `xml:"ListServiceAccountsResponse"`
	ListServiceAccountsResult struct {
		ServiceAccounts []ServiceAccountInfo `xml:"ServiceAccounts>member"`
		IsTruncated     bool                 `xml:"IsTruncated"`
	} `xml:"ListServiceAccountsResult"`
}

// GetServiceAccountResponse represents the response for GetServiceAccount
type GetServiceAccountResponse struct {
	XMLName                 xml.Name `xml:"GetServiceAccountResponse"`
	GetServiceAccountResult struct {
		ServiceAccount ServiceAccountInfo `xml:"ServiceAccount"`
	} `xml:"GetServiceAccountResult"`
}

// TestServiceAccountLifecycle tests the complete lifecycle of service accounts
// This is a high-value test covering Create, Get, List, Update, Delete operations
func TestServiceAccountLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Check if SeaweedFS is running
	if !isSeaweedFSRunning(t) {
		t.Skip("SeaweedFS is not running at", TestIAMEndpoint)
	}

	// First, ensure the parent user exists
	parentUserName := fmt.Sprintf("testuser-%d", time.Now().UnixNano())

	t.Run("create_parent_user", func(t *testing.T) {
		resp, err := callIAMAPI(t, "CreateUser", url.Values{
			"UserName": {parentUserName},
		})
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "CreateUser should succeed")
	})

	defer func() {
		// Cleanup: delete the parent user
		resp, _ := callIAMAPI(t, "DeleteUser", url.Values{
			"UserName": {parentUserName},
		})
		if resp != nil {
			resp.Body.Close()
		}
	}()

	var createdSAId string
	var createdAccessKeyId string
	var createdSecretAccessKey string

	t.Run("create_service_account", func(t *testing.T) {
		resp, err := callIAMAPI(t, "CreateServiceAccount", url.Values{
			"ParentUser":  {parentUserName},
			"Description": {"Test service account for CI/CD"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "CreateServiceAccount should succeed")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var createResp CreateServiceAccountResponse
		err = xml.Unmarshal(body, &createResp)
		require.NoError(t, err)

		sa := createResp.CreateServiceAccountResult.ServiceAccount
		assert.NotEmpty(t, sa.ServiceAccountId, "ServiceAccountId should not be empty")
		assert.Equal(t, parentUserName, sa.ParentUser, "ParentUser should match")
		assert.Equal(t, "Test service account for CI/CD", sa.Description)
		assert.Equal(t, "Active", sa.Status)
		assert.NotEmpty(t, sa.AccessKeyId, "AccessKeyId should not be empty")
		assert.NotEmpty(t, sa.SecretAccessKey, "SecretAccessKey should be returned on create")
		assert.True(t, strings.HasPrefix(sa.AccessKeyId, "ABIA"),
			"Service account AccessKeyId should have ABIA prefix")

		createdSAId = sa.ServiceAccountId
		createdAccessKeyId = sa.AccessKeyId
		createdSecretAccessKey = sa.SecretAccessKey

		t.Logf("Created service account: ID=%s, AccessKeyId=%s", createdSAId, createdAccessKeyId)
	})

	t.Run("get_service_account", func(t *testing.T) {
		require.NotEmpty(t, createdSAId, "Service account should have been created")

		resp, err := callIAMAPI(t, "GetServiceAccount", url.Values{
			"ServiceAccountId": {createdSAId},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var getResp GetServiceAccountResponse
		err = xml.Unmarshal(body, &getResp)
		require.NoError(t, err)

		sa := getResp.GetServiceAccountResult.ServiceAccount
		assert.Equal(t, createdSAId, sa.ServiceAccountId)
		assert.Equal(t, parentUserName, sa.ParentUser)
		assert.Empty(t, sa.SecretAccessKey, "SecretAccessKey should not be returned on Get")
	})

	t.Run("list_service_accounts", func(t *testing.T) {
		resp, err := callIAMAPI(t, "ListServiceAccounts", url.Values{
			"ParentUser": {parentUserName},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var listResp ListServiceAccountsResponse
		err = xml.Unmarshal(body, &listResp)
		require.NoError(t, err)

		assert.GreaterOrEqual(t, len(listResp.ListServiceAccountsResult.ServiceAccounts), 1,
			"Should have at least one service account for the parent user")
	})

	t.Run("update_service_account_status", func(t *testing.T) {
		require.NotEmpty(t, createdSAId)

		// Disable the service account
		resp, err := callIAMAPI(t, "UpdateServiceAccount", url.Values{
			"ServiceAccountId": {createdSAId},
			"Status":           {"Inactive"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify it's now inactive
		getResp, err := callIAMAPI(t, "GetServiceAccount", url.Values{
			"ServiceAccountId": {createdSAId},
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		body, _ := io.ReadAll(getResp.Body)
		var result GetServiceAccountResponse
		xml.Unmarshal(body, &result)
		assert.Equal(t, "Inactive", result.GetServiceAccountResult.ServiceAccount.Status)
	})

	t.Run("delete_service_account", func(t *testing.T) {
		require.NotEmpty(t, createdSAId)

		resp, err := callIAMAPI(t, "DeleteServiceAccount", url.Values{
			"ServiceAccountId": {createdSAId},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify it no longer exists
		getResp, err := callIAMAPI(t, "GetServiceAccount", url.Values{
			"ServiceAccountId": {createdSAId},
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		// Should return an error (not found)
		assert.NotEqual(t, http.StatusOK, getResp.StatusCode,
			"GetServiceAccount should fail after deletion")
	})

	// Test the credentials could be used (if service account was active)
	_ = createdAccessKeyId
	_ = createdSecretAccessKey
}

// TestServiceAccountValidation tests validation of service account operations
func TestServiceAccountValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if !isSeaweedFSRunning(t) {
		t.Skip("SeaweedFS is not running at", TestIAMEndpoint)
	}

	t.Run("create_without_parent_user", func(t *testing.T) {
		resp, err := callIAMAPI(t, "CreateServiceAccount", url.Values{
			"Description": {"Test without parent"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"CreateServiceAccount without ParentUser should fail")
	})

	t.Run("create_with_nonexistent_parent", func(t *testing.T) {
		resp, err := callIAMAPI(t, "CreateServiceAccount", url.Values{
			"ParentUser":  {"nonexistent-user-12345"},
			"Description": {"Test with nonexistent parent"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"CreateServiceAccount with nonexistent parent should fail")
	})

	t.Run("get_nonexistent_service_account", func(t *testing.T) {
		resp, err := callIAMAPI(t, "GetServiceAccount", url.Values{
			"ServiceAccountId": {"sa-NONEXISTENT123"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"GetServiceAccount for nonexistent ID should fail")
	})

	t.Run("delete_nonexistent_service_account", func(t *testing.T) {
		resp, err := callIAMAPI(t, "DeleteServiceAccount", url.Values{
			"ServiceAccountId": {"sa-NONEXISTENT123"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEqual(t, http.StatusOK, resp.StatusCode,
			"DeleteServiceAccount for nonexistent ID should fail")
	})
}

// callIAMAPI is a helper to make IAM API calls
func callIAMAPI(t *testing.T, action string, params url.Values) (*http.Response, error) {
	params.Set("Action", action)

	req, err := http.NewRequest(http.MethodPost, TestIAMEndpoint+"/",
		strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}

// isSeaweedFSRunning checks if SeaweedFS S3 API is running
func isSeaweedFSRunning(t *testing.T) bool {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(TestIAMEndpoint + "/status")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
