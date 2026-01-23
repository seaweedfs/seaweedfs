package iam

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIAMUserManagement tests user management operations
func TestIAMUserManagement(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	// Create IAM client with admin privileges
	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	t.Run("create_and_get_user", func(t *testing.T) {
		userName := "test-user-mgm"

		// Create user
		createResp, err := iamClient.CreateUser(&iam.CreateUserInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
		assert.Equal(t, userName, *createResp.User.UserName)

		// Get user
		getResp, err := iamClient.GetUser(&iam.GetUserInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
		assert.Equal(t, userName, *getResp.User.UserName)

		// List users to verify existence
		listResp, err := iamClient.ListUsers(&iam.ListUsersInput{})
		require.NoError(t, err)
		found := false
		for _, user := range listResp.Users {
			if *user.UserName == userName {
				found = true
				break
			}
		}
		assert.True(t, found, "Created user should be listed")

		// Clean up
		_, err = iamClient.DeleteUser(&iam.DeleteUserInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
	})

	t.Run("delete_non_existent_user", func(t *testing.T) {
		_, err := iamClient.DeleteUser(&iam.DeleteUserInput{
			UserName: aws.String("non-existent-user"),
		})
		require.Error(t, err)
		if awsErr, ok := err.(awserr.Error); ok {
			assert.Equal(t, iam.ErrCodeNoSuchEntityException, awsErr.Code())
		}
	})
}

// TestIAMAccessKeyManagement tests access key operations
func TestIAMAccessKeyManagement(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	userName := "test-user-keys"
	_, err = iamClient.CreateUser(&iam.CreateUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	defer iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(userName)})

	t.Run("create_list_delete_access_key", func(t *testing.T) {
		// Create access key
		createResp, err := iamClient.CreateAccessKey(&iam.CreateAccessKeyInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, *createResp.AccessKey.AccessKeyId)
		assert.NotEmpty(t, *createResp.AccessKey.SecretAccessKey)
		assert.Equal(t, "Active", *createResp.AccessKey.Status)

		// List access keys
		listResp, err := iamClient.ListAccessKeys(&iam.ListAccessKeysInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
		assert.Equal(t, 1, len(listResp.AccessKeyMetadata))
		assert.Equal(t, *createResp.AccessKey.AccessKeyId, *listResp.AccessKeyMetadata[0].AccessKeyId)

		// Delete access key
		_, err = iamClient.DeleteAccessKey(&iam.DeleteAccessKeyInput{
			UserName:    aws.String(userName),
			AccessKeyId: createResp.AccessKey.AccessKeyId,
		})
		require.NoError(t, err)

		// Verify deletion
		listResp, err = iamClient.ListAccessKeys(&iam.ListAccessKeysInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
		assert.Equal(t, 0, len(listResp.AccessKeyMetadata))
	})
}

// TestIAMPolicyManagement tests policy operations
func TestIAMPolicyManagement(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	t.Run("create_managed_policy", func(t *testing.T) {
		policyName := "test-managed-policy"
		policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`

		createResp, err := iamClient.CreatePolicy(&iam.CreatePolicyInput{
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(policyDoc),
		})
		require.NoError(t, err)
		assert.Equal(t, policyName, *createResp.Policy.PolicyName)
		assert.NotEmpty(t, *createResp.Policy.Arn)
	})

	t.Run("user_inline_policy", func(t *testing.T) {
		userName := "test-user-policy"
		_, err := iamClient.CreateUser(&iam.CreateUserInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
		defer iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(userName)})

		policyName := "test-inline-policy"
		policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::*"}]}`

		// Put user policy
		_, err = iamClient.PutUserPolicy(&iam.PutUserPolicyInput{
			UserName:       aws.String(userName),
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(policyDoc),
		})
		require.NoError(t, err)

		// Get user policy
		getResp, err := iamClient.GetUserPolicy(&iam.GetUserPolicyInput{
			UserName:   aws.String(userName),
			PolicyName: aws.String(policyName),
		})
		require.NoError(t, err)
		assert.Equal(t, userName, *getResp.UserName)
		assert.Equal(t, policyName, *getResp.PolicyName)
		assert.Contains(t, *getResp.PolicyDocument, "s3:Get")

		// Delete user policy
		_, err = iamClient.DeleteUserPolicy(&iam.DeleteUserPolicyInput{
			UserName:   aws.String(userName),
			PolicyName: aws.String(policyName),
		})
		require.NoError(t, err)
	})
}
