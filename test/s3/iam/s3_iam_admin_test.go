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

	t.Run("update_user", func(t *testing.T) {
		userName := "user-to-update"
		newUserName := "user-updated"

		// Create user
		_, err := iamClient.CreateUser(&iam.CreateUserInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
		defer func() {
			// Try to delete both just in case
			iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(userName)})
			iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(newUserName)})
		}()

		// Update user name
		_, err = iamClient.UpdateUser(&iam.UpdateUserInput{
			UserName:    aws.String(userName),
			NewUserName: aws.String(newUserName),
		})
		require.NoError(t, err)

		// Verify update (GetUser with NEW name should work)
		getResp, err := iamClient.GetUser(&iam.GetUserInput{
			UserName: aws.String(newUserName),
		})
		require.NoError(t, err)
		assert.Equal(t, newUserName, *getResp.User.UserName)

		// GetUser with OLD name should fail
		_, err = iamClient.GetUser(&iam.GetUserInput{
			UserName: aws.String(userName),
		})
		require.Error(t, err)
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

	t.Run("update_access_key_status", func(t *testing.T) {
		// Create access key
		createResp, err := iamClient.CreateAccessKey(&iam.CreateAccessKeyInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
		defer iamClient.DeleteAccessKey(&iam.DeleteAccessKeyInput{
			UserName:    aws.String(userName),
			AccessKeyId: createResp.AccessKey.AccessKeyId,
		})

		// Update to Inactive
		_, err = iamClient.UpdateAccessKey(&iam.UpdateAccessKeyInput{
			UserName:    aws.String(userName),
			AccessKeyId: createResp.AccessKey.AccessKeyId,
			Status:      aws.String("Inactive"),
		})
		require.NoError(t, err)

		// Verify update in ListAccessKeys
		listResp, err := iamClient.ListAccessKeys(&iam.ListAccessKeysInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
		found := false
		for _, key := range listResp.AccessKeyMetadata {
			if *key.AccessKeyId == *createResp.AccessKey.AccessKeyId {
				assert.Equal(t, "Inactive", *key.Status)
				found = true
				break
			}
		}
		assert.True(t, found)
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

		t.Cleanup(func() {
			_, _ = iamClient.DeletePolicy(&iam.DeletePolicyInput{
				PolicyArn: createResp.Policy.Arn,
			})
		})
	})

	t.Run("managed_policy_crud_lifecycle", func(t *testing.T) {
		policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::*"}]}`

		policyNames := []string{"test-managed-policy-lifecycle-a", "test-managed-policy-lifecycle-b"}
		policyArns := make([]*string, 0, len(policyNames))
		for _, policyName := range policyNames {
			createResp, err := iamClient.CreatePolicy(&iam.CreatePolicyInput{
				PolicyName:     aws.String(policyName),
				PolicyDocument: aws.String(policyDoc),
			})
			require.NoError(t, err)
			policyArns = append(policyArns, createResp.Policy.Arn)
		}

		t.Cleanup(func() {
			for _, policyArn := range policyArns {
				_, _ = iamClient.DeletePolicy(&iam.DeletePolicyInput{PolicyArn: policyArn})
			}
		})

		listResp, err := iamClient.ListPolicies(&iam.ListPoliciesInput{})
		require.NoError(t, err)

		foundByName := map[string]bool{}
		for _, policy := range listResp.Policies {
			if policy.PolicyName != nil {
				foundByName[*policy.PolicyName] = true
			}
		}
		for _, policyName := range policyNames {
			assert.True(t, foundByName[policyName], "policy %s should be listed", policyName)
		}

		getResp, err := iamClient.GetPolicy(&iam.GetPolicyInput{PolicyArn: policyArns[0]})
		require.NoError(t, err)
		require.NotNil(t, getResp.Policy)
		assert.Equal(t, policyNames[0], aws.StringValue(getResp.Policy.PolicyName))
		assert.Equal(t, aws.StringValue(policyArns[0]), aws.StringValue(getResp.Policy.Arn))

		_, err = iamClient.DeletePolicy(&iam.DeletePolicyInput{PolicyArn: policyArns[0]})
		require.NoError(t, err)

		_, err = iamClient.GetPolicy(&iam.GetPolicyInput{PolicyArn: policyArns[0]})
		require.Error(t, err)
		awsErr, ok := err.(awserr.Error)
		require.True(t, ok)
		assert.Equal(t, iam.ErrCodeNoSuchEntityException, awsErr.Code())

		listAfterDeleteResp, err := iamClient.ListPolicies(&iam.ListPoliciesInput{})
		require.NoError(t, err)
		deletedPolicyFound := false
		remainingPolicyFound := false
		for _, policy := range listAfterDeleteResp.Policies {
			if policy.PolicyName == nil {
				continue
			}
			if *policy.PolicyName == policyNames[0] {
				deletedPolicyFound = true
			}
			if *policy.PolicyName == policyNames[1] {
				remainingPolicyFound = true
			}
		}
		assert.False(t, deletedPolicyFound, "deleted policy should no longer be listed")
		assert.True(t, remainingPolicyFound, "remaining policy should still be listed")

		policyArns[0] = nil
	})

	t.Run("managed_policy_versions", func(t *testing.T) {
		policyName := "test-managed-policy-version"
		policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`

		createResp, err := iamClient.CreatePolicy(&iam.CreatePolicyInput{
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(policyDoc),
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			_, _ = iamClient.DeletePolicy(&iam.DeletePolicyInput{PolicyArn: createResp.Policy.Arn})
		})

		listVersionsResp, err := iamClient.ListPolicyVersions(&iam.ListPolicyVersionsInput{
			PolicyArn: createResp.Policy.Arn,
		})
		require.NoError(t, err)
		require.NotEmpty(t, listVersionsResp.Versions)
		assert.Equal(t, "v1", aws.StringValue(listVersionsResp.Versions[0].VersionId))
		assert.Equal(t, true, aws.BoolValue(listVersionsResp.Versions[0].IsDefaultVersion))

		getVersionResp, err := iamClient.GetPolicyVersion(&iam.GetPolicyVersionInput{
			PolicyArn:  createResp.Policy.Arn,
			VersionId:  aws.String("v1"),
		})
		require.NoError(t, err)
		require.NotNil(t, getVersionResp.PolicyVersion)
		assert.Equal(t, "v1", aws.StringValue(getVersionResp.PolicyVersion.VersionId))
		assert.Contains(t, aws.StringValue(getVersionResp.PolicyVersion.Document), "s3:ListBucket")

		_, err = iamClient.GetPolicyVersion(&iam.GetPolicyVersionInput{
			PolicyArn: createResp.Policy.Arn,
			VersionId: aws.String("v2"),
		})
		require.Error(t, err)
		awsErr, ok := err.(awserr.Error)
		require.True(t, ok)
		assert.Equal(t, iam.ErrCodeNoSuchEntityException, awsErr.Code())
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
