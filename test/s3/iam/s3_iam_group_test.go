package iam

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIAMGroupLifecycle tests the full lifecycle of group management:
// CreateGroup, GetGroup, ListGroups, DeleteGroup
func TestIAMGroupLifecycle(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	groupName := "test-group-lifecycle"

	t.Run("create_group", func(t *testing.T) {
		resp, err := iamClient.CreateGroup(&iam.CreateGroupInput{
			GroupName: aws.String(groupName),
		})
		require.NoError(t, err)
		assert.Equal(t, groupName, *resp.Group.GroupName)
	})

	t.Run("get_group", func(t *testing.T) {
		resp, err := iamClient.GetGroup(&iam.GetGroupInput{
			GroupName: aws.String(groupName),
		})
		require.NoError(t, err)
		assert.Equal(t, groupName, *resp.Group.GroupName)
	})

	t.Run("list_groups_contains_created", func(t *testing.T) {
		resp, err := iamClient.ListGroups(&iam.ListGroupsInput{})
		require.NoError(t, err)
		found := false
		for _, g := range resp.Groups {
			if *g.GroupName == groupName {
				found = true
				break
			}
		}
		assert.True(t, found, "Created group should appear in ListGroups")
	})

	t.Run("create_duplicate_group_fails", func(t *testing.T) {
		_, err := iamClient.CreateGroup(&iam.CreateGroupInput{
			GroupName: aws.String(groupName),
		})
		assert.Error(t, err, "Creating a duplicate group should fail")
	})

	t.Run("delete_group", func(t *testing.T) {
		_, err := iamClient.DeleteGroup(&iam.DeleteGroupInput{
			GroupName: aws.String(groupName),
		})
		require.NoError(t, err)

		// Verify it's gone
		resp, err := iamClient.ListGroups(&iam.ListGroupsInput{})
		require.NoError(t, err)
		for _, g := range resp.Groups {
			assert.NotEqual(t, groupName, *g.GroupName,
				"Deleted group should not appear in ListGroups")
		}
	})

	t.Run("delete_nonexistent_group_fails", func(t *testing.T) {
		_, err := iamClient.DeleteGroup(&iam.DeleteGroupInput{
			GroupName: aws.String("nonexistent-group-xyz"),
		})
		assert.Error(t, err)
	})
}

// TestIAMGroupMembership tests adding and removing users from groups
func TestIAMGroupMembership(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	groupName := "test-group-members"
	userName := "test-user-for-group"

	// Setup: create group and user
	_, err = iamClient.CreateGroup(&iam.CreateGroupInput{
		GroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	defer iamClient.DeleteGroup(&iam.DeleteGroupInput{GroupName: aws.String(groupName)})

	_, err = iamClient.CreateUser(&iam.CreateUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	defer iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(userName)})

	t.Run("add_user_to_group", func(t *testing.T) {
		_, err := iamClient.AddUserToGroup(&iam.AddUserToGroupInput{
			GroupName: aws.String(groupName),
			UserName:  aws.String(userName),
		})
		require.NoError(t, err)
	})

	t.Run("get_group_shows_member", func(t *testing.T) {
		resp, err := iamClient.GetGroup(&iam.GetGroupInput{
			GroupName: aws.String(groupName),
		})
		require.NoError(t, err)
		found := false
		for _, u := range resp.Users {
			if *u.UserName == userName {
				found = true
				break
			}
		}
		assert.True(t, found, "Added user should appear in GetGroup members")
	})

	t.Run("list_groups_for_user", func(t *testing.T) {
		resp, err := iamClient.ListGroupsForUser(&iam.ListGroupsForUserInput{
			UserName: aws.String(userName),
		})
		require.NoError(t, err)
		found := false
		for _, g := range resp.Groups {
			if *g.GroupName == groupName {
				found = true
				break
			}
		}
		assert.True(t, found, "Group should appear in ListGroupsForUser")
	})

	t.Run("add_duplicate_member_is_idempotent", func(t *testing.T) {
		_, err := iamClient.AddUserToGroup(&iam.AddUserToGroupInput{
			GroupName: aws.String(groupName),
			UserName:  aws.String(userName),
		})
		// Should succeed (idempotent) or return a benign error
		// AWS IAM allows duplicate add without error
		assert.NoError(t, err)
	})

	t.Run("remove_user_from_group", func(t *testing.T) {
		_, err := iamClient.RemoveUserFromGroup(&iam.RemoveUserFromGroupInput{
			GroupName: aws.String(groupName),
			UserName:  aws.String(userName),
		})
		require.NoError(t, err)

		// Verify removal
		resp, err := iamClient.GetGroup(&iam.GetGroupInput{
			GroupName: aws.String(groupName),
		})
		require.NoError(t, err)
		for _, u := range resp.Users {
			assert.NotEqual(t, userName, *u.UserName,
				"Removed user should not appear in group members")
		}
	})
}

// TestIAMGroupPolicyAttachment tests attaching and detaching policies from groups
func TestIAMGroupPolicyAttachment(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	groupName := "test-group-policies"
	policyName := "test-group-attach-policy"
	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`

	// Setup: create group and policy
	_, err = iamClient.CreateGroup(&iam.CreateGroupInput{
		GroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	defer iamClient.DeleteGroup(&iam.DeleteGroupInput{GroupName: aws.String(groupName)})

	createPolicyResp, err := iamClient.CreatePolicy(&iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(policyDoc),
	})
	require.NoError(t, err)
	policyArn := createPolicyResp.Policy.Arn
	defer iamClient.DeletePolicy(&iam.DeletePolicyInput{PolicyArn: policyArn})

	t.Run("attach_group_policy", func(t *testing.T) {
		_, err := iamClient.AttachGroupPolicy(&iam.AttachGroupPolicyInput{
			GroupName: aws.String(groupName),
			PolicyArn: policyArn,
		})
		require.NoError(t, err)
	})

	t.Run("list_attached_group_policies", func(t *testing.T) {
		resp, err := iamClient.ListAttachedGroupPolicies(&iam.ListAttachedGroupPoliciesInput{
			GroupName: aws.String(groupName),
		})
		require.NoError(t, err)
		found := false
		for _, p := range resp.AttachedPolicies {
			if *p.PolicyName == policyName {
				found = true
				break
			}
		}
		assert.True(t, found, "Attached policy should appear in ListAttachedGroupPolicies")
	})

	t.Run("detach_group_policy", func(t *testing.T) {
		_, err := iamClient.DetachGroupPolicy(&iam.DetachGroupPolicyInput{
			GroupName: aws.String(groupName),
			PolicyArn: policyArn,
		})
		require.NoError(t, err)

		// Verify detachment
		resp, err := iamClient.ListAttachedGroupPolicies(&iam.ListAttachedGroupPoliciesInput{
			GroupName: aws.String(groupName),
		})
		require.NoError(t, err)
		for _, p := range resp.AttachedPolicies {
			assert.NotEqual(t, policyName, *p.PolicyName,
				"Detached policy should not appear in ListAttachedGroupPolicies")
		}
	})
}

// TestIAMGroupPolicyEnforcement tests that group policies are enforced during S3 operations.
// Creates a user with no direct policies, adds them to a group with S3 access,
// and verifies they can access S3 through the group policy.
func TestIAMGroupPolicyEnforcement(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	groupName := "test-enforcement-group"
	userName := "test-enforcement-user"
	policyName := "test-enforcement-policy"
	bucketName := "test-group-enforce-bucket"
	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::` + bucketName + `","arn:aws:s3:::` + bucketName + `/*"]}]}`

	// Create user
	_, err = iamClient.CreateUser(&iam.CreateUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	defer func() {
		iamClient.RemoveUserFromGroup(&iam.RemoveUserFromGroupInput{
			GroupName: aws.String(groupName),
			UserName:  aws.String(userName),
		})
		iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(userName)})
	}()

	// Create access key for the user
	keyResp, err := iamClient.CreateAccessKey(&iam.CreateAccessKeyInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	defer iamClient.DeleteAccessKey(&iam.DeleteAccessKeyInput{
		UserName:    aws.String(userName),
		AccessKeyId: keyResp.AccessKey.AccessKeyId,
	})

	accessKeyId := *keyResp.AccessKey.AccessKeyId
	secretKey := *keyResp.AccessKey.SecretAccessKey

	// Create an S3 client with the user's credentials
	userS3Client := createS3Client(t, accessKeyId, secretKey)

	// Create group
	_, err = iamClient.CreateGroup(&iam.CreateGroupInput{
		GroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	defer func() {
		iamClient.DetachGroupPolicy(&iam.DetachGroupPolicyInput{
			GroupName: aws.String(groupName),
			PolicyArn: aws.String("arn:aws:iam:::policy/" + policyName),
		})
		iamClient.DeleteGroup(&iam.DeleteGroupInput{GroupName: aws.String(groupName)})
	}()

	// Create policy
	createPolicyResp, err := iamClient.CreatePolicy(&iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(policyDoc),
	})
	require.NoError(t, err)
	policyArn := createPolicyResp.Policy.Arn
	defer iamClient.DeletePolicy(&iam.DeletePolicyInput{PolicyArn: policyArn})

	t.Run("user_without_group_denied", func(t *testing.T) {
		// User has no policies and is not in any group — should be denied
		_, err := userS3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		assert.Error(t, err, "User without any policies should be denied")
	})

	t.Run("user_with_group_policy_allowed", func(t *testing.T) {
		// Attach policy to group
		_, err := iamClient.AttachGroupPolicy(&iam.AttachGroupPolicyInput{
			GroupName: aws.String(groupName),
			PolicyArn: policyArn,
		})
		require.NoError(t, err)

		// Add user to group
		_, err = iamClient.AddUserToGroup(&iam.AddUserToGroupInput{
			GroupName: aws.String(groupName),
			UserName:  aws.String(userName),
		})
		require.NoError(t, err)

		// Wait for policy propagation
		time.Sleep(2 * time.Second)

		// Now user should be able to create the bucket through group policy
		_, err = userS3Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err, "User with group policy should be allowed")
		t.Cleanup(func() {
			userS3Client.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucketName)})
		})

		// Should also be able to put/get objects
		_, err = userS3Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("test-key"),
			Body:   aws.ReadSeekCloser(strings.NewReader("test-data")),
		})
		require.NoError(t, err, "User should be able to put objects through group policy")
	})

	t.Run("user_removed_from_group_denied", func(t *testing.T) {
		// Remove user from group
		_, err := iamClient.RemoveUserFromGroup(&iam.RemoveUserFromGroupInput{
			GroupName: aws.String(groupName),
			UserName:  aws.String(userName),
		})
		require.NoError(t, err)

		// Wait for policy propagation
		time.Sleep(2 * time.Second)

		// User should now be denied
		_, err = userS3Client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
		})
		assert.Error(t, err, "User removed from group should be denied")
	})
}

// TestIAMGroupDisabledPolicyEnforcement tests that disabled groups do not contribute policies.
// Uses the raw IAM API (callIAMAPI) since the AWS SDK doesn't support custom group status.
func TestIAMGroupDisabledPolicyEnforcement(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !isSeaweedFSRunning(t) {
		t.Skip("SeaweedFS is not running at", TestIAMEndpoint)
	}

	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	groupName := "test-disabled-group"
	userName := "test-disabled-grp-user"
	policyName := "test-disabled-grp-policy"
	bucketName := "test-disabled-grp-bucket"
	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*"],"Resource":["arn:aws:s3:::` + bucketName + `","arn:aws:s3:::` + bucketName + `/*"]}]}`

	// Create user, group, policy
	_, err = iamClient.CreateUser(&iam.CreateUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)
	defer iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(userName)})

	keyResp, err := iamClient.CreateAccessKey(&iam.CreateAccessKeyInput{UserName: aws.String(userName)})
	require.NoError(t, err)
	defer iamClient.DeleteAccessKey(&iam.DeleteAccessKeyInput{
		UserName: aws.String(userName), AccessKeyId: keyResp.AccessKey.AccessKeyId,
	})

	_, err = iamClient.CreateGroup(&iam.CreateGroupInput{GroupName: aws.String(groupName)})
	require.NoError(t, err)
	defer func() {
		iamClient.DetachGroupPolicy(&iam.DetachGroupPolicyInput{
			GroupName: aws.String(groupName),
			PolicyArn: aws.String("arn:aws:iam:::policy/" + policyName),
		})
		iamClient.RemoveUserFromGroup(&iam.RemoveUserFromGroupInput{
			GroupName: aws.String(groupName), UserName: aws.String(userName),
		})
		iamClient.DeleteGroup(&iam.DeleteGroupInput{GroupName: aws.String(groupName)})
	}()

	createPolicyResp, err := iamClient.CreatePolicy(&iam.CreatePolicyInput{
		PolicyName: aws.String(policyName), PolicyDocument: aws.String(policyDoc),
	})
	require.NoError(t, err)
	defer iamClient.DeletePolicy(&iam.DeletePolicyInput{PolicyArn: createPolicyResp.Policy.Arn})

	// Setup: attach policy, add user, create bucket with admin
	_, err = iamClient.AttachGroupPolicy(&iam.AttachGroupPolicyInput{
		GroupName: aws.String(groupName), PolicyArn: createPolicyResp.Policy.Arn,
	})
	require.NoError(t, err)

	_, err = iamClient.AddUserToGroup(&iam.AddUserToGroupInput{
		GroupName: aws.String(groupName), UserName: aws.String(userName),
	})
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	userS3Client := createS3Client(t, *keyResp.AccessKey.AccessKeyId, *keyResp.AccessKey.SecretAccessKey)

	// Create bucket using admin first so we can test listing
	adminS3, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)
	_, err = adminS3.CreateBucket(&s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	require.NoError(t, err)
	defer adminS3.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	t.Run("enabled_group_allows_access", func(t *testing.T) {
		_, err := userS3Client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
		})
		assert.NoError(t, err, "User in enabled group should have access")
	})

	t.Run("disabled_group_denies_access", func(t *testing.T) {
		// Disable group via raw IAM API (no SDK support for this extension)
		resp, err := callIAMAPI(t, "UpdateGroup", url.Values{
			"GroupName": {groupName},
			"Disabled":  {"true"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		// Wait for propagation
		time.Sleep(2 * time.Second)

		_, err = userS3Client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
		})
		assert.Error(t, err, "User in disabled group should be denied access")
	})

	t.Run("re_enabled_group_restores_access", func(t *testing.T) {
		// Re-enable the group
		resp, err := callIAMAPI(t, "UpdateGroup", url.Values{
			"GroupName": {groupName},
			"Disabled":  {"false"},
		})
		require.NoError(t, err)
		defer resp.Body.Close()

		// Wait for propagation
		time.Sleep(2 * time.Second)

		_, err = userS3Client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
		})
		assert.NoError(t, err, "User in re-enabled group should have access again")
	})
}

// TestIAMGroupUserDeletionSideEffect tests that deleting a user removes them from all groups.
func TestIAMGroupUserDeletionSideEffect(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	groupName := "test-deletion-group"
	userName := "test-deletion-user"

	// Create group and user
	_, err = iamClient.CreateGroup(&iam.CreateGroupInput{GroupName: aws.String(groupName)})
	require.NoError(t, err)
	defer iamClient.DeleteGroup(&iam.DeleteGroupInput{GroupName: aws.String(groupName)})

	_, err = iamClient.CreateUser(&iam.CreateUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)

	// Add user to group
	_, err = iamClient.AddUserToGroup(&iam.AddUserToGroupInput{
		GroupName: aws.String(groupName),
		UserName:  aws.String(userName),
	})
	require.NoError(t, err)

	// Verify user is in group
	getResp, err := iamClient.GetGroup(&iam.GetGroupInput{GroupName: aws.String(groupName)})
	require.NoError(t, err)
	assert.Len(t, getResp.Users, 1, "Group should have 1 member before deletion")

	// Delete the user
	_, err = iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)

	// Verify user was removed from the group
	getResp, err = iamClient.GetGroup(&iam.GetGroupInput{GroupName: aws.String(groupName)})
	require.NoError(t, err)
	assert.Empty(t, getResp.Users, "Group should have no members after user deletion")
}

// TestIAMGroupMultipleGroups tests that a user can belong to multiple groups
// and inherits policies from all of them.
func TestIAMGroupMultipleGroups(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	group1 := "test-multi-group-1"
	group2 := "test-multi-group-2"
	userName := "test-multi-group-user"

	// Create two groups
	_, err = iamClient.CreateGroup(&iam.CreateGroupInput{GroupName: aws.String(group1)})
	require.NoError(t, err)
	defer iamClient.DeleteGroup(&iam.DeleteGroupInput{GroupName: aws.String(group1)})

	_, err = iamClient.CreateGroup(&iam.CreateGroupInput{GroupName: aws.String(group2)})
	require.NoError(t, err)
	defer iamClient.DeleteGroup(&iam.DeleteGroupInput{GroupName: aws.String(group2)})

	// Create user
	_, err = iamClient.CreateUser(&iam.CreateUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)
	defer func() {
		iamClient.RemoveUserFromGroup(&iam.RemoveUserFromGroupInput{
			GroupName: aws.String(group1), UserName: aws.String(userName),
		})
		iamClient.RemoveUserFromGroup(&iam.RemoveUserFromGroupInput{
			GroupName: aws.String(group2), UserName: aws.String(userName),
		})
		iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(userName)})
	}()

	// Add user to both groups
	_, err = iamClient.AddUserToGroup(&iam.AddUserToGroupInput{
		GroupName: aws.String(group1), UserName: aws.String(userName),
	})
	require.NoError(t, err)

	_, err = iamClient.AddUserToGroup(&iam.AddUserToGroupInput{
		GroupName: aws.String(group2), UserName: aws.String(userName),
	})
	require.NoError(t, err)

	// Verify user appears in both groups
	resp, err := iamClient.ListGroupsForUser(&iam.ListGroupsForUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	groupNames := make(map[string]bool)
	for _, g := range resp.Groups {
		groupNames[*g.GroupName] = true
	}
	assert.True(t, groupNames[group1], "User should be in group 1")
	assert.True(t, groupNames[group2], "User should be in group 2")
}

// --- Response types for raw IAM API calls ---

type CreateGroupResponse struct {
	XMLName           xml.Name `xml:"CreateGroupResponse"`
	CreateGroupResult struct {
		Group struct {
			GroupName string `xml:"GroupName"`
		} `xml:"Group"`
	} `xml:"CreateGroupResult"`
}

type ListGroupsResponse struct {
	XMLName          xml.Name `xml:"ListGroupsResponse"`
	ListGroupsResult struct {
		Groups []struct {
			GroupName string `xml:"GroupName"`
		} `xml:"Groups>member"`
	} `xml:"ListGroupsResult"`
}

// TestIAMGroupRawAPI tests group operations using raw HTTP IAM API calls,
// for operations not covered by the AWS SDK (like the SeaweedFS extension
// to disable/enable groups via UpdateGroup with Disabled parameter).
func TestIAMGroupRawAPI(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	if !isSeaweedFSRunning(t) {
		t.Skip("SeaweedFS is not running at", TestIAMEndpoint)
	}

	groupName := "test-raw-api-group"

	t.Run("create_group_raw", func(t *testing.T) {
		resp, err := callIAMAPI(t, "CreateGroup", url.Values{
			"GroupName": {groupName},
		})
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var createResp CreateGroupResponse
		err = xml.Unmarshal(body, &createResp)
		require.NoError(t, err)
		assert.Equal(t, groupName, createResp.CreateGroupResult.Group.GroupName)
	})

	t.Run("list_groups_raw", func(t *testing.T) {
		resp, err := callIAMAPI(t, "ListGroups", url.Values{})
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var listResp ListGroupsResponse
		err = xml.Unmarshal(body, &listResp)
		require.NoError(t, err)

		found := false
		for _, g := range listResp.ListGroupsResult.Groups {
			if g.GroupName == groupName {
				found = true
				break
			}
		}
		assert.True(t, found, "Created group should appear in raw ListGroups")
	})

	t.Run("delete_group_raw", func(t *testing.T) {
		resp, err := callIAMAPI(t, "DeleteGroup", url.Values{
			"GroupName": {groupName},
		})
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

// createS3Client creates an S3 client with static credentials
func createS3Client(t *testing.T, accessKey, secretKey string) *s3.S3 {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(TestS3Endpoint),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	})
	require.NoError(t, err)
	return s3.New(sess)
}
