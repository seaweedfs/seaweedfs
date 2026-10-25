package iam

import (
	"fmt"
	mathrand "math/rand"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// uniqueResourceSuffix returns a lowercased per-test, per-invocation suffix
// safe for use in IAM resource and S3 bucket names. Avoids EntityAlreadyExists
// / BucketAlreadyExists collisions when integration jobs retry or run in
// parallel against a shared stack.
func uniqueResourceSuffix(t *testing.T) string {
	name := strings.ToLower(t.Name())
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, "_", "-")
	return fmt.Sprintf("%s-%d", name, mathrand.Intn(10000))
}

// isAccessDenied returns true when err is an AWS error with code "AccessDenied".
// Used to gate deny-path polling so transient setup errors don't end the
// Eventually loop prematurely.
func isAccessDenied(err error) bool {
	if err == nil {
		return false
	}
	awsErr, ok := err.(awserr.Error)
	return ok && awsErr.Code() == "AccessDenied"
}

// localSourceAllowCIDRs lists every address a loopback-targeted test client may
// present as aws:SourceIp. The SDK reaches the server over localhost, but
// depending on resolver order and host routing the source the server observes
// can be IPv4 loopback, IPv6 loopback, or one of the host's RFC1918 addresses
// (CI runners advertise the S3 endpoint on a 10.x interface). An allow policy
// meant to match "this local client" must cover all of them or the
// positive-condition assertion flakes.
var localSourceAllowCIDRs = []string{
	"127.0.0.0/8",
	"::1/128",
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
}

// sourceIpAllowPolicy builds an Allow policy for s3:* on bucketName gated by an
// aws:SourceIp IpAddress condition over the given CIDRs.
func sourceIpAllowPolicy(bucketName string, cidrs ...string) string {
	quoted := make([]string, len(cidrs))
	for i, c := range cidrs {
		quoted[i] = `"` + c + `"`
	}
	return `{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"Action":"s3:*",
			"Resource":["arn:aws:s3:::` + bucketName + `","arn:aws:s3:::` + bucketName + `/*"],
			"Condition":{"IpAddress":{"aws:SourceIp":[` + strings.Join(quoted, ",") + `]}}
		}]
	}`
}

// TestIAMUserInlinePolicySourceIpCondition verifies that an aws:SourceIp condition
// on a user inline policy is honored. Tests run against a local server, so a
// policy that only allows a non-local CIDR must deny the request, and a policy
// that allows the local client's address (see localSourceAllowCIDRs) must allow it.
func TestIAMUserInlinePolicySourceIpCondition(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	suffix := uniqueResourceSuffix(t)
	userName := "user-" + suffix
	policyName := "policy-" + suffix
	bucketName := "bucket-" + suffix

	_, err = iamClient.CreateUser(&iam.CreateUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)

	keyResp, err := iamClient.CreateAccessKey(&iam.CreateAccessKeyInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	accessKeyId := *keyResp.AccessKey.AccessKeyId
	secretKey := *keyResp.AccessKey.SecretAccessKey

	userS3 := createS3Client(t, accessKeyId, secretKey)

	adminS3, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)
	require.NoError(t, framework.CreateBucketWithCleanup(adminS3, bucketName))

	t.Cleanup(func() {
		if _, err := iamClient.DeleteUserPolicy(&iam.DeleteUserPolicyInput{
			UserName:   aws.String(userName),
			PolicyName: aws.String(policyName),
		}); err != nil {
			t.Logf("cleanup: failed to delete user policy: %v", err)
		}
		if _, err := iamClient.DeleteAccessKey(&iam.DeleteAccessKeyInput{
			UserName:    aws.String(userName),
			AccessKeyId: keyResp.AccessKey.AccessKeyId,
		}); err != nil {
			t.Logf("cleanup: failed to delete access key: %v", err)
		}
		if _, err := iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(userName)}); err != nil {
			t.Logf("cleanup: failed to delete user: %v", err)
		}
	})

	t.Run("denies_when_source_ip_does_not_match", func(t *testing.T) {
		// SourceIp 198.51.100.0/24 is RFC5737 TEST-NET-2; the test client is on
		// the local host, so the condition must fail and the action be denied.
		_, err = iamClient.PutUserPolicy(&iam.PutUserPolicyInput{
			UserName:       aws.String(userName),
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(sourceIpAllowPolicy(bucketName, "198.51.100.0/24")),
		})
		require.NoError(t, err)

		var lastErr error
		require.Eventually(t, func() bool {
			_, lastErr = userS3.PutObject(&s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("denied.txt"),
				Body:   aws.ReadSeekCloser(strings.NewReader("nope")),
			})
			return isAccessDenied(lastErr)
		}, 10*time.Second, 500*time.Millisecond,
			"PutObject must be denied with AccessDenied when aws:SourceIp condition does not match (last error: %v)", lastErr)
	})

	t.Run("allows_when_source_ip_matches", func(t *testing.T) {
		_, err = iamClient.PutUserPolicy(&iam.PutUserPolicyInput{
			UserName:       aws.String(userName),
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(sourceIpAllowPolicy(bucketName, localSourceAllowCIDRs...)),
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, putErr := userS3.PutObject(&s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("allowed.txt"),
				Body:   aws.ReadSeekCloser(strings.NewReader("ok")),
			})
			if putErr != nil {
				t.Logf("allow attempt denied (source IP not in %v?): %v", localSourceAllowCIDRs, putErr)
			}
			return putErr == nil
		}, 10*time.Second, 500*time.Millisecond,
			"PutObject must succeed when aws:SourceIp condition matches the local client address")
	})
}

// TestIAMGroupInlinePolicyEnforcement verifies that PutGroupPolicy is supported
// and that the resulting inline policy is enforced for members of the group,
// including its Condition block.
func TestIAMGroupInlinePolicyEnforcement(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	iamClient, err := framework.CreateIAMClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	suffix := uniqueResourceSuffix(t)
	groupName := "group-" + suffix
	userName := "user-" + suffix
	policyName := "policy-" + suffix
	bucketName := "bucket-" + suffix

	_, err = iamClient.CreateUser(&iam.CreateUserInput{UserName: aws.String(userName)})
	require.NoError(t, err)

	keyResp, err := iamClient.CreateAccessKey(&iam.CreateAccessKeyInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	_, err = iamClient.CreateGroup(&iam.CreateGroupInput{GroupName: aws.String(groupName)})
	require.NoError(t, err)

	_, err = iamClient.AddUserToGroup(&iam.AddUserToGroupInput{
		GroupName: aws.String(groupName),
		UserName:  aws.String(userName),
	})
	require.NoError(t, err)

	userS3 := createS3Client(t, *keyResp.AccessKey.AccessKeyId, *keyResp.AccessKey.SecretAccessKey)

	adminS3, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)
	require.NoError(t, framework.CreateBucketWithCleanup(adminS3, bucketName))

	t.Cleanup(func() {
		if _, err := iamClient.DeleteGroupPolicy(&iam.DeleteGroupPolicyInput{
			GroupName:  aws.String(groupName),
			PolicyName: aws.String(policyName),
		}); err != nil {
			t.Logf("cleanup: failed to delete group policy: %v", err)
		}
		if _, err := iamClient.RemoveUserFromGroup(&iam.RemoveUserFromGroupInput{
			GroupName: aws.String(groupName),
			UserName:  aws.String(userName),
		}); err != nil {
			t.Logf("cleanup: failed to remove user from group: %v", err)
		}
		if _, err := iamClient.DeleteAccessKey(&iam.DeleteAccessKeyInput{
			UserName:    aws.String(userName),
			AccessKeyId: keyResp.AccessKey.AccessKeyId,
		}); err != nil {
			t.Logf("cleanup: failed to delete access key: %v", err)
		}
		if _, err := iamClient.DeleteUser(&iam.DeleteUserInput{UserName: aws.String(userName)}); err != nil {
			t.Logf("cleanup: failed to delete user: %v", err)
		}
		if _, err := iamClient.DeleteGroup(&iam.DeleteGroupInput{GroupName: aws.String(groupName)}); err != nil {
			t.Logf("cleanup: failed to delete group: %v", err)
		}
	})

	allowDoc := sourceIpAllowPolicy(bucketName, localSourceAllowCIDRs...)
	denyDoc := sourceIpAllowPolicy(bucketName, "198.51.100.0/24")

	t.Run("crud_round_trip", func(t *testing.T) {
		_, err := iamClient.PutGroupPolicy(&iam.PutGroupPolicyInput{
			GroupName:      aws.String(groupName),
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(allowDoc),
		})
		require.NoError(t, err, "PutGroupPolicy must succeed (no longer NotImplemented)")

		listResp, err := iamClient.ListGroupPolicies(&iam.ListGroupPoliciesInput{
			GroupName: aws.String(groupName),
		})
		require.NoError(t, err)
		found := false
		for _, name := range listResp.PolicyNames {
			if name != nil && *name == policyName {
				found = true
				break
			}
		}
		assert.True(t, found, "ListGroupPolicies must return the freshly added policy")

		getResp, err := iamClient.GetGroupPolicy(&iam.GetGroupPolicyInput{
			GroupName:  aws.String(groupName),
			PolicyName: aws.String(policyName),
		})
		require.NoError(t, err)
		require.NotNil(t, getResp.PolicyDocument)
		assert.Contains(t, *getResp.PolicyDocument, "aws:SourceIp",
			"GetGroupPolicy must round-trip the Condition block")
	})

	t.Run("enforces_allow_when_condition_matches", func(t *testing.T) {
		_, err := iamClient.PutGroupPolicy(&iam.PutGroupPolicyInput{
			GroupName:      aws.String(groupName),
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(allowDoc),
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, putErr := userS3.PutObject(&s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("group-allowed.txt"),
				Body:   aws.ReadSeekCloser(strings.NewReader("ok")),
			})
			if putErr != nil {
				t.Logf("allow attempt denied (source IP not in %v?): %v", localSourceAllowCIDRs, putErr)
			}
			return putErr == nil
		}, 10*time.Second, 500*time.Millisecond,
			"group member must be allowed when the group policy condition matches")
	})

	t.Run("enforces_deny_when_condition_does_not_match", func(t *testing.T) {
		_, err := iamClient.PutGroupPolicy(&iam.PutGroupPolicyInput{
			GroupName:      aws.String(groupName),
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(denyDoc),
		})
		require.NoError(t, err)

		var lastErr error
		require.Eventually(t, func() bool {
			_, lastErr = userS3.PutObject(&s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String("group-denied.txt"),
				Body:   aws.ReadSeekCloser(strings.NewReader("nope")),
			})
			return isAccessDenied(lastErr)
		}, 10*time.Second, 500*time.Millisecond,
			"group member must be denied with AccessDenied when the group policy condition does not match (last error: %v)", lastErr)
	})
}
