package iam

import (
"fmt"
"strings"
"testing"

"github.com/aws/aws-sdk-go/aws"
"github.com/aws/aws-sdk-go/service/s3"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

// TestS3PolicyVariablesUsernameInResource tests ${aws:username} in resource paths
func TestS3PolicyVariablesUsernameInResource(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	bucketName := framework.GenerateUniqueBucketName("test-policy-vars")
	err = framework.CreateBucket(adminClient, bucketName)
	require.NoError(t, err)
	defer adminClient.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Policy with ${aws:username} in resource
	bucketPolicy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:GetObject", "s3:PutObject"],
			"Resource": "arn:aws:s3:::%s/${aws:username}/*"
		}]
	}`, bucketName)

	_, err = adminClient.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(bucketPolicy),
	})
	require.NoError(t, err)

	// Verify policy contains variable
	policyResult, err := adminClient.GetBucketPolicy(&s3.GetBucketPolicyInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Contains(t, *policyResult.Policy, "${aws:username}")
}

// TestS3PolicyVariablesUsernameInCondition tests ${aws:username} in conditions
func TestS3PolicyVariablesUsernameInCondition(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	bucketName := framework.GenerateUniqueBucketName("test-policy-cond")
	err = framework.CreateBucket(adminClient, bucketName)
	require.NoError(t, err)
	defer adminClient.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Policy with variable in condition
	bucketPolicy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:ListBucket",
			"Resource": "arn:aws:s3:::%s",
			"Condition": {
				"StringLike": {
					"s3:prefix": ["${aws:username}/*"]
				}
			}
		}]
	}`, bucketName)

	_, err = adminClient.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(bucketPolicy),
	})
	require.NoError(t, err)

	policyResult, err := adminClient.GetBucketPolicy(&s3.GetBucketPolicyInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Contains(t, *policyResult.Policy, "${aws:username}")
}

// TestS3PolicyVariablesJWTClaims tests ${jwt:*} variables
func TestS3PolicyVariablesJWTClaims(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	bucketName := framework.GenerateUniqueBucketName("test-policy-jwt")
	err = framework.CreateBucket(adminClient, bucketName)
	require.NoError(t, err)
	defer adminClient.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	// Policy with JWT claim variable
	bucketPolicy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::%s/${jwt:preferred_username}/*"
		}]
	}`, bucketName)

	_, err = adminClient.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(bucketPolicy),
	})
	require.NoError(t, err)

	policyResult, err := adminClient.GetBucketPolicy(&s3.GetBucketPolicyInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Contains(t, *policyResult.Policy, "jwt:preferred_username")
}
