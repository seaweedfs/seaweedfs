package iam

import (
	"fmt"
	"strings"
	"testing"
	"time"

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
			"Resource": ["arn:aws:s3:::%s/${aws:username}/*"]
		}, {
			"Sid": "DenyOthers",
			"Effect": "Deny",
			"Principal": "*",
			"Action": ["s3:GetObject", "s3:PutObject"],
			"NotResource": ["arn:aws:s3:::%s/${aws:username}/*"]
		}]
	}`, bucketName, bucketName)

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

	// Test Enforcement: Alice should be able to write to her own folder
	aliceClient, err := framework.CreateS3ClientWithJWT("alice", "TestReadOnlyRole")
	require.NoError(t, err)

	_, err = aliceClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("alice/file.txt"),
		Body:   nil, // Empty body is fine for this test
	})
	assert.NoError(t, err, "Alice should be allowed to put to alice/file.txt")

	// Test Enforcement: Alice should NOT be able to write to bob's folder
	_, err = aliceClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("bob/file.txt"),
		Body:   nil,
	})
	assert.Error(t, err, "Alice should be denied put to bob/file.txt")
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
			"Action": ["s3:GetObject", "s3:PutObject"],
			"Resource": ["arn:aws:s3:::%s/${aws:username}/*"]
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

	// Test Enforcement: Alice should be able to write to her own folder
	aliceClient, err := framework.CreateS3ClientWithJWT("alice", "TestReadOnlyRole")
	require.NoError(t, err)

	_, err = aliceClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("alice/file.txt"),
		Body:   nil, // Empty body is fine for this test
	})
	assert.NoError(t, err, "Alice should be allowed to put to alice/file.txt")

	// Test Enforcement: Alice should NOT be able to write to bob's folder
	_, err = aliceClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("bob/file.txt"),
		Body:   nil,
	})
	assert.Error(t, err, "Alice should be denied put to bob/file.txt")
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
			"Action": ["s3:GetObject"],
			"Resource": ["arn:aws:s3:::%s/${jwt:preferred_username}/*"]
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

func TestS3PolicyVariablesUsernameIsolation(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	bucketName := framework.GenerateUniqueBucketName("test-isolation")
	err = framework.CreateBucket(adminClient, bucketName)
	require.NoError(t, err)
	defer adminClient.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	bucketPolicy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Sid": "AllowOwnFolder",
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:GetObject", "s3:PutObject"],
			"Resource": "arn:aws:s3:::%s/${aws:username}/*"
		}, {
			"Sid": "AllowListOwnPrefix",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:ListBucket",
			"Resource": "arn:aws:s3:::%s",
			"Condition": {
				"StringLike": {
					"s3:prefix": ["${aws:username}/*", "${aws:username}"]
				}
			}
		}, {
			"Sid": "DenyOtherFolders",
			"Effect": "Deny",
			"Principal": "*",
			"Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
			"NotResource": "arn:aws:s3:::%s/${aws:username}/*"
		}]
	}`, bucketName, bucketName, bucketName)

	_, err = adminClient.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(bucketPolicy),
	})
	require.NoError(t, err)

	// Wait for policy to propagate (fix race condition)
	time.Sleep(2 * time.Second)

	aliceClient, err := framework.CreateS3ClientWithJWT("alice", "TestReadOnlyRole")
	require.NoError(t, err)

	bobClient, err := framework.CreateS3ClientWithJWT("bob", "TestReadOnlyRole")
	require.NoError(t, err)

	_, err = aliceClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("alice/data.txt"),
		Body:   strings.NewReader("Alice Private Data"),
	})
	assert.NoError(t, err, "Alice should be able to upload to her own folder")

	_, err = aliceClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("bob/data.txt"),
		Body:   strings.NewReader("Alice Intrusion"),
	})
	assert.Error(t, err, "Alice should be denied access to Bob's folder")

	_, err = bobClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("bob/data.txt"),
		Body:   strings.NewReader("Bob Private Data"),
	})
	assert.NoError(t, err, "Bob should be able to upload to his own folder")

	_, err = bobClient.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("alice/data.txt"),
	})
	assert.Error(t, err, "Bob should be denied access to Alice's folder")

	listAlice, err := aliceClient.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("alice/"),
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(listAlice.Contents))

	_, err = aliceClient.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("bob/"),
	})
	assert.Error(t, err, "Alice should be denied listing Bob's folder")
}

func TestS3PolicyVariablesAccountEnforcement(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	bucketName := framework.GenerateUniqueBucketName("test-account")
	err = framework.CreateBucket(adminClient, bucketName)
	require.NoError(t, err)
	defer adminClient.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	bucketPolicy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Deny",
			"Principal": "*",
			"Action": ["s3:*"],
			"Resource": ["arn:aws:s3:::%s/*"],
			"Condition": {
				"StringNotEquals": {
					"aws:PrincipalAccount": ["999988887777"]
				}
			}
		}, {
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:*"],
			"Resource": ["arn:aws:s3:::%s/*"]
		}]
	}`, bucketName, bucketName)

	_, err = adminClient.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(bucketPolicy),
	})
	require.NoError(t, err)

	authorizedClient, err := framework.CreateS3ClientWithCustomClaims("user1", "TestAdminRole", "999988887777", nil)
	require.NoError(t, err)

	unauthorizedClient, err := framework.CreateS3ClientWithCustomClaims("user2", "TestAdminRole", "111122223333", nil)
	require.NoError(t, err)

	_, err = authorizedClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("test.txt"),
		Body:   strings.NewReader("Authorized Data"),
	})
	assert.NoError(t, err, "Authorized account should be able to upload")

	_, err = unauthorizedClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("fail.txt"),
		Body:   strings.NewReader("Unauthorized Data"),
	})
	assert.Error(t, err, "Unauthorized account should be denied")
}

func TestS3PolicyVariablesJWTPreferredUsername(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	bucketName := framework.GenerateUniqueBucketName("test-jwt-claim")
	err = framework.CreateBucket(adminClient, bucketName)
	require.NoError(t, err)
	defer adminClient.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	bucketPolicy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Sid": "AllowOwnFolder",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:PutObject",
			"Resource": "arn:aws:s3:::%s/${jwt:preferred_username}/*"
		}, {
			"Sid": "DenyOtherFolders",
			"Effect": "Deny",
			"Principal": "*",
			"Action": "s3:PutObject",
			"NotResource": "arn:aws:s3:::%s/${jwt:preferred_username}/*"
		}]
	}`, bucketName, bucketName)

	_, err = adminClient.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(bucketPolicy),
	})
	require.NoError(t, err)

	claims := map[string]interface{}{
		"preferred_username": "jdoe",
	}
	client, err := framework.CreateS3ClientWithCustomClaims("jdoe", "TestReadOnlyRole", "", claims)
	require.NoError(t, err)

	_, err = client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("jdoe/file.txt"),
		Body:   strings.NewReader("JWT Claim Data"),
	})
	assert.NoError(t, err, "Should allow access based on jwt:preferred_username")

	_, err = client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("other/file.txt"),
		Body:   strings.NewReader("JWT Claim Data"),
	})
	assert.Error(t, err, "Should deny access if prefix doesn't match jwt:preferred_username")
}

func TestS3PolicyVariablesLDAPClaims(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	bucketName := framework.GenerateUniqueBucketName("test-ldap-claim")
	err = framework.CreateBucket(adminClient, bucketName)
	require.NoError(t, err)
	defer adminClient.DeleteBucket(&s3.DeleteBucketInput{Bucket: aws.String(bucketName)})

	bucketPolicy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:PutObject"],
			"Resource": ["arn:aws:s3:::%s/${ldap:username}/*"]
		}, {
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:GetObject"],
			"Resource": ["arn:aws:s3:::%s/*"],
			"Condition": {
				"StringEquals": {
					"ldap:dn": ["cn=manager,dc=example,dc=org"]
				}
			}
		}]
	}`, bucketName, bucketName)

	_, err = adminClient.PutBucketPolicy(&s3.PutBucketPolicyInput{
		Bucket: aws.String(bucketName),
		Policy: aws.String(bucketPolicy),
	})
	require.NoError(t, err)

	claims := map[string]interface{}{
		"ldap:username": "manager",
		"ldap:dn":       "cn=manager,dc=example,dc=org",
	}
	client, err := framework.CreateS3ClientWithCustomClaims("manager", "TestReadOnlyRole", "", claims)
	require.NoError(t, err)

	_, err = client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("manager/data.txt"),
		Body:   strings.NewReader("LDAP Upload"),
	})
	assert.NoError(t, err)

	_, err = client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("manager/data.txt"),
	})
	assert.NoError(t, err, "Should allow download based on ldap:dn condition")
}
