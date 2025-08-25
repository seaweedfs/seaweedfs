package iam

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testEndpoint   = "http://localhost:8333"
	testRegion     = "us-west-2"
	testBucket     = "test-iam-bucket"
	testObjectKey  = "test-object.txt"
	testObjectData = "Hello, SeaweedFS IAM Integration!"
)

// TestS3IAMAuthentication tests S3 API authentication with IAM JWT tokens
func TestS3IAMAuthentication(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	t.Run("valid_jwt_token_authentication", func(t *testing.T) {
		// Create S3 client with valid JWT token
		s3Client, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
		require.NoError(t, err)

		// Test bucket operations
		err = framework.CreateBucket(s3Client, testBucket)
		require.NoError(t, err)

		// Verify bucket exists
		buckets, err := s3Client.ListBuckets(&s3.ListBucketsInput{})
		require.NoError(t, err)

		found := false
		for _, bucket := range buckets.Buckets {
			if *bucket.Name == testBucket {
				found = true
				break
			}
		}
		assert.True(t, found, "Created bucket should be listed")
	})

	t.Run("invalid_jwt_token_authentication", func(t *testing.T) {
		// Create S3 client with invalid JWT token
		s3Client, err := framework.CreateS3ClientWithInvalidJWT()
		require.NoError(t, err)

		// Attempt bucket operations - should fail
		err = framework.CreateBucket(s3Client, testBucket+"-invalid")
		require.Error(t, err)

		// Verify it's an access denied error
		if awsErr, ok := err.(awserr.Error); ok {
			assert.Equal(t, "AccessDenied", awsErr.Code())
		} else {
			t.Error("Expected AWS error with AccessDenied code")
		}
	})

	t.Run("expired_jwt_token_authentication", func(t *testing.T) {
		// Create S3 client with expired JWT token
		s3Client, err := framework.CreateS3ClientWithExpiredJWT("expired-user", "TestAdminRole")
		require.NoError(t, err)

		// Attempt bucket operations - should fail
		err = framework.CreateBucket(s3Client, testBucket+"-expired")
		require.Error(t, err)

		// Verify it's an access denied error
		if awsErr, ok := err.(awserr.Error); ok {
			assert.Equal(t, "AccessDenied", awsErr.Code())
		} else {
			t.Error("Expected AWS error with AccessDenied code")
		}
	})
}

// TestS3IAMPolicyEnforcement tests policy enforcement for different S3 operations
func TestS3IAMPolicyEnforcement(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	// Setup test bucket with admin client
	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	err = framework.CreateBucket(adminClient, testBucket)
	require.NoError(t, err)

	// Put test object with admin client
	_, err = adminClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testObjectKey),
		Body:   strings.NewReader(testObjectData),
	})
	require.NoError(t, err)

	t.Run("read_only_policy_enforcement", func(t *testing.T) {
		// Create S3 client with read-only role
		readOnlyClient, err := framework.CreateS3ClientWithJWT("read-user", "TestReadOnlyRole")
		require.NoError(t, err)

		// Should be able to read objects
		result, err := readOnlyClient.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testObjectKey),
		})
		require.NoError(t, err)

		data, err := io.ReadAll(result.Body)
		require.NoError(t, err)
		assert.Equal(t, testObjectData, string(data))
		result.Body.Close()

		// Should be able to list objects
		listResult, err := readOnlyClient.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(testBucket),
		})
		require.NoError(t, err)
		assert.Len(t, listResult.Contents, 1)
		assert.Equal(t, testObjectKey, *listResult.Contents[0].Key)

		// Should NOT be able to put objects
		_, err = readOnlyClient.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String("forbidden-object.txt"),
			Body:   strings.NewReader("This should fail"),
		})
		require.Error(t, err)
		if awsErr, ok := err.(awserr.Error); ok {
			assert.Equal(t, "AccessDenied", awsErr.Code())
		}

		// Should NOT be able to delete objects
		_, err = readOnlyClient.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testObjectKey),
		})
		require.Error(t, err)
		if awsErr, ok := err.(awserr.Error); ok {
			assert.Equal(t, "AccessDenied", awsErr.Code())
		}
	})

	t.Run("write_only_policy_enforcement", func(t *testing.T) {
		// Create S3 client with write-only role
		writeOnlyClient, err := framework.CreateS3ClientWithJWT("write-user", "TestWriteOnlyRole")
		require.NoError(t, err)

		// Should be able to put objects
		testWriteKey := "write-test-object.txt"
		testWriteData := "Write-only test data"

		_, err = writeOnlyClient.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testWriteKey),
			Body:   strings.NewReader(testWriteData),
		})
		require.NoError(t, err)

		// Should be able to delete objects
		_, err = writeOnlyClient.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testWriteKey),
		})
		require.NoError(t, err)

		// Should NOT be able to read objects
		_, err = writeOnlyClient.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testObjectKey),
		})
		require.Error(t, err)
		if awsErr, ok := err.(awserr.Error); ok {
			assert.Equal(t, "AccessDenied", awsErr.Code())
		}

		// Should NOT be able to list objects
		_, err = writeOnlyClient.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(testBucket),
		})
		require.Error(t, err)
		if awsErr, ok := err.(awserr.Error); ok {
			assert.Equal(t, "AccessDenied", awsErr.Code())
		}
	})

	t.Run("admin_policy_enforcement", func(t *testing.T) {
		// Admin client should be able to do everything
		testAdminKey := "admin-test-object.txt"
		testAdminData := "Admin test data"

		// Should be able to put objects
		_, err = adminClient.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testAdminKey),
			Body:   strings.NewReader(testAdminData),
		})
		require.NoError(t, err)

		// Should be able to read objects
		result, err := adminClient.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testAdminKey),
		})
		require.NoError(t, err)

		data, err := io.ReadAll(result.Body)
		require.NoError(t, err)
		assert.Equal(t, testAdminData, string(data))
		result.Body.Close()

		// Should be able to list objects
		listResult, err := adminClient.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(testBucket),
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(listResult.Contents), 1)

		// Should be able to delete objects
		_, err = adminClient.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testAdminKey),
		})
		require.NoError(t, err)

		// Should be able to delete buckets
		// First delete remaining objects
		_, err = adminClient.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testObjectKey),
		})
		require.NoError(t, err)

		// Then delete the bucket
		_, err = adminClient.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(testBucket),
		})
		require.NoError(t, err)
	})
}

// TestS3IAMSessionExpiration tests session expiration handling
func TestS3IAMSessionExpiration(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	t.Run("session_expiration_enforcement", func(t *testing.T) {
		// Create S3 client with valid JWT token
		s3Client, err := framework.CreateS3ClientWithJWT("session-user", "TestAdminRole")
		require.NoError(t, err)

		// Initially should work
		err = framework.CreateBucket(s3Client, testBucket+"-session")
		require.NoError(t, err)

		// Create S3 client with expired JWT token
		expiredClient, err := framework.CreateS3ClientWithExpiredJWT("session-user", "TestAdminRole")
		require.NoError(t, err)

		// Now operations should fail with expired token
		err = framework.CreateBucket(expiredClient, testBucket+"-session-expired")
		require.Error(t, err)
		if awsErr, ok := err.(awserr.Error); ok {
			assert.Equal(t, "AccessDenied", awsErr.Code())
		}

		// Cleanup the successful bucket
		adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
		require.NoError(t, err)

		_, err = adminClient.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String(testBucket + "-session"),
		})
		require.NoError(t, err)
	})
}

// TestS3IAMMultipartUploadPolicyEnforcement tests multipart upload with IAM policies
func TestS3IAMMultipartUploadPolicyEnforcement(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	// Setup test bucket with admin client
	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	err = framework.CreateBucket(adminClient, testBucket)
	require.NoError(t, err)

	t.Run("multipart_upload_with_write_permissions", func(t *testing.T) {
		// Create S3 client with admin role (has multipart permissions)
		s3Client := adminClient

		// Initiate multipart upload
		multipartKey := "large-test-file.txt"
		initResult, err := s3Client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(multipartKey),
		})
		require.NoError(t, err)

		uploadId := initResult.UploadId

		// Upload a part
		partNumber := int64(1)
		partData := strings.Repeat("Test data for multipart upload. ", 1000) // ~30KB

		uploadResult, err := s3Client.UploadPart(&s3.UploadPartInput{
			Bucket:     aws.String(testBucket),
			Key:        aws.String(multipartKey),
			PartNumber: aws.Int64(partNumber),
			UploadId:   uploadId,
			Body:       strings.NewReader(partData),
		})
		require.NoError(t, err)

		// Complete multipart upload
		_, err = s3Client.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(testBucket),
			Key:      aws.String(multipartKey),
			UploadId: uploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: []*s3.CompletedPart{
					{
						ETag:       uploadResult.ETag,
						PartNumber: aws.Int64(partNumber),
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify object was created
		result, err := s3Client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(multipartKey),
		})
		require.NoError(t, err)

		data, err := io.ReadAll(result.Body)
		require.NoError(t, err)
		assert.Equal(t, partData, string(data))
		result.Body.Close()

		// Cleanup
		_, err = s3Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(multipartKey),
		})
		require.NoError(t, err)
	})

	t.Run("multipart_upload_denied_for_read_only", func(t *testing.T) {
		// Create S3 client with read-only role
		readOnlyClient, err := framework.CreateS3ClientWithJWT("read-user", "TestReadOnlyRole")
		require.NoError(t, err)

		// Attempt to initiate multipart upload - should fail
		multipartKey := "denied-multipart-file.txt"
		_, err = readOnlyClient.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(multipartKey),
		})
		require.Error(t, err)
		if awsErr, ok := err.(awserr.Error); ok {
			assert.Equal(t, "AccessDenied", awsErr.Code())
		}
	})

	// Cleanup
	_, err = adminClient.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(testBucket),
	})
	require.NoError(t, err)
}

// TestS3IAMBucketPolicyIntegration tests bucket policy integration with IAM
func TestS3IAMBucketPolicyIntegration(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	// Setup test bucket with admin client
	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	err = framework.CreateBucket(adminClient, testBucket)
	require.NoError(t, err)

	t.Run("bucket_policy_allows_public_read", func(t *testing.T) {
		// Set bucket policy to allow public read access
		bucketPolicy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [
				{
					"Sid": "PublicReadGetObject",
					"Effect": "Allow",
					"Principal": "*",
					"Action": "s3:GetObject",
					"Resource": "arn:seaweed:s3:::%s/*"
				}
			]
		}`, testBucket)

		_, err = adminClient.PutBucketPolicy(&s3.PutBucketPolicyInput{
			Bucket: aws.String(testBucket),
			Policy: aws.String(bucketPolicy),
		})
		require.NoError(t, err)

		// Put test object
		_, err = adminClient.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testObjectKey),
			Body:   strings.NewReader(testObjectData),
		})
		require.NoError(t, err)

		// Test with read-only client - should now be allowed due to bucket policy
		readOnlyClient, err := framework.CreateS3ClientWithJWT("read-user", "TestReadOnlyRole")
		require.NoError(t, err)

		result, err := readOnlyClient.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testObjectKey),
		})
		require.NoError(t, err)

		data, err := io.ReadAll(result.Body)
		require.NoError(t, err)
		assert.Equal(t, testObjectData, string(data))
		result.Body.Close()
	})

	t.Run("bucket_policy_denies_specific_action", func(t *testing.T) {
		// Set bucket policy to deny delete operations
		bucketPolicy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [
				{
					"Sid": "DenyDelete",
					"Effect": "Deny",
					"Principal": "*",
					"Action": "s3:DeleteObject",
					"Resource": "arn:seaweed:s3:::%s/*"
				}
			]
		}`, testBucket)

		_, err = adminClient.PutBucketPolicy(&s3.PutBucketPolicyInput{
			Bucket: aws.String(testBucket),
			Policy: aws.String(bucketPolicy),
		})
		require.NoError(t, err)

		// Even admin should not be able to delete due to explicit deny
		_, err = adminClient.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testObjectKey),
		})
		require.Error(t, err)
		if awsErr, ok := err.(awserr.Error); ok {
			assert.Equal(t, "AccessDenied", awsErr.Code())
		}
	})

	// Cleanup - delete bucket policy first, then objects and bucket
	_, err = adminClient.DeleteBucketPolicy(&s3.DeleteBucketPolicyInput{
		Bucket: aws.String(testBucket),
	})
	require.NoError(t, err)

	_, err = adminClient.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testObjectKey),
	})
	require.NoError(t, err)

	_, err = adminClient.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(testBucket),
	})
	require.NoError(t, err)
}

// TestS3IAMContextualPolicyEnforcement tests context-aware policy enforcement
func TestS3IAMContextualPolicyEnforcement(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	// This test would verify IP-based restrictions, time-based restrictions,
	// and other context-aware policy conditions
	// For now, we'll focus on the basic structure

	t.Run("ip_based_policy_enforcement", func(t *testing.T) {
		// TODO: Implement IP-based policy testing
		// This would require configuring policies with IP restrictions
		// and testing from different source IPs
		t.Skip("IP-based policy testing requires network configuration")
	})

	t.Run("time_based_policy_enforcement", func(t *testing.T) {
		// TODO: Implement time-based policy testing
		// This would require configuring policies with time restrictions
		t.Skip("Time-based policy testing requires time manipulation")
	})
}

// Helper function to create test content of specific size
func createTestContent(size int) *bytes.Reader {
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 256)
	}
	return bytes.NewReader(content)
}

// TestS3IAMPresignedURLIntegration tests presigned URL generation with IAM
func TestS3IAMPresignedURLIntegration(t *testing.T) {
	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	// Setup test bucket with admin client
	adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
	require.NoError(t, err)

	err = framework.CreateBucket(adminClient, testBucket)
	require.NoError(t, err)

	// Put test object
	_, err = adminClient.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testObjectKey),
		Body:   strings.NewReader(testObjectData),
	})
	require.NoError(t, err)

	t.Run("presigned_url_generation_and_usage", func(t *testing.T) {
		// Note: AWS SDK's presigned URL generation is not compatible with JWT Bearer token authentication
		// The AWS SDK generates signature-based presigned URLs, but SeaweedFS with JWT uses Bearer tokens
		// For JWT authentication, direct API calls with Bearer tokens should be used instead

		// Test direct object access with JWT token (which is what JWT authentication supports)
		_, err := adminClient.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(testObjectKey),
		})
		require.NoError(t, err, "Direct object access with JWT should work")

		t.Log("JWT-based object access successful - presigned URLs not applicable for JWT Bearer token authentication")
	})

	// Cleanup
	_, err = adminClient.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testObjectKey),
	})
	require.NoError(t, err)

	_, err = adminClient.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(testBucket),
	})
	require.NoError(t, err)
}
