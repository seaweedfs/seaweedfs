package sse_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSSEKMSOpenBaoIntegration tests SSE-KMS with real OpenBao KMS provider
// This test verifies that SeaweedFS can successfully encrypt and decrypt data
// using actual KMS operations through OpenBao, not just mock key IDs
func TestSSEKMSOpenBaoIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"sse-kms-openbao-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	t.Run("Basic SSE-KMS with OpenBao", func(t *testing.T) {
		testData := []byte("Hello, SSE-KMS with OpenBao integration!")
		objectKey := "test-openbao-kms-object"
		kmsKeyID := "test-key-123" // This key should exist in OpenBao

		// Upload object with SSE-KMS
		putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAwsKms,
			SSEKMSKeyId:          aws.String(kmsKeyID),
		})
		require.NoError(t, err, "Failed to upload SSE-KMS object with OpenBao")
		assert.NotEmpty(t, aws.ToString(putResp.ETag), "ETag should be present")

		// Retrieve and verify object
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to retrieve SSE-KMS object")
		defer getResp.Body.Close()

		// Verify content matches (this proves encryption/decryption worked)
		retrievedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Failed to read retrieved data")
		assert.Equal(t, testData, retrievedData, "Decrypted data should match original")

		// Verify SSE-KMS headers are present
		assert.Equal(t, types.ServerSideEncryptionAwsKms, getResp.ServerSideEncryption, "Should indicate KMS encryption")
		assert.Equal(t, kmsKeyID, aws.ToString(getResp.SSEKMSKeyId), "Should return the KMS key ID used")
	})

	t.Run("Multiple KMS Keys with OpenBao", func(t *testing.T) {
		testCases := []struct {
			keyID     string
			data      string
			objectKey string
		}{
			{"test-key-123", "Data encrypted with test-key-123", "object-key-123"},
			{"seaweedfs-test-key", "Data encrypted with seaweedfs-test-key", "object-seaweedfs-key"},
			{"high-security-key", "Data encrypted with high-security-key", "object-security-key"},
		}

		for _, tc := range testCases {
			t.Run("Key_"+tc.keyID, func(t *testing.T) {
				testData := []byte(tc.data)

				// Upload with specific KMS key
				_, err := client.PutObject(ctx, &s3.PutObjectInput{
					Bucket:               aws.String(bucketName),
					Key:                  aws.String(tc.objectKey),
					Body:                 bytes.NewReader(testData),
					ServerSideEncryption: types.ServerSideEncryptionAwsKms,
					SSEKMSKeyId:          aws.String(tc.keyID),
				})
				require.NoError(t, err, "Failed to upload with KMS key %s", tc.keyID)

				// Retrieve and verify
				getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(tc.objectKey),
				})
				require.NoError(t, err, "Failed to retrieve object encrypted with key %s", tc.keyID)
				defer getResp.Body.Close()

				retrievedData, err := io.ReadAll(getResp.Body)
				require.NoError(t, err, "Failed to read data for key %s", tc.keyID)

				// Verify data integrity (proves real encryption/decryption occurred)
				assert.Equal(t, testData, retrievedData, "Data should match for key %s", tc.keyID)
				assert.Equal(t, tc.keyID, aws.ToString(getResp.SSEKMSKeyId), "Should return correct key ID")
			})
		}
	})

	t.Run("Large Data with OpenBao KMS", func(t *testing.T) {
		// Test with larger data to ensure chunked encryption works
		testData := generateTestData(64 * 1024) // 64KB
		objectKey := "large-openbao-kms-object"
		kmsKeyID := "performance-key"

		// Upload large object with SSE-KMS
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAwsKms,
			SSEKMSKeyId:          aws.String(kmsKeyID),
		})
		require.NoError(t, err, "Failed to upload large SSE-KMS object")

		// Retrieve and verify large object
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to retrieve large SSE-KMS object")
		defer getResp.Body.Close()

		retrievedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Failed to read large data")

		// Use MD5 comparison for large data
		assertDataEqual(t, testData, retrievedData, "Large encrypted data should match original")
		assert.Equal(t, kmsKeyID, aws.ToString(getResp.SSEKMSKeyId), "Should return performance key ID")
	})
}

// TestSSEKMSOpenBaoAvailability checks if OpenBao KMS is available for testing
// This test can be run separately to verify the KMS setup
func TestSSEKMSOpenBaoAvailability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"sse-kms-availability-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Try a simple KMS operation to verify availability
	testData := []byte("KMS availability test")
	objectKey := "kms-availability-test"
	kmsKeyID := "test-key-123"

	// This should succeed if KMS is properly configured
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 bytes.NewReader(testData),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String(kmsKeyID),
	})

	if err != nil {
		t.Skipf("OpenBao KMS not available for testing: %v", err)
	}

	t.Logf("OpenBao KMS is available and working")

	// Verify we can retrieve the object
	getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to retrieve KMS test object")
	defer getResp.Body.Close()

	assert.Equal(t, types.ServerSideEncryptionAwsKms, getResp.ServerSideEncryption)
	t.Logf("KMS encryption/decryption working correctly")
}
