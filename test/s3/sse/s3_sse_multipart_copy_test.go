package sse_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// TestSSEMultipartCopy tests copying multipart encrypted objects
func TestSSEMultipartCopy(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"sse-multipart-copy-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Generate test data for multipart upload (7.5MB)
	originalData := generateTestData(7*1024*1024 + 512*1024)
	originalMD5 := fmt.Sprintf("%x", md5.Sum(originalData))

	t.Run("Copy SSE-C Multipart Object", func(t *testing.T) {
		testSSECMultipartCopy(t, ctx, client, bucketName, originalData, originalMD5)
	})

	t.Run("Copy SSE-KMS Multipart Object", func(t *testing.T) {
		testSSEKMSMultipartCopy(t, ctx, client, bucketName, originalData, originalMD5)
	})

	t.Run("Copy SSE-C to SSE-KMS", func(t *testing.T) {
		testSSECToSSEKMSCopy(t, ctx, client, bucketName, originalData, originalMD5)
	})

	t.Run("Copy SSE-KMS to SSE-C", func(t *testing.T) {
		testSSEKMSToSSECCopy(t, ctx, client, bucketName, originalData, originalMD5)
	})

	t.Run("Copy SSE-C to Unencrypted", func(t *testing.T) {
		testSSECToUnencryptedCopy(t, ctx, client, bucketName, originalData, originalMD5)
	})

	t.Run("Copy SSE-KMS to Unencrypted", func(t *testing.T) {
		testSSEKMSToUnencryptedCopy(t, ctx, client, bucketName, originalData, originalMD5)
	})
}

// testSSECMultipartCopy tests copying SSE-C multipart objects with same key
func testSSECMultipartCopy(t *testing.T, ctx context.Context, client *s3.Client, bucketName string, originalData []byte, originalMD5 string) {
	sseKey := generateSSECKey()

	// Upload original multipart SSE-C object
	sourceKey := "source-ssec-multipart-object"
	err := uploadMultipartSSECObject(ctx, client, bucketName, sourceKey, originalData, *sseKey)
	require.NoError(t, err, "Failed to upload source SSE-C multipart object")

	// Copy with same SSE-C key
	destKey := "dest-ssec-multipart-object"
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, sourceKey)),
		// Copy source SSE-C headers
		CopySourceSSECustomerAlgorithm: aws.String("AES256"),
		CopySourceSSECustomerKey:       aws.String(sseKey.KeyB64),
		CopySourceSSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		// Destination SSE-C headers (same key)
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(sseKey.KeyB64),
		SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
	})
	require.NoError(t, err, "Failed to copy SSE-C multipart object")

	// Verify copied object
	verifyEncryptedObject(t, ctx, client, bucketName, destKey, originalData, originalMD5, sseKey, nil)
}

// testSSEKMSMultipartCopy tests copying SSE-KMS multipart objects with same key
func testSSEKMSMultipartCopy(t *testing.T, ctx context.Context, client *s3.Client, bucketName string, originalData []byte, originalMD5 string) {
	// Upload original multipart SSE-KMS object
	sourceKey := "source-ssekms-multipart-object"
	err := uploadMultipartSSEKMSObject(ctx, client, bucketName, sourceKey, "test-multipart-key", originalData)
	require.NoError(t, err, "Failed to upload source SSE-KMS multipart object")

	// Copy with same SSE-KMS key
	destKey := "dest-ssekms-multipart-object"
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(destKey),
		CopySource:           aws.String(fmt.Sprintf("%s/%s", bucketName, sourceKey)),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String("test-multipart-key"),
		BucketKeyEnabled:     aws.Bool(false),
	})
	require.NoError(t, err, "Failed to copy SSE-KMS multipart object")

	// Verify copied object
	verifyEncryptedObject(t, ctx, client, bucketName, destKey, originalData, originalMD5, nil, aws.String("test-multipart-key"))
}

// testSSECToSSEKMSCopy tests copying SSE-C multipart objects to SSE-KMS
func testSSECToSSEKMSCopy(t *testing.T, ctx context.Context, client *s3.Client, bucketName string, originalData []byte, originalMD5 string) {
	sseKey := generateSSECKey()

	// Upload original multipart SSE-C object
	sourceKey := "source-ssec-multipart-for-kms"
	err := uploadMultipartSSECObject(ctx, client, bucketName, sourceKey, originalData, *sseKey)
	require.NoError(t, err, "Failed to upload source SSE-C multipart object")

	// Copy to SSE-KMS
	destKey := "dest-ssekms-from-ssec"
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, sourceKey)),
		// Copy source SSE-C headers
		CopySourceSSECustomerAlgorithm: aws.String("AES256"),
		CopySourceSSECustomerKey:       aws.String(sseKey.KeyB64),
		CopySourceSSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		// Destination SSE-KMS headers
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String("test-multipart-key"),
		BucketKeyEnabled:     aws.Bool(false),
	})
	require.NoError(t, err, "Failed to copy SSE-C to SSE-KMS")

	// Verify copied object as SSE-KMS
	verifyEncryptedObject(t, ctx, client, bucketName, destKey, originalData, originalMD5, nil, aws.String("test-multipart-key"))
}

// testSSEKMSToSSECCopy tests copying SSE-KMS multipart objects to SSE-C
func testSSEKMSToSSECCopy(t *testing.T, ctx context.Context, client *s3.Client, bucketName string, originalData []byte, originalMD5 string) {
	sseKey := generateSSECKey()

	// Upload original multipart SSE-KMS object
	sourceKey := "source-ssekms-multipart-for-ssec"
	err := uploadMultipartSSEKMSObject(ctx, client, bucketName, sourceKey, "test-multipart-key", originalData)
	require.NoError(t, err, "Failed to upload source SSE-KMS multipart object")

	// Copy to SSE-C
	destKey := "dest-ssec-from-ssekms"
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, sourceKey)),
		// Destination SSE-C headers
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(sseKey.KeyB64),
		SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
	})
	require.NoError(t, err, "Failed to copy SSE-KMS to SSE-C")

	// Verify copied object as SSE-C
	verifyEncryptedObject(t, ctx, client, bucketName, destKey, originalData, originalMD5, sseKey, nil)
}

// testSSECToUnencryptedCopy tests copying SSE-C multipart objects to unencrypted
func testSSECToUnencryptedCopy(t *testing.T, ctx context.Context, client *s3.Client, bucketName string, originalData []byte, originalMD5 string) {
	sseKey := generateSSECKey()

	// Upload original multipart SSE-C object
	sourceKey := "source-ssec-multipart-for-plain"
	err := uploadMultipartSSECObject(ctx, client, bucketName, sourceKey, originalData, *sseKey)
	require.NoError(t, err, "Failed to upload source SSE-C multipart object")

	// Copy to unencrypted
	destKey := "dest-plain-from-ssec"
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, sourceKey)),
		// Copy source SSE-C headers
		CopySourceSSECustomerAlgorithm: aws.String("AES256"),
		CopySourceSSECustomerKey:       aws.String(sseKey.KeyB64),
		CopySourceSSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		// No destination encryption headers
	})
	require.NoError(t, err, "Failed to copy SSE-C to unencrypted")

	// Verify copied object as unencrypted
	verifyEncryptedObject(t, ctx, client, bucketName, destKey, originalData, originalMD5, nil, nil)
}

// testSSEKMSToUnencryptedCopy tests copying SSE-KMS multipart objects to unencrypted
func testSSEKMSToUnencryptedCopy(t *testing.T, ctx context.Context, client *s3.Client, bucketName string, originalData []byte, originalMD5 string) {
	// Upload original multipart SSE-KMS object
	sourceKey := "source-ssekms-multipart-for-plain"
	err := uploadMultipartSSEKMSObject(ctx, client, bucketName, sourceKey, "test-multipart-key", originalData)
	require.NoError(t, err, "Failed to upload source SSE-KMS multipart object")

	// Copy to unencrypted
	destKey := "dest-plain-from-ssekms"
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, sourceKey)),
		// No destination encryption headers
	})
	require.NoError(t, err, "Failed to copy SSE-KMS to unencrypted")

	// Verify copied object as unencrypted
	verifyEncryptedObject(t, ctx, client, bucketName, destKey, originalData, originalMD5, nil, nil)
}

// uploadMultipartSSECObject uploads a multipart SSE-C object
func uploadMultipartSSECObject(ctx context.Context, client *s3.Client, bucketName, objectKey string, data []byte, sseKey SSECKey) error {
	// Create multipart upload
	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(sseKey.KeyB64),
		SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
	})
	if err != nil {
		return err
	}
	uploadID := aws.ToString(createResp.UploadId)

	// Upload parts
	partSize := 5 * 1024 * 1024 // 5MB
	var completedParts []types.CompletedPart

	for i := 0; i < len(data); i += partSize {
		end := i + partSize
		if end > len(data) {
			end = len(data)
		}

		partNumber := int32(len(completedParts) + 1)
		partResp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			PartNumber:           aws.Int32(partNumber),
			UploadId:             aws.String(uploadID),
			Body:                 bytes.NewReader(data[i:end]),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		if err != nil {
			return err
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       partResp.ETag,
			PartNumber: aws.Int32(partNumber),
		})
	}

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})

	return err
}

// uploadMultipartSSEKMSObject uploads a multipart SSE-KMS object
func uploadMultipartSSEKMSObject(ctx context.Context, client *s3.Client, bucketName, objectKey, keyID string, data []byte) error {
	// Create multipart upload
	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String(keyID),
		BucketKeyEnabled:     aws.Bool(false),
	})
	if err != nil {
		return err
	}
	uploadID := aws.ToString(createResp.UploadId)

	// Upload parts
	partSize := 5 * 1024 * 1024 // 5MB
	var completedParts []types.CompletedPart

	for i := 0; i < len(data); i += partSize {
		end := i + partSize
		if end > len(data) {
			end = len(data)
		}

		partNumber := int32(len(completedParts) + 1)
		partResp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			PartNumber: aws.Int32(partNumber),
			UploadId:   aws.String(uploadID),
			Body:       bytes.NewReader(data[i:end]),
		})
		if err != nil {
			return err
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       partResp.ETag,
			PartNumber: aws.Int32(partNumber),
		})
	}

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})

	return err
}

// verifyEncryptedObject verifies that a copied object can be retrieved and matches the original data
func verifyEncryptedObject(t *testing.T, ctx context.Context, client *s3.Client, bucketName, objectKey string, expectedData []byte, expectedMD5 string, sseKey *SSECKey, kmsKeyID *string) {
	var getInput *s3.GetObjectInput

	if sseKey != nil {
		// SSE-C object
		getInput = &s3.GetObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		}
	} else {
		// SSE-KMS or unencrypted object
		getInput = &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		}
	}

	getResp, err := client.GetObject(ctx, getInput)
	require.NoError(t, err, "Failed to retrieve copied object %s", objectKey)
	defer getResp.Body.Close()

	// Read and verify data
	retrievedData, err := io.ReadAll(getResp.Body)
	require.NoError(t, err, "Failed to read copied object data")

	require.Equal(t, len(expectedData), len(retrievedData), "Data size mismatch for object %s", objectKey)

	// Verify data using MD5
	retrievedMD5 := fmt.Sprintf("%x", md5.Sum(retrievedData))
	require.Equal(t, expectedMD5, retrievedMD5, "Data MD5 mismatch for object %s", objectKey)

	// Verify encryption headers
	if sseKey != nil {
		require.Equal(t, "AES256", aws.ToString(getResp.SSECustomerAlgorithm), "SSE-C algorithm mismatch")
		require.Equal(t, sseKey.KeyMD5, aws.ToString(getResp.SSECustomerKeyMD5), "SSE-C key MD5 mismatch")
	} else if kmsKeyID != nil {
		require.Equal(t, types.ServerSideEncryptionAwsKms, getResp.ServerSideEncryption, "SSE-KMS encryption mismatch")
		require.Contains(t, aws.ToString(getResp.SSEKMSKeyId), *kmsKeyID, "SSE-KMS key ID mismatch")
	}

	t.Logf("Successfully verified copied object %s: %d bytes, MD5=%s", objectKey, len(retrievedData), retrievedMD5)
}
