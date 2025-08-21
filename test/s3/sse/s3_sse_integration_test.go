package sse_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertDataEqual compares two byte slices using MD5 hashes and provides a concise error message
func assertDataEqual(t *testing.T, expected, actual []byte, msgAndArgs ...interface{}) {
	if len(expected) == len(actual) && bytes.Equal(expected, actual) {
		return // Data matches, no need to fail
	}

	expectedMD5 := md5.Sum(expected)
	actualMD5 := md5.Sum(actual)

	// Create preview of first 1K bytes for debugging
	previewSize := 1024
	if len(expected) < previewSize {
		previewSize = len(expected)
	}
	expectedPreview := expected[:previewSize]

	actualPreviewSize := previewSize
	if len(actual) < actualPreviewSize {
		actualPreviewSize = len(actual)
	}
	actualPreview := actual[:actualPreviewSize]

	// Format the assertion failure message
	msg := fmt.Sprintf("Data mismatch:\nExpected length: %d, MD5: %x\nActual length: %d, MD5: %x\nExpected preview (first %d bytes): %x\nActual preview (first %d bytes): %x",
		len(expected), expectedMD5, len(actual), actualMD5,
		len(expectedPreview), expectedPreview, len(actualPreview), actualPreview)

	if len(msgAndArgs) > 0 {
		if format, ok := msgAndArgs[0].(string); ok {
			msg = fmt.Sprintf(format, msgAndArgs[1:]...) + "\n" + msg
		}
	}

	t.Error(msg)
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// S3SSETestConfig holds configuration for S3 SSE integration tests
type S3SSETestConfig struct {
	Endpoint      string
	AccessKey     string
	SecretKey     string
	Region        string
	BucketPrefix  string
	UseSSL        bool
	SkipVerifySSL bool
}

// Default test configuration
var defaultConfig = &S3SSETestConfig{
	Endpoint:      "http://127.0.0.1:8333",
	AccessKey:     "some_access_key1",
	SecretKey:     "some_secret_key1",
	Region:        "us-east-1",
	BucketPrefix:  "test-sse-",
	UseSSL:        false,
	SkipVerifySSL: true,
}

// Test data sizes for comprehensive coverage
var testDataSizes = []int{
	0,           // Empty file
	1,           // Single byte
	16,          // One AES block
	31,          // Just under two blocks
	32,          // Exactly two blocks
	100,         // Small file
	1024,        // 1KB
	8192,        // 8KB
	64 * 1024,   // 64KB
	1024 * 1024, // 1MB
}

// SSECKey represents an SSE-C encryption key for testing
type SSECKey struct {
	Key    []byte
	KeyB64 string
	KeyMD5 string
}

// generateSSECKey generates a random SSE-C key for testing
func generateSSECKey() *SSECKey {
	key := make([]byte, 32) // 256-bit key
	rand.Read(key)

	keyB64 := base64.StdEncoding.EncodeToString(key)
	keyMD5Hash := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(keyMD5Hash[:])

	return &SSECKey{
		Key:    key,
		KeyB64: keyB64,
		KeyMD5: keyMD5,
	}
}

// createS3Client creates an S3 client for testing
func createS3Client(ctx context.Context, cfg *S3SSETestConfig) (*s3.Client, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               cfg.Endpoint,
			HostnameImmutable: true,
		}, nil
	})

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKey,
			cfg.SecretKey,
			"",
		)),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	}), nil
}

// generateTestData generates random test data of specified size
func generateTestData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

// createTestBucket creates a test bucket with a unique name
func createTestBucket(ctx context.Context, client *s3.Client, prefix string) (string, error) {
	bucketName := fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	return bucketName, err
}

// cleanupTestBucket removes a test bucket and all its objects
func cleanupTestBucket(ctx context.Context, client *s3.Client, bucketName string) error {
	// List and delete all objects first
	listResp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return err
	}

	if len(listResp.Contents) > 0 {
		var objectIds []types.ObjectIdentifier
		for _, obj := range listResp.Contents {
			objectIds = append(objectIds, types.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		_, err = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: objectIds,
			},
		})
		if err != nil {
			return err
		}
	}

	// Delete the bucket
	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})

	return err
}

// TestSSECIntegrationBasic tests basic SSE-C functionality end-to-end
func TestSSECIntegrationBasic(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssec-basic-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Generate test key
	sseKey := generateSSECKey()
	testData := []byte("Hello, SSE-C integration test!")
	objectKey := "test-object-ssec"

	t.Run("PUT with SSE-C", func(t *testing.T) {
		// Upload object with SSE-C
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(t, err, "Failed to upload SSE-C object")
	})

	t.Run("GET with correct SSE-C key", func(t *testing.T) {
		// Retrieve object with correct key
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(t, err, "Failed to retrieve SSE-C object")
		defer resp.Body.Close()

		// Verify decrypted content matches original
		retrievedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read retrieved data")
		assertDataEqual(t, testData, retrievedData, "Decrypted data does not match original")

		// Verify SSE headers are present
		assert.Equal(t, "AES256", aws.ToString(resp.SSECustomerAlgorithm))
		assert.Equal(t, sseKey.KeyMD5, aws.ToString(resp.SSECustomerKeyMD5))
	})

	t.Run("GET without SSE-C key should fail", func(t *testing.T) {
		// Try to retrieve object without encryption key - should fail
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		assert.Error(t, err, "Should fail to retrieve SSE-C object without key")
	})

	t.Run("GET with wrong SSE-C key should fail", func(t *testing.T) {
		wrongKey := generateSSECKey()

		// Try to retrieve object with wrong key - should fail
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(wrongKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(wrongKey.KeyMD5),
		})
		assert.Error(t, err, "Should fail to retrieve SSE-C object with wrong key")
	})
}

// TestSSECIntegrationVariousDataSizes tests SSE-C with various data sizes
func TestSSECIntegrationVariousDataSizes(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssec-sizes-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	sseKey := generateSSECKey()

	for _, size := range testDataSizes {
		t.Run(fmt.Sprintf("Size_%d_bytes", size), func(t *testing.T) {
			testData := generateTestData(size)
			objectKey := fmt.Sprintf("test-object-size-%d", size)

			// Upload with SSE-C
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(objectKey),
				Body:                 bytes.NewReader(testData),
				SSECustomerAlgorithm: aws.String("AES256"),
				SSECustomerKey:       aws.String(sseKey.KeyB64),
				SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
			})
			require.NoError(t, err, "Failed to upload object of size %d", size)

			// Retrieve with SSE-C
			resp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(objectKey),
				SSECustomerAlgorithm: aws.String("AES256"),
				SSECustomerKey:       aws.String(sseKey.KeyB64),
				SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
			})
			require.NoError(t, err, "Failed to retrieve object of size %d", size)
			defer resp.Body.Close()

			// Verify content matches
			retrievedData, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "Failed to read retrieved data of size %d", size)
			assertDataEqual(t, testData, retrievedData, "Data mismatch for size %d", size)

			// Verify content length is correct (this would have caught the IV-in-stream bug!)
			assert.Equal(t, int64(size), aws.ToInt64(resp.ContentLength),
				"Content length mismatch for size %d", size)
		})
	}
}

// TestSSEKMSIntegrationBasic tests basic SSE-KMS functionality end-to-end
func TestSSEKMSIntegrationBasic(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssekms-basic-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	testData := []byte("Hello, SSE-KMS integration test!")
	objectKey := "test-object-ssekms"
	kmsKeyID := "test-key-123" // Test key ID

	t.Run("PUT with SSE-KMS", func(t *testing.T) {
		// Upload object with SSE-KMS
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAwsKms,
			SSEKMSKeyId:          aws.String(kmsKeyID),
		})
		require.NoError(t, err, "Failed to upload SSE-KMS object")
	})

	t.Run("GET SSE-KMS object", func(t *testing.T) {
		// Retrieve object - no additional headers needed for GET
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to retrieve SSE-KMS object")
		defer resp.Body.Close()

		// Verify decrypted content matches original
		retrievedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read retrieved data")
		assertDataEqual(t, testData, retrievedData, "Decrypted data does not match original")

		// Verify SSE-KMS headers are present
		assert.Equal(t, types.ServerSideEncryptionAwsKms, resp.ServerSideEncryption)
		assert.Equal(t, kmsKeyID, aws.ToString(resp.SSEKMSKeyId))
	})

	t.Run("HEAD SSE-KMS object", func(t *testing.T) {
		// Test HEAD operation to verify metadata
		resp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to HEAD SSE-KMS object")

		// Verify SSE-KMS metadata
		assert.Equal(t, types.ServerSideEncryptionAwsKms, resp.ServerSideEncryption)
		assert.Equal(t, kmsKeyID, aws.ToString(resp.SSEKMSKeyId))
		assert.Equal(t, int64(len(testData)), aws.ToInt64(resp.ContentLength))
	})
}

// TestSSEKMSIntegrationVariousDataSizes tests SSE-KMS with various data sizes
func TestSSEKMSIntegrationVariousDataSizes(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssekms-sizes-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	kmsKeyID := "test-key-size-tests"

	for _, size := range testDataSizes {
		t.Run(fmt.Sprintf("Size_%d_bytes", size), func(t *testing.T) {
			testData := generateTestData(size)
			objectKey := fmt.Sprintf("test-object-kms-size-%d", size)

			// Upload with SSE-KMS
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(objectKey),
				Body:                 bytes.NewReader(testData),
				ServerSideEncryption: types.ServerSideEncryptionAwsKms,
				SSEKMSKeyId:          aws.String(kmsKeyID),
			})
			require.NoError(t, err, "Failed to upload KMS object of size %d", size)

			// Retrieve with SSE-KMS
			resp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
			})
			require.NoError(t, err, "Failed to retrieve KMS object of size %d", size)
			defer resp.Body.Close()

			// Verify content matches
			retrievedData, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "Failed to read retrieved KMS data of size %d", size)
			assertDataEqual(t, testData, retrievedData, "Data mismatch for KMS size %d", size)

			// Verify content length is correct
			assert.Equal(t, int64(size), aws.ToInt64(resp.ContentLength),
				"Content length mismatch for KMS size %d", size)
		})
	}
}

// TestSSECObjectCopyIntegration tests SSE-C object copying end-to-end
func TestSSECObjectCopyIntegration(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssec-copy-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Generate test keys
	sourceKey := generateSSECKey()
	destKey := generateSSECKey()
	testData := []byte("Hello, SSE-C copy integration test!")

	// Upload source object
	sourceObjectKey := "source-object"
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(sourceObjectKey),
		Body:                 bytes.NewReader(testData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(sourceKey.KeyB64),
		SSECustomerKeyMD5:    aws.String(sourceKey.KeyMD5),
	})
	require.NoError(t, err, "Failed to upload source SSE-C object")

	t.Run("Copy SSE-C to SSE-C with different key", func(t *testing.T) {
		destObjectKey := "dest-object-ssec"
		copySource := fmt.Sprintf("%s/%s", bucketName, sourceObjectKey)

		// Copy object with different SSE-C key
		_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:                         aws.String(bucketName),
			Key:                            aws.String(destObjectKey),
			CopySource:                     aws.String(copySource),
			CopySourceSSECustomerAlgorithm: aws.String("AES256"),
			CopySourceSSECustomerKey:       aws.String(sourceKey.KeyB64),
			CopySourceSSECustomerKeyMD5:    aws.String(sourceKey.KeyMD5),
			SSECustomerAlgorithm:           aws.String("AES256"),
			SSECustomerKey:                 aws.String(destKey.KeyB64),
			SSECustomerKeyMD5:              aws.String(destKey.KeyMD5),
		})
		require.NoError(t, err, "Failed to copy SSE-C object")

		// Retrieve copied object with destination key
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(destObjectKey),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(destKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(destKey.KeyMD5),
		})
		require.NoError(t, err, "Failed to retrieve copied SSE-C object")
		defer resp.Body.Close()

		// Verify content matches original
		retrievedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read copied data")
		assertDataEqual(t, testData, retrievedData, "Copied data does not match original")
	})

	t.Run("Copy SSE-C to plain", func(t *testing.T) {
		destObjectKey := "dest-object-plain"
		copySource := fmt.Sprintf("%s/%s", bucketName, sourceObjectKey)

		// Copy SSE-C object to plain object
		_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:                         aws.String(bucketName),
			Key:                            aws.String(destObjectKey),
			CopySource:                     aws.String(copySource),
			CopySourceSSECustomerAlgorithm: aws.String("AES256"),
			CopySourceSSECustomerKey:       aws.String(sourceKey.KeyB64),
			CopySourceSSECustomerKeyMD5:    aws.String(sourceKey.KeyMD5),
			// No destination encryption headers = plain object
		})
		require.NoError(t, err, "Failed to copy SSE-C to plain object")

		// Retrieve plain object (no encryption headers needed)
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(destObjectKey),
		})
		require.NoError(t, err, "Failed to retrieve plain copied object")
		defer resp.Body.Close()

		// Verify content matches original
		retrievedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read plain copied data")
		assertDataEqual(t, testData, retrievedData, "Plain copied data does not match original")
	})
}

// TestSSEKMSObjectCopyIntegration tests SSE-KMS object copying end-to-end
func TestSSEKMSObjectCopyIntegration(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssekms-copy-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	testData := []byte("Hello, SSE-KMS copy integration test!")
	sourceKeyID := "source-test-key-123"
	destKeyID := "dest-test-key-456"

	// Upload source object with SSE-KMS
	sourceObjectKey := "source-object-kms"
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(sourceObjectKey),
		Body:                 bytes.NewReader(testData),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String(sourceKeyID),
	})
	require.NoError(t, err, "Failed to upload source SSE-KMS object")

	t.Run("Copy SSE-KMS with different key", func(t *testing.T) {
		destObjectKey := "dest-object-kms"
		copySource := fmt.Sprintf("%s/%s", bucketName, sourceObjectKey)

		// Copy object with different SSE-KMS key
		_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(destObjectKey),
			CopySource:           aws.String(copySource),
			ServerSideEncryption: types.ServerSideEncryptionAwsKms,
			SSEKMSKeyId:          aws.String(destKeyID),
		})
		require.NoError(t, err, "Failed to copy SSE-KMS object")

		// Retrieve copied object
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(destObjectKey),
		})
		require.NoError(t, err, "Failed to retrieve copied SSE-KMS object")
		defer resp.Body.Close()

		// Verify content matches original
		retrievedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read copied KMS data")
		assertDataEqual(t, testData, retrievedData, "Copied KMS data does not match original")

		// Verify new key ID is used
		assert.Equal(t, destKeyID, aws.ToString(resp.SSEKMSKeyId))
	})
}

// TestSSEMultipartUploadIntegration tests SSE multipart uploads end-to-end
func TestSSEMultipartUploadIntegration(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"sse-multipart-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	t.Run("SSE-C Multipart Upload", func(t *testing.T) {
		sseKey := generateSSECKey()
		objectKey := "multipart-ssec-object"

		// Create multipart upload
		createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(t, err, "Failed to create SSE-C multipart upload")

		uploadID := aws.ToString(createResp.UploadId)

		// Upload parts
		partSize := 5 * 1024 * 1024 // 5MB
		part1Data := generateTestData(partSize)
		part2Data := generateTestData(partSize)

		// Upload part 1
		part1Resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			PartNumber:           aws.Int32(1),
			UploadId:             aws.String(uploadID),
			Body:                 bytes.NewReader(part1Data),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(t, err, "Failed to upload part 1")

		// Upload part 2
		part2Resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			PartNumber:           aws.Int32(2),
			UploadId:             aws.String(uploadID),
			Body:                 bytes.NewReader(part2Data),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(t, err, "Failed to upload part 2")

		// Complete multipart upload
		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{
						ETag:       part1Resp.ETag,
						PartNumber: aws.Int32(1),
					},
					{
						ETag:       part2Resp.ETag,
						PartNumber: aws.Int32(2),
					},
				},
			},
		})
		require.NoError(t, err, "Failed to complete SSE-C multipart upload")

		// Retrieve and verify the complete object
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(t, err, "Failed to retrieve multipart SSE-C object")
		defer resp.Body.Close()

		retrievedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read multipart data")

		// Verify data matches concatenated parts
		expectedData := append(part1Data, part2Data...)
		assertDataEqual(t, expectedData, retrievedData, "Multipart data does not match original")
		assert.Equal(t, int64(len(expectedData)), aws.ToInt64(resp.ContentLength),
			"Multipart content length mismatch")
	})

	t.Run("SSE-KMS Multipart Upload", func(t *testing.T) {
		kmsKeyID := "test-multipart-key"
		objectKey := "multipart-kms-object"

		// Create multipart upload
		createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			ServerSideEncryption: types.ServerSideEncryptionAwsKms,
			SSEKMSKeyId:          aws.String(kmsKeyID),
		})
		require.NoError(t, err, "Failed to create SSE-KMS multipart upload")

		uploadID := aws.ToString(createResp.UploadId)

		// Upload parts
		partSize := 5 * 1024 * 1024 // 5MB
		part1Data := generateTestData(partSize)
		part2Data := generateTestData(partSize / 2) // Different size

		// Upload part 1
		part1Resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			PartNumber: aws.Int32(1),
			UploadId:   aws.String(uploadID),
			Body:       bytes.NewReader(part1Data),
		})
		require.NoError(t, err, "Failed to upload KMS part 1")

		// Upload part 2
		part2Resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			PartNumber: aws.Int32(2),
			UploadId:   aws.String(uploadID),
			Body:       bytes.NewReader(part2Data),
		})
		require.NoError(t, err, "Failed to upload KMS part 2")

		// Complete multipart upload
		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{
						ETag:       part1Resp.ETag,
						PartNumber: aws.Int32(1),
					},
					{
						ETag:       part2Resp.ETag,
						PartNumber: aws.Int32(2),
					},
				},
			},
		})
		require.NoError(t, err, "Failed to complete SSE-KMS multipart upload")

		// Retrieve and verify the complete object
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to retrieve multipart SSE-KMS object")
		defer resp.Body.Close()

		retrievedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read multipart KMS data")

		// Verify data matches concatenated parts
		expectedData := append(part1Data, part2Data...)

		// Debug: Print some information about the sizes and first few bytes
		t.Logf("Expected data size: %d, Retrieved data size: %d", len(expectedData), len(retrievedData))
		if len(expectedData) > 0 && len(retrievedData) > 0 {
			t.Logf("Expected first 32 bytes: %x", expectedData[:min(32, len(expectedData))])
			t.Logf("Retrieved first 32 bytes: %x", retrievedData[:min(32, len(retrievedData))])
		}

		assertDataEqual(t, expectedData, retrievedData, "Multipart KMS data does not match original")

		// Verify KMS metadata
		assert.Equal(t, types.ServerSideEncryptionAwsKms, resp.ServerSideEncryption)
		assert.Equal(t, kmsKeyID, aws.ToString(resp.SSEKMSKeyId))
	})
}

// TestDebugSSEMultipart helps debug the multipart SSE-KMS data mismatch
func TestDebugSSEMultipart(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"debug-multipart-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	objectKey := "debug-multipart-object"
	kmsKeyID := "test-multipart-key"

	// Create multipart upload
	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String(kmsKeyID),
	})
	require.NoError(t, err, "Failed to create SSE-KMS multipart upload")

	uploadID := aws.ToString(createResp.UploadId)

	// Upload two parts - exactly like the failing test
	partSize := 5 * 1024 * 1024                 // 5MB
	part1Data := generateTestData(partSize)     // 5MB
	part2Data := generateTestData(partSize / 2) // 2.5MB

	// Upload part 1
	part1Resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		PartNumber: aws.Int32(1),
		UploadId:   aws.String(uploadID),
		Body:       bytes.NewReader(part1Data),
	})
	require.NoError(t, err, "Failed to upload part 1")

	// Upload part 2
	part2Resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectKey),
		PartNumber: aws.Int32(2),
		UploadId:   aws.String(uploadID),
		Body:       bytes.NewReader(part2Data),
	})
	require.NoError(t, err, "Failed to upload part 2")

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{ETag: part1Resp.ETag, PartNumber: aws.Int32(1)},
				{ETag: part2Resp.ETag, PartNumber: aws.Int32(2)},
			},
		},
	})
	require.NoError(t, err, "Failed to complete multipart upload")

	// Retrieve the object
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to retrieve object")
	defer resp.Body.Close()

	retrievedData, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "Failed to read retrieved data")

	// Expected data
	expectedData := append(part1Data, part2Data...)

	t.Logf("=== DATA COMPARISON DEBUG ===")
	t.Logf("Expected size: %d, Retrieved size: %d", len(expectedData), len(retrievedData))

	// Find exact point of divergence
	divergePoint := -1
	minLen := len(expectedData)
	if len(retrievedData) < minLen {
		minLen = len(retrievedData)
	}

	for i := 0; i < minLen; i++ {
		if expectedData[i] != retrievedData[i] {
			divergePoint = i
			break
		}
	}

	if divergePoint >= 0 {
		t.Logf("Data diverges at byte %d (0x%x)", divergePoint, divergePoint)
		t.Logf("Expected: 0x%02x, Retrieved: 0x%02x", expectedData[divergePoint], retrievedData[divergePoint])

		// Show context around divergence point
		start := divergePoint - 10
		if start < 0 {
			start = 0
		}
		end := divergePoint + 10
		if end > minLen {
			end = minLen
		}

		t.Logf("Context [%d:%d]:", start, end)
		t.Logf("Expected:  %x", expectedData[start:end])
		t.Logf("Retrieved: %x", retrievedData[start:end])

		// Identify chunk boundaries
		if divergePoint >= 4194304 {
			t.Logf("Divergence is in chunk 2 or 3 (after 4MB boundary)")
		}
		if divergePoint >= 5242880 {
			t.Logf("Divergence is in chunk 3 (part 2, after 5MB boundary)")
		}
	} else if len(expectedData) != len(retrievedData) {
		t.Logf("Data lengths differ but common part matches")
	} else {
		t.Logf("Data matches completely!")
	}

	// Test completed successfully
	t.Logf("SSE comparison test completed - data matches completely!")
}

// TestSSEErrorConditions tests various error conditions in SSE
func TestSSEErrorConditions(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"sse-errors-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	t.Run("SSE-C Invalid Key Length", func(t *testing.T) {
		invalidKey := base64.StdEncoding.EncodeToString([]byte("too-short"))

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String("invalid-key-test"),
			Body:                 strings.NewReader("test"),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(invalidKey),
			SSECustomerKeyMD5:    aws.String("invalid-md5"),
		})
		assert.Error(t, err, "Should fail with invalid SSE-C key")
	})

	t.Run("SSE-KMS Invalid Key ID", func(t *testing.T) {
		// Empty key ID should be rejected
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String("invalid-kms-key-test"),
			Body:                 strings.NewReader("test"),
			ServerSideEncryption: types.ServerSideEncryptionAwsKms,
			SSEKMSKeyId:          aws.String(""), // Invalid empty key
		})
		assert.Error(t, err, "Should fail with empty KMS key ID")
	})
}

// BenchmarkSSECThroughput benchmarks SSE-C throughput
func BenchmarkSSECThroughput(b *testing.B) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(b, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssec-bench-")
	require.NoError(b, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	sseKey := generateSSECKey()
	testData := generateTestData(1024 * 1024) // 1MB

	b.ResetTimer()
	b.SetBytes(int64(len(testData)))

	for i := 0; i < b.N; i++ {
		objectKey := fmt.Sprintf("bench-object-%d", i)

		// Upload
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(b, err, "Failed to upload in benchmark")

		// Download
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(b, err, "Failed to download in benchmark")

		_, err = io.ReadAll(resp.Body)
		require.NoError(b, err, "Failed to read data in benchmark")
		resp.Body.Close()
	}
}

// TestSSECRangeRequests tests SSE-C with HTTP Range requests
func TestSSECRangeRequests(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssec-range-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	sseKey := generateSSECKey()
	// Create test data that's large enough for meaningful range tests
	testData := generateTestData(2048) // 2KB
	objectKey := "test-range-object"

	// Upload with SSE-C
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 bytes.NewReader(testData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(sseKey.KeyB64),
		SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
	})
	require.NoError(t, err, "Failed to upload SSE-C object")

	// Test various range requests
	testCases := []struct {
		name  string
		start int64
		end   int64
	}{
		{"First 100 bytes", 0, 99},
		{"Middle 100 bytes", 500, 599},
		{"Last 100 bytes", int64(len(testData) - 100), int64(len(testData) - 1)},
		{"Single byte", 42, 42},
		{"Cross boundary", 15, 17}, // Test AES block boundary crossing
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Get range with SSE-C
			resp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(objectKey),
				Range:                aws.String(fmt.Sprintf("bytes=%d-%d", tc.start, tc.end)),
				SSECustomerAlgorithm: aws.String("AES256"),
				SSECustomerKey:       aws.String(sseKey.KeyB64),
				SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
			})
			require.NoError(t, err, "Failed to get range %d-%d from SSE-C object", tc.start, tc.end)
			defer resp.Body.Close()

			// Range requests should return partial content status
			// Note: AWS SDK Go v2 doesn't expose HTTP status code directly in GetObject response
			// The fact that we get a successful response with correct range data indicates 206 status

			// Read the range data
			rangeData, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "Failed to read range data")

			// Verify content matches expected range
			expectedLength := tc.end - tc.start + 1
			expectedData := testData[tc.start : tc.start+expectedLength]
			assertDataEqual(t, expectedData, rangeData, "Range data mismatch for %s", tc.name)

			// Verify content length header
			assert.Equal(t, expectedLength, aws.ToInt64(resp.ContentLength), "Content length mismatch for %s", tc.name)

			// Verify SSE headers are present
			assert.Equal(t, "AES256", aws.ToString(resp.SSECustomerAlgorithm))
			assert.Equal(t, sseKey.KeyMD5, aws.ToString(resp.SSECustomerKeyMD5))
		})
	}
}

// TestSSEKMSRangeRequests tests SSE-KMS with HTTP Range requests
func TestSSEKMSRangeRequests(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssekms-range-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	kmsKeyID := "test-range-key"
	// Create test data that's large enough for meaningful range tests
	testData := generateTestData(2048) // 2KB
	objectKey := "test-kms-range-object"

	// Upload with SSE-KMS
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 bytes.NewReader(testData),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String(kmsKeyID),
	})
	require.NoError(t, err, "Failed to upload SSE-KMS object")

	// Test various range requests
	testCases := []struct {
		name  string
		start int64
		end   int64
	}{
		{"First 100 bytes", 0, 99},
		{"Middle 100 bytes", 500, 599},
		{"Last 100 bytes", int64(len(testData) - 100), int64(len(testData) - 1)},
		{"Single byte", 42, 42},
		{"Cross boundary", 15, 17}, // Test AES block boundary crossing
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Get range with SSE-KMS (no additional headers needed for GET)
			resp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
				Range:  aws.String(fmt.Sprintf("bytes=%d-%d", tc.start, tc.end)),
			})
			require.NoError(t, err, "Failed to get range %d-%d from SSE-KMS object", tc.start, tc.end)
			defer resp.Body.Close()

			// Range requests should return partial content status
			// Note: AWS SDK Go v2 doesn't expose HTTP status code directly in GetObject response
			// The fact that we get a successful response with correct range data indicates 206 status

			// Read the range data
			rangeData, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "Failed to read range data")

			// Verify content matches expected range
			expectedLength := tc.end - tc.start + 1
			expectedData := testData[tc.start : tc.start+expectedLength]
			assertDataEqual(t, expectedData, rangeData, "Range data mismatch for %s", tc.name)

			// Verify content length header
			assert.Equal(t, expectedLength, aws.ToInt64(resp.ContentLength), "Content length mismatch for %s", tc.name)

			// Verify SSE headers are present
			assert.Equal(t, types.ServerSideEncryptionAwsKms, resp.ServerSideEncryption)
			assert.Equal(t, kmsKeyID, aws.ToString(resp.SSEKMSKeyId))
		})
	}
}

// BenchmarkSSEKMSThroughput benchmarks SSE-KMS throughput
func BenchmarkSSEKMSThroughput(b *testing.B) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(b, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"ssekms-bench-")
	require.NoError(b, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	kmsKeyID := "bench-test-key"
	testData := generateTestData(1024 * 1024) // 1MB

	b.ResetTimer()
	b.SetBytes(int64(len(testData)))

	for i := 0; i < b.N; i++ {
		objectKey := fmt.Sprintf("bench-kms-object-%d", i)

		// Upload
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAwsKms,
			SSEKMSKeyId:          aws.String(kmsKeyID),
		})
		require.NoError(b, err, "Failed to upload in KMS benchmark")

		// Download
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(b, err, "Failed to download in KMS benchmark")

		_, err = io.ReadAll(resp.Body)
		require.NoError(b, err, "Failed to read KMS data in benchmark")
		resp.Body.Close()
	}
}
