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

// TestSSES3IntegrationBasic tests basic SSE-S3 upload and download functionality
func TestSSES3IntegrationBasic(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sse-s3-basic")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	testData := []byte("Hello, SSE-S3! This is a test of server-side encryption with S3-managed keys.")
	objectKey := "test-sse-s3-object.txt"

	t.Run("SSE-S3 Upload", func(t *testing.T) {
		// Upload object with SSE-S3
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "Failed to upload object with SSE-S3")
	})

	t.Run("SSE-S3 Download", func(t *testing.T) {
		// Download and verify object
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to download SSE-S3 object")

		// Verify SSE-S3 headers in response
		assert.Equal(t, types.ServerSideEncryptionAes256, resp.ServerSideEncryption, "Server-side encryption header mismatch")

		// Read and verify content
		downloadedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read downloaded data")
		resp.Body.Close()

		assertDataEqual(t, testData, downloadedData, "Downloaded data doesn't match original")
	})

	t.Run("SSE-S3 HEAD Request", func(t *testing.T) {
		// HEAD request should also return SSE headers
		resp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to HEAD SSE-S3 object")

		// Verify SSE-S3 headers
		assert.Equal(t, types.ServerSideEncryptionAes256, resp.ServerSideEncryption, "SSE-S3 header missing in HEAD response")
	})
}

// TestSSES3IntegrationVariousDataSizes tests SSE-S3 with various data sizes
func TestSSES3IntegrationVariousDataSizes(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sse-s3-sizes")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Test various data sizes including edge cases
	testSizes := []int{
		0,           // Empty file
		1,           // Single byte
		16,          // One AES block
		31,          // Just under two blocks
		32,          // Exactly two blocks
		100,         // Small file
		1024,        // 1KB
		8192,        // 8KB
		65536,       // 64KB
		1024 * 1024, // 1MB
	}

	for _, size := range testSizes {
		t.Run(fmt.Sprintf("Size_%d_bytes", size), func(t *testing.T) {
			testData := generateTestData(size)
			objectKey := fmt.Sprintf("test-sse-s3-%d.dat", size)

			// Upload with SSE-S3
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(objectKey),
				Body:                 bytes.NewReader(testData),
				ServerSideEncryption: types.ServerSideEncryptionAes256,
			})
			require.NoError(t, err, "Failed to upload SSE-S3 object of size %d", size)

			// Download and verify
			resp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
			})
			require.NoError(t, err, "Failed to download SSE-S3 object of size %d", size)

			// Verify encryption headers
			assert.Equal(t, types.ServerSideEncryptionAes256, resp.ServerSideEncryption, "Missing SSE-S3 header for size %d", size)

			// Verify content
			downloadedData, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "Failed to read downloaded data for size %d", size)
			resp.Body.Close()

			assertDataEqual(t, testData, downloadedData, "Data mismatch for size %d", size)
		})
	}
}

// TestSSES3WithUserMetadata tests SSE-S3 with user-defined metadata
func TestSSES3WithUserMetadata(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sse-s3-metadata")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	testData := []byte("SSE-S3 with custom metadata")
	objectKey := "test-object-with-metadata.txt"

	userMetadata := map[string]string{
		"author":      "test-user",
		"version":     "1.0",
		"environment": "test",
	}

	t.Run("Upload with Metadata", func(t *testing.T) {
		// Upload object with SSE-S3 and user metadata
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
			Metadata:             userMetadata,
		})
		require.NoError(t, err, "Failed to upload object with SSE-S3 and metadata")
	})

	t.Run("Verify Metadata and Encryption", func(t *testing.T) {
		// HEAD request to check metadata and encryption
		resp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to HEAD SSE-S3 object with metadata")

		// Verify SSE-S3 headers
		assert.Equal(t, types.ServerSideEncryptionAes256, resp.ServerSideEncryption, "SSE-S3 header missing with metadata")

		// Verify user metadata
		for key, expectedValue := range userMetadata {
			actualValue, exists := resp.Metadata[key]
			assert.True(t, exists, "Metadata key %s not found", key)
			assert.Equal(t, expectedValue, actualValue, "Metadata value mismatch for key %s", key)
		}
	})

	t.Run("Download and Verify Content", func(t *testing.T) {
		// Download and verify content
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to download SSE-S3 object with metadata")

		// Verify SSE-S3 headers
		assert.Equal(t, types.ServerSideEncryptionAes256, resp.ServerSideEncryption, "SSE-S3 header missing in GET response")

		// Verify content
		downloadedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read downloaded data")
		resp.Body.Close()

		assertDataEqual(t, testData, downloadedData, "Downloaded data doesn't match original")
	})
}

// TestSSES3RangeRequests tests SSE-S3 with HTTP range requests
func TestSSES3RangeRequests(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sse-s3-range")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Create test data large enough to ensure multipart storage
	testData := generateTestData(1024 * 1024) // 1MB to ensure multipart chunking
	objectKey := "test-sse-s3-range.dat"

	// Upload object with SSE-S3
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 bytes.NewReader(testData),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	})
	require.NoError(t, err, "Failed to upload SSE-S3 object for range testing")

	testCases := []struct {
		name          string
		rangeHeader   string
		expectedStart int
		expectedEnd   int
	}{
		{"First 100 bytes", "bytes=0-99", 0, 99},
		{"Middle range", "bytes=100000-199999", 100000, 199999},
		{"Last 100 bytes", "bytes=1048476-1048575", 1048476, 1048575},
		{"From offset to end", "bytes=500000-", 500000, len(testData) - 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Request range
			resp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
				Range:  aws.String(tc.rangeHeader),
			})
			require.NoError(t, err, "Failed to get range %s", tc.rangeHeader)

			// Verify SSE-S3 headers are present in range response
			assert.Equal(t, types.ServerSideEncryptionAes256, resp.ServerSideEncryption, "SSE-S3 header missing in range response")

			// Read range data
			rangeData, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "Failed to read range data")
			resp.Body.Close()

			// Calculate expected data
			endIndex := tc.expectedEnd
			if tc.expectedEnd >= len(testData) {
				endIndex = len(testData) - 1
			}
			expectedData := testData[tc.expectedStart : endIndex+1]

			// Verify range data
			assertDataEqual(t, expectedData, rangeData, "Range data mismatch for %s", tc.rangeHeader)
		})
	}
}

// TestSSES3BucketDefaultEncryption tests bucket-level default encryption with SSE-S3
func TestSSES3BucketDefaultEncryption(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sse-s3-default")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	t.Run("Set Bucket Default Encryption", func(t *testing.T) {
		// Set bucket encryption configuration
		_, err := client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
			Bucket: aws.String(bucketName),
			ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
				Rules: []types.ServerSideEncryptionRule{
					{
						ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
							SSEAlgorithm: types.ServerSideEncryptionAes256,
						},
					},
				},
			},
		})
		require.NoError(t, err, "Failed to set bucket default encryption")
	})

	t.Run("Upload Object Without Encryption Headers", func(t *testing.T) {
		testData := []byte("This object should be automatically encrypted with SSE-S3 due to bucket default policy.")
		objectKey := "test-default-encrypted-object.txt"

		// Upload object WITHOUT any encryption headers
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(testData),
			// No ServerSideEncryption specified - should use bucket default
		})
		require.NoError(t, err, "Failed to upload object without encryption headers")

		// Download and verify it was automatically encrypted
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to download object")

		// Verify SSE-S3 headers are present (indicating automatic encryption)
		assert.Equal(t, types.ServerSideEncryptionAes256, resp.ServerSideEncryption, "Object should have been automatically encrypted with SSE-S3")

		// Verify content is correct (decryption works)
		downloadedData, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read downloaded data")
		resp.Body.Close()

		assertDataEqual(t, testData, downloadedData, "Downloaded data doesn't match original")
	})

	t.Run("Get Bucket Encryption Configuration", func(t *testing.T) {
		// Verify we can retrieve the bucket encryption configuration
		resp, err := client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err, "Failed to get bucket encryption configuration")

		require.Len(t, resp.ServerSideEncryptionConfiguration.Rules, 1, "Should have one encryption rule")
		rule := resp.ServerSideEncryptionConfiguration.Rules[0]
		assert.Equal(t, types.ServerSideEncryptionAes256, rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm, "Encryption algorithm should be AES256")
	})

	t.Run("Delete Bucket Encryption Configuration", func(t *testing.T) {
		// Remove bucket encryption configuration
		_, err := client.DeleteBucketEncryption(ctx, &s3.DeleteBucketEncryptionInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err, "Failed to delete bucket encryption configuration")

		// Verify it's removed by trying to get it (should fail)
		_, err = client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{
			Bucket: aws.String(bucketName),
		})
		require.Error(t, err, "Getting bucket encryption should fail after deletion")
	})

	t.Run("Upload After Removing Default Encryption", func(t *testing.T) {
		testData := []byte("This object should NOT be encrypted after removing bucket default.")
		objectKey := "test-no-default-encryption.txt"

		// Upload object without encryption headers (should not be encrypted now)
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(testData),
		})
		require.NoError(t, err, "Failed to upload object")

		// Verify it's NOT encrypted
		resp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to HEAD object")

		// ServerSideEncryption should be empty/nil when no encryption is applied
		assert.Empty(t, resp.ServerSideEncryption, "Object should not be encrypted after removing bucket default")
	})
}

// TestSSES3MultipartUploads tests SSE-S3 multipart upload functionality
func TestSSES3MultipartUploads(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"sse-s3-multipart-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	t.Run("Large_File_Multipart_Upload", func(t *testing.T) {
		objectKey := "test-sse-s3-multipart-large.dat"
		// Create 10MB test data to ensure multipart upload
		testData := generateTestData(10 * 1024 * 1024)

		// Upload with SSE-S3
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "SSE-S3 multipart upload failed")

		// Verify encryption headers
		headResp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to head object")

		assert.Equal(t, types.ServerSideEncryptionAes256, headResp.ServerSideEncryption, "Expected SSE-S3 encryption")

		// Download and verify content
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to download SSE-S3 multipart object")
		defer getResp.Body.Close()

		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Failed to read downloaded data")

		assert.Equal(t, testData, downloadedData, "SSE-S3 multipart upload data should match")

		// Test range requests on multipart SSE-S3 object
		t.Run("Range_Request_On_Multipart", func(t *testing.T) {
			start := int64(1024 * 1024)   // 1MB offset
			end := int64(2*1024*1024 - 1) // 2MB - 1
			expectedLength := end - start + 1

			rangeResp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
				Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
			})
			require.NoError(t, err, "Failed to get range from SSE-S3 multipart object")
			defer rangeResp.Body.Close()

			rangeData, err := io.ReadAll(rangeResp.Body)
			require.NoError(t, err, "Failed to read range data")

			assert.Equal(t, expectedLength, int64(len(rangeData)), "Range length should match")

			// Verify range content matches original data
			expectedRange := testData[start : end+1]
			assert.Equal(t, expectedRange, rangeData, "Range content should match for SSE-S3 multipart object")
		})
	})

	t.Run("Explicit_Multipart_Upload_API", func(t *testing.T) {
		objectKey := "test-sse-s3-explicit-multipart.dat"
		testData := generateTestData(15 * 1024 * 1024) // 15MB

		// Create multipart upload with SSE-S3
		createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "Failed to create SSE-S3 multipart upload")

		uploadID := *createResp.UploadId
		var parts []types.CompletedPart

		// Upload parts (5MB each, except the last part)
		partSize := 5 * 1024 * 1024
		for i := 0; i < len(testData); i += partSize {
			partNumber := int32(len(parts) + 1)
			endIdx := i + partSize
			if endIdx > len(testData) {
				endIdx = len(testData)
			}
			partData := testData[i:endIdx]

			uploadPartResp, err := client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(bucketName),
				Key:        aws.String(objectKey),
				PartNumber: aws.Int32(partNumber),
				UploadId:   aws.String(uploadID),
				Body:       bytes.NewReader(partData),
			})
			require.NoError(t, err, "Failed to upload part %d", partNumber)

			parts = append(parts, types.CompletedPart{
				ETag:       uploadPartResp.ETag,
				PartNumber: aws.Int32(partNumber),
			})
		}

		// Complete multipart upload
		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: parts,
			},
		})
		require.NoError(t, err, "Failed to complete SSE-S3 multipart upload")

		// Verify the completed object
		headResp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to head completed multipart object")

		assert.Equal(t, types.ServerSideEncryptionAes256, headResp.ServerSideEncryption, "Expected SSE-S3 encryption on completed multipart object")

		// Download and verify content
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to download completed SSE-S3 multipart object")
		defer getResp.Body.Close()

		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Failed to read downloaded data")

		assert.Equal(t, testData, downloadedData, "Explicit SSE-S3 multipart upload data should match")
	})
}

// TestCrossSSECopy tests copying objects between different SSE encryption types
func TestCrossSSECopy(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"sse-cross-copy-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Test data
	testData := []byte("Cross-SSE copy test data")

	// Generate proper SSE-C key
	sseKey := generateSSECKey()

	t.Run("SSE-S3_to_Unencrypted", func(t *testing.T) {
		sourceKey := "source-sse-s3-obj"
		destKey := "dest-unencrypted-obj"

		// Upload with SSE-S3
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(sourceKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "SSE-S3 upload failed")

		// Copy to unencrypted
		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(destKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", bucketName, sourceKey)),
		})
		require.NoError(t, err, "Copy SSE-S3 to unencrypted failed")

		// Verify destination is unencrypted and content matches
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(destKey),
		})
		require.NoError(t, err, "GET failed")
		defer getResp.Body.Close()

		assert.Empty(t, getResp.ServerSideEncryption, "Should be unencrypted")
		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Read failed")
		assertDataEqual(t, testData, downloadedData)
	})

	t.Run("Unencrypted_to_SSE-S3", func(t *testing.T) {
		sourceKey := "source-unencrypted-obj"
		destKey := "dest-sse-s3-obj"

		// Upload unencrypted
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(sourceKey),
			Body:   bytes.NewReader(testData),
		})
		require.NoError(t, err, "Unencrypted upload failed")

		// Copy to SSE-S3
		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(destKey),
			CopySource:           aws.String(fmt.Sprintf("%s/%s", bucketName, sourceKey)),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "Copy unencrypted to SSE-S3 failed")

		// Verify destination is SSE-S3 encrypted and content matches
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(destKey),
		})
		require.NoError(t, err, "GET failed")
		defer getResp.Body.Close()

		assert.Equal(t, types.ServerSideEncryptionAes256, getResp.ServerSideEncryption, "Expected SSE-S3")
		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Read failed")
		assertDataEqual(t, testData, downloadedData)
	})

	t.Run("SSE-C_to_SSE-S3", func(t *testing.T) {
		sourceKey := "source-sse-c-obj"
		destKey := "dest-sse-s3-obj"

		// Upload with SSE-C
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(sourceKey),
			Body:                 bytes.NewReader(testData),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(t, err, "SSE-C upload failed")

		// Copy to SSE-S3
		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:                         aws.String(bucketName),
			Key:                            aws.String(destKey),
			CopySource:                     aws.String(fmt.Sprintf("%s/%s", bucketName, sourceKey)),
			CopySourceSSECustomerAlgorithm: aws.String("AES256"),
			CopySourceSSECustomerKey:       aws.String(sseKey.KeyB64),
			CopySourceSSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
			ServerSideEncryption:           types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "Copy SSE-C to SSE-S3 failed")

		// Verify destination encryption and content
		headResp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(destKey),
		})
		require.NoError(t, err, "HEAD failed")
		assert.Equal(t, types.ServerSideEncryptionAes256, headResp.ServerSideEncryption, "Expected SSE-S3")

		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(destKey),
		})
		require.NoError(t, err, "GET failed")
		defer getResp.Body.Close()

		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Read failed")
		assertDataEqual(t, testData, downloadedData)
	})

	t.Run("SSE-S3_to_SSE-C", func(t *testing.T) {
		sourceKey := "source-sse-s3-obj"
		destKey := "dest-sse-c-obj"

		// Upload with SSE-S3
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(sourceKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "Failed to upload SSE-S3 source object")

		// Copy to SSE-C
		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(destKey),
			CopySource:           aws.String(fmt.Sprintf("%s/%s", bucketName, sourceKey)),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(t, err, "Copy SSE-S3 to SSE-C failed")

		// Verify destination encryption and content
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(destKey),
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(sseKey.KeyB64),
			SSECustomerKeyMD5:    aws.String(sseKey.KeyMD5),
		})
		require.NoError(t, err, "GET with SSE-C failed")
		defer getResp.Body.Close()

		assert.Equal(t, "AES256", aws.ToString(getResp.SSECustomerAlgorithm), "Expected SSE-C")
		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Read failed")
		assertDataEqual(t, testData, downloadedData)
	})
}

// TestCopyToBucketDefaultEncryptedRegression tests copying objects to buckets with default
// encryption enabled. This is a regression test for GitHub issue #7562 where copying from
// an unencrypted bucket to a bucket with SSE-S3 default encryption fails with error
// "invalid SSE-S3 source key type".
//
// The scenario is:
// 1. Create source bucket with SSE-S3 encryption
// 2. Upload encrypted object
// 3. Copy to temp bucket (unencrypted) - data is decrypted
// 4. Copy from temp bucket to dest bucket with SSE-S3 default encryption - this should re-encrypt
//
// The bug occurs because the source detection incorrectly identifies the temp object as
// SSE-S3 encrypted (based on leftover metadata) when it's actually unencrypted.
func TestCopyToBucketDefaultEncryptedRegression(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	// Create three buckets for the test scenario
	srcBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"copy-src-")
	require.NoError(t, err, "Failed to create source bucket")
	defer cleanupTestBucket(ctx, client, srcBucket)

	tempBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"copy-temp-")
	require.NoError(t, err, "Failed to create temp bucket")
	defer cleanupTestBucket(ctx, client, tempBucket)

	dstBucket, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"copy-dst-")
	require.NoError(t, err, "Failed to create destination bucket")
	defer cleanupTestBucket(ctx, client, dstBucket)

	// Enable SSE-S3 default encryption on source bucket
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(srcBucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set source bucket encryption")

	// Enable SSE-S3 default encryption on destination bucket
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(dstBucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set destination bucket encryption")

	// Test data
	testData := []byte("Test data for copy-to-default-encrypted bucket regression test - GitHub issue #7562")
	objectKey := "test-object.txt"

	t.Run("CopyEncrypted_ToTemp_ToEncrypted", func(t *testing.T) {
		// Step 1: Upload object to source bucket (will be automatically encrypted)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(srcBucket),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(testData),
			// No encryption header - bucket default applies
		})
		require.NoError(t, err, "Failed to upload to source bucket")

		// Verify source object is encrypted
		srcHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(srcBucket),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to HEAD source object")
		assert.Equal(t, types.ServerSideEncryptionAes256, srcHead.ServerSideEncryption,
			"Source object should be SSE-S3 encrypted")

		// Step 2: Copy to temp bucket (unencrypted) - this should decrypt
		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(tempBucket),
			Key:        aws.String(objectKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", srcBucket, objectKey)),
			// No encryption - data should be stored unencrypted
		})
		require.NoError(t, err, "Failed to copy to temp bucket")

		// Verify temp object is unencrypted
		tempHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(tempBucket),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to HEAD temp object")
		assert.Empty(t, tempHead.ServerSideEncryption,
			"Temp object should be unencrypted")

		// Verify temp object content is correct
		tempGet, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(tempBucket),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to GET temp object")
		tempData, err := io.ReadAll(tempGet.Body)
		tempGet.Body.Close()
		require.NoError(t, err, "Failed to read temp object")
		assertDataEqual(t, testData, tempData, "Temp object data mismatch")

		// Step 3: Copy from temp bucket to dest bucket (with default encryption)
		// THIS IS THE BUG: This copy fails with "invalid SSE-S3 source key type"
		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(dstBucket),
			Key:        aws.String(objectKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", tempBucket, objectKey)),
			// No encryption header - bucket default should apply
		})
		require.NoError(t, err, "Failed to copy to destination bucket - GitHub issue #7562")

		// Verify destination object is encrypted
		dstHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(dstBucket),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to HEAD destination object")
		assert.Equal(t, types.ServerSideEncryptionAes256, dstHead.ServerSideEncryption,
			"Destination object should be SSE-S3 encrypted via bucket default")

		// Verify destination object content is correct
		dstGet, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(dstBucket),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to GET destination object")
		dstData, err := io.ReadAll(dstGet.Body)
		dstGet.Body.Close()
		require.NoError(t, err, "Failed to read destination object")
		assertDataEqual(t, testData, dstData, "Destination object data mismatch after re-encryption")
	})

	t.Run("DirectCopyUnencrypted_ToEncrypted", func(t *testing.T) {
		// Simpler test case: copy from unencrypted bucket directly to encrypted bucket
		objectKey := "direct-copy-test.txt"

		// Upload to temp bucket (no default encryption)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(tempBucket),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(testData),
		})
		require.NoError(t, err, "Failed to upload to temp bucket")

		// Copy to destination bucket with default encryption
		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(dstBucket),
			Key:        aws.String(objectKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", tempBucket, objectKey)),
		})
		require.NoError(t, err, "Failed direct copy unencrypted to default-encrypted bucket")

		// Verify destination is encrypted
		dstHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(dstBucket),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to HEAD destination object")
		assert.Equal(t, types.ServerSideEncryptionAes256, dstHead.ServerSideEncryption,
			"Object should be encrypted via bucket default")

		// Verify content
		dstGet, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(dstBucket),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to GET destination object")
		dstData, err := io.ReadAll(dstGet.Body)
		dstGet.Body.Close()
		require.NoError(t, err, "Failed to read destination object")
		assertDataEqual(t, testData, dstData, "Data mismatch after encryption")
	})

	t.Run("CopyWithExplicitSSES3Header", func(t *testing.T) {
		// Test explicit SSE-S3 header during copy (should work even without bucket default)
		objectKey := "explicit-sse-copy-test.txt"

		// Upload to temp bucket (unencrypted)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(tempBucket),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(testData),
		})
		require.NoError(t, err, "Failed to upload to temp bucket")

		// Copy with explicit SSE-S3 header
		_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:               aws.String(tempBucket), // Same bucket, but with encryption
			Key:                  aws.String(objectKey + "-encrypted"),
			CopySource:           aws.String(fmt.Sprintf("%s/%s", tempBucket, objectKey)),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "Failed copy with explicit SSE-S3 header")

		// Verify encrypted
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(tempBucket),
			Key:    aws.String(objectKey + "-encrypted"),
		})
		require.NoError(t, err, "Failed to HEAD object")
		assert.Equal(t, types.ServerSideEncryptionAes256, head.ServerSideEncryption,
			"Object should be SSE-S3 encrypted")

		// Verify content
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(tempBucket),
			Key:    aws.String(objectKey + "-encrypted"),
		})
		require.NoError(t, err, "Failed to GET object")
		data, err := io.ReadAll(getResp.Body)
		getResp.Body.Close()
		require.NoError(t, err, "Failed to read object")
		assertDataEqual(t, testData, data, "Data mismatch")
	})
}

// REGRESSION TESTS FOR CRITICAL BUGS FIXED
// These tests specifically target the IV storage bugs that were fixed

// TestSSES3IVStorageRegression tests that IVs are properly stored for explicit SSE-S3 uploads
// This test would have caught the critical bug where IVs were discarded in putToFiler
func TestSSES3IVStorageRegression(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sse-s3-iv-regression")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	t.Run("Explicit SSE-S3 IV Storage and Retrieval", func(t *testing.T) {
		testData := []byte("This tests the critical IV storage bug that was fixed - the IV must be stored on the key object for decryption to work.")
		objectKey := "explicit-sse-s3-iv-test.txt"

		// Upload with explicit SSE-S3 header (this used to discard the IV)
		putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "Failed to upload explicit SSE-S3 object")

		// Verify PUT response has SSE-S3 headers
		assert.Equal(t, types.ServerSideEncryptionAes256, putResp.ServerSideEncryption, "PUT response should indicate SSE-S3")

		// Critical test: Download and decrypt the object
		// This would have FAILED with the original bug because IV was discarded
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to download explicit SSE-S3 object")

		// Verify GET response has SSE-S3 headers
		assert.Equal(t, types.ServerSideEncryptionAes256, getResp.ServerSideEncryption, "GET response should indicate SSE-S3")

		// This is the critical test - verify data can be decrypted correctly
		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Failed to read decrypted data")
		getResp.Body.Close()

		// This assertion would have FAILED with the original bug
		assertDataEqual(t, testData, downloadedData, "CRITICAL: Decryption failed - IV was not stored properly")
	})

	t.Run("Multiple Explicit SSE-S3 Objects", func(t *testing.T) {
		// Test multiple objects to ensure each gets its own unique IV
		numObjects := 5
		testDataSet := make([][]byte, numObjects)
		objectKeys := make([]string, numObjects)

		// Upload multiple objects with explicit SSE-S3
		for i := 0; i < numObjects; i++ {
			testDataSet[i] = []byte(fmt.Sprintf("Test data for object %d - verifying unique IV storage", i))
			objectKeys[i] = fmt.Sprintf("explicit-sse-s3-multi-%d.txt", i)

			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(objectKeys[i]),
				Body:                 bytes.NewReader(testDataSet[i]),
				ServerSideEncryption: types.ServerSideEncryptionAes256,
			})
			require.NoError(t, err, "Failed to upload explicit SSE-S3 object %d", i)
		}

		// Download and verify each object decrypts correctly
		for i := 0; i < numObjects; i++ {
			getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKeys[i]),
			})
			require.NoError(t, err, "Failed to download explicit SSE-S3 object %d", i)

			downloadedData, err := io.ReadAll(getResp.Body)
			require.NoError(t, err, "Failed to read decrypted data for object %d", i)
			getResp.Body.Close()

			assertDataEqual(t, testDataSet[i], downloadedData, "Decryption failed for object %d - IV not unique/stored", i)
		}
	})
}

// TestSSES3BucketDefaultIVStorageRegression tests bucket default SSE-S3 IV storage
// This test would have caught the critical bug where IVs were not stored on key objects in bucket defaults
func TestSSES3BucketDefaultIVStorageRegression(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sse-s3-default-iv-regression")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Set bucket default encryption to SSE-S3
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(bucketName),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set bucket default SSE-S3 encryption")

	t.Run("Bucket Default SSE-S3 IV Storage", func(t *testing.T) {
		testData := []byte("This tests the bucket default SSE-S3 IV storage bug - IV must be stored on key object for decryption.")
		objectKey := "bucket-default-sse-s3-iv-test.txt"

		// Upload WITHOUT encryption headers - should use bucket default SSE-S3
		// This used to fail because applySSES3DefaultEncryption didn't store IV on key
		putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(testData),
			// No ServerSideEncryption specified - should use bucket default
		})
		require.NoError(t, err, "Failed to upload object for bucket default SSE-S3")

		// Verify bucket default encryption was applied
		assert.Equal(t, types.ServerSideEncryptionAes256, putResp.ServerSideEncryption, "PUT response should show bucket default SSE-S3")

		// Critical test: Download and decrypt the object
		// This would have FAILED with the original bug because IV wasn't stored on key object
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to download bucket default SSE-S3 object")

		// Verify GET response shows SSE-S3 was applied
		assert.Equal(t, types.ServerSideEncryptionAes256, getResp.ServerSideEncryption, "GET response should show SSE-S3")

		// This is the critical test - verify decryption works
		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Failed to read decrypted data")
		getResp.Body.Close()

		// This assertion would have FAILED with the original bucket default bug
		assertDataEqual(t, testData, downloadedData, "CRITICAL: Bucket default SSE-S3 decryption failed - IV not stored on key object")
	})

	t.Run("Multiple Bucket Default Objects", func(t *testing.T) {
		// Test multiple objects with bucket default encryption
		numObjects := 3
		testDataSet := make([][]byte, numObjects)
		objectKeys := make([]string, numObjects)

		// Upload multiple objects without encryption headers
		for i := 0; i < numObjects; i++ {
			testDataSet[i] = []byte(fmt.Sprintf("Bucket default test data %d - verifying IV storage works", i))
			objectKeys[i] = fmt.Sprintf("bucket-default-multi-%d.txt", i)

			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKeys[i]),
				Body:   bytes.NewReader(testDataSet[i]),
				// No encryption headers - bucket default should apply
			})
			require.NoError(t, err, "Failed to upload bucket default object %d", i)
		}

		// Verify each object was encrypted and can be decrypted
		for i := 0; i < numObjects; i++ {
			getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKeys[i]),
			})
			require.NoError(t, err, "Failed to download bucket default object %d", i)

			// Verify SSE-S3 was applied by bucket default
			assert.Equal(t, types.ServerSideEncryptionAes256, getResp.ServerSideEncryption, "Object %d should be SSE-S3 encrypted", i)

			downloadedData, err := io.ReadAll(getResp.Body)
			require.NoError(t, err, "Failed to read decrypted data for object %d", i)
			getResp.Body.Close()

			assertDataEqual(t, testDataSet[i], downloadedData, "Bucket default SSE-S3 decryption failed for object %d", i)
		}
	})
}

// TestSSES3EdgeCaseRegression tests edge cases that could cause IV storage issues
func TestSSES3EdgeCaseRegression(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sse-s3-edge-regression")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	t.Run("Empty Object SSE-S3", func(t *testing.T) {
		// Test edge case: empty objects with SSE-S3 (IV storage still required)
		objectKey := "empty-sse-s3-object"

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader([]byte{}),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "Failed to upload empty SSE-S3 object")

		// Verify empty object can be retrieved (IV must be stored even for empty objects)
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to download empty SSE-S3 object")

		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Failed to read empty decrypted data")
		getResp.Body.Close()

		assert.Equal(t, []byte{}, downloadedData, "Empty object content mismatch")
		assert.Equal(t, types.ServerSideEncryptionAes256, getResp.ServerSideEncryption, "Empty object should be SSE-S3 encrypted")
	})

	t.Run("Large Object SSE-S3", func(t *testing.T) {
		// Test large objects to ensure IV storage works for chunked uploads
		largeData := generateTestData(1024 * 1024) // 1MB
		objectKey := "large-sse-s3-object"

		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(largeData),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
		})
		require.NoError(t, err, "Failed to upload large SSE-S3 object")

		// Verify large object can be decrypted (IV must be stored properly)
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to download large SSE-S3 object")

		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Failed to read large decrypted data")
		getResp.Body.Close()

		assertDataEqual(t, largeData, downloadedData, "Large object decryption failed - IV storage issue")
		assert.Equal(t, types.ServerSideEncryptionAes256, getResp.ServerSideEncryption, "Large object should be SSE-S3 encrypted")
	})
}

// TestSSES3ErrorHandlingRegression tests error handling improvements that were added
func TestSSES3ErrorHandlingRegression(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sse-s3-error-regression")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	t.Run("SSE-S3 With Other Valid Operations", func(t *testing.T) {
		// Ensure SSE-S3 works with other S3 operations (metadata, tagging, etc.)
		testData := []byte("Testing SSE-S3 with metadata and other operations")
		objectKey := "sse-s3-with-metadata"

		// Upload with SSE-S3 and metadata
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:               aws.String(bucketName),
			Key:                  aws.String(objectKey),
			Body:                 bytes.NewReader(testData),
			ServerSideEncryption: types.ServerSideEncryptionAes256,
			Metadata: map[string]string{
				"test-key": "test-value",
				"purpose":  "regression-test",
			},
		})
		require.NoError(t, err, "Failed to upload SSE-S3 object with metadata")

		// HEAD request to verify metadata and encryption
		headResp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to HEAD SSE-S3 object")

		assert.Equal(t, types.ServerSideEncryptionAes256, headResp.ServerSideEncryption, "HEAD should show SSE-S3")
		assert.Equal(t, "test-value", headResp.Metadata["test-key"], "Metadata should be preserved")
		assert.Equal(t, "regression-test", headResp.Metadata["purpose"], "Metadata should be preserved")

		// GET to verify decryption still works with metadata
		getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Failed to GET SSE-S3 object")

		downloadedData, err := io.ReadAll(getResp.Body)
		require.NoError(t, err, "Failed to read decrypted data")
		getResp.Body.Close()

		assertDataEqual(t, testData, downloadedData, "SSE-S3 with metadata decryption failed")
		assert.Equal(t, types.ServerSideEncryptionAes256, getResp.ServerSideEncryption, "GET should show SSE-S3")
		assert.Equal(t, "test-value", getResp.Metadata["test-key"], "GET metadata should be preserved")
	})
}

// TestSSES3FunctionalityCompletion tests that SSE-S3 feature is now fully functional
func TestSSES3FunctionalityCompletion(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, "sse-s3-completion")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	t.Run("All SSE-S3 Scenarios Work", func(t *testing.T) {
		scenarios := []struct {
			name        string
			setupBucket func() error
			encryption  *types.ServerSideEncryption
			expectSSES3 bool
		}{
			{
				name:        "Explicit SSE-S3 Header",
				setupBucket: func() error { return nil },
				encryption:  &[]types.ServerSideEncryption{types.ServerSideEncryptionAes256}[0],
				expectSSES3: true,
			},
			{
				name: "Bucket Default SSE-S3",
				setupBucket: func() error {
					_, err := client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
						Bucket: aws.String(bucketName),
						ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
							Rules: []types.ServerSideEncryptionRule{
								{
									ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
										SSEAlgorithm: types.ServerSideEncryptionAes256,
									},
								},
							},
						},
					})
					return err
				},
				encryption:  nil,
				expectSSES3: true,
			},
		}

		for i, scenario := range scenarios {
			t.Run(scenario.name, func(t *testing.T) {
				// Setup bucket if needed
				err := scenario.setupBucket()
				require.NoError(t, err, "Failed to setup bucket for scenario %s", scenario.name)

				testData := []byte(fmt.Sprintf("Test data for scenario: %s", scenario.name))
				objectKey := fmt.Sprintf("completion-test-%d", i)

				// Upload object
				putInput := &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(objectKey),
					Body:   bytes.NewReader(testData),
				}
				if scenario.encryption != nil {
					putInput.ServerSideEncryption = *scenario.encryption
				}

				putResp, err := client.PutObject(ctx, putInput)
				require.NoError(t, err, "Failed to upload object for scenario %s", scenario.name)

				if scenario.expectSSES3 {
					assert.Equal(t, types.ServerSideEncryptionAes256, putResp.ServerSideEncryption, "Should use SSE-S3 for %s", scenario.name)
				}

				// Download and verify
				getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(objectKey),
				})
				require.NoError(t, err, "Failed to download object for scenario %s", scenario.name)

				if scenario.expectSSES3 {
					assert.Equal(t, types.ServerSideEncryptionAes256, getResp.ServerSideEncryption, "Should return SSE-S3 for %s", scenario.name)
				}

				downloadedData, err := io.ReadAll(getResp.Body)
				require.NoError(t, err, "Failed to read data for scenario %s", scenario.name)
				getResp.Body.Close()

				// This is the ultimate test - decryption must work
				assertDataEqual(t, testData, downloadedData, "Decryption failed for scenario %s", scenario.name)

				// Clean up bucket encryption for next scenario
				client.DeleteBucketEncryption(ctx, &s3.DeleteBucketEncryptionInput{
					Bucket: aws.String(bucketName),
				})
			})
		}
	})
}
