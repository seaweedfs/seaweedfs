package encryption

import (
	"bytes"
	"context"
	"crypto/md5"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// S3TestConfig holds configuration for S3 tests
type S3TestConfig struct {
	Endpoint      string
	AccessKey     string
	SecretKey     string
	Region        string
	BucketPrefix  string
	UseSSL        bool
	SkipVerifySSL bool
}

// Default test configuration - should match s3tests.conf
var defaultConfig = &S3TestConfig{
	Endpoint:      "http://localhost:8333", // Default SeaweedFS S3 port
	AccessKey:     "some_access_key1",
	SecretKey:     "some_secret_key1",
	Region:        "us-east-1",
	BucketPrefix:  "test-sse-c-",
	UseSSL:        false,
	SkipVerifySSL: true,
}

// SSE-C test constants (matching s3tests)
const (
	testSSEKey    = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs="
	testSSEKeyMD5 = "0d6ca09c746d82227bec70a6fb5aef1f"

	wrongSSEKey    = "bGVhcmVzc2VjcmV0a2V5dGhhdGlzMzJieXRlc2xvbmc="
	wrongSSEKeyMD5 = "7aeef79f6e2de4e3b0b25b3a83b8cfb1"
)

// getS3Client creates an AWS S3 client for testing
func getS3Client(t *testing.T) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(defaultConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			defaultConfig.AccessKey,
			defaultConfig.SecretKey,
			"",
		)),
		config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               defaultConfig.Endpoint,
					SigningRegion:     defaultConfig.Region,
					HostnameImmutable: true,
				}, nil
			}),
		),
	)
	require.NoError(t, err, "Failed to load AWS config")

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

// createTestBucket creates a unique test bucket
func createTestBucket(t *testing.T, client *s3.Client) string {
	bucketName := fmt.Sprintf("%s%d", defaultConfig.BucketPrefix, time.Now().UnixNano())

	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "Failed to create test bucket")

	// Schedule cleanup
	t.Cleanup(func() {
		// List and delete all objects first
		listResp, _ := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})

		if listResp != nil && len(listResp.Contents) > 0 {
			for _, obj := range listResp.Contents {
				client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    obj.Key,
				})
			}
		}

		// Delete the bucket
		client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
	})

	return bucketName
}

func TestSSECBasicEncryption(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	testCases := []struct {
		name     string
		key      string
		data     string
		dataSize int
	}{
		{"small_object", "test-small", "Hello, World!", 13},
		{"1kb_object", "test-1kb", strings.Repeat("A", 1024), 1024},
		{"empty_object", "test-empty", "", 0},
		{"special_chars", "test-special", "Special chars: üñíčødé 🚀", 27},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Upload object with SSE-C
			_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(tc.key),
				Body:                 strings.NewReader(tc.data),
				SSECustomerAlgorithm: aws.String("AES256"),
				SSECustomerKey:       aws.String(testSSEKey),
				SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
			})
			require.NoError(t, err, "Failed to upload object with SSE-C")

			// Download object with correct SSE-C key
			getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(tc.key),
				SSECustomerAlgorithm: aws.String("AES256"),
				SSECustomerKey:       aws.String(testSSEKey),
				SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
			})
			require.NoError(t, err, "Failed to download object with SSE-C")

			// Verify response headers
			assert.Equal(t, "AES256", aws.ToString(getResp.SSECustomerAlgorithm))
			assert.Equal(t, testSSEKeyMD5, aws.ToString(getResp.SSECustomerKeyMD5))

			// Verify content
			downloadedData, err := io.ReadAll(getResp.Body)
			require.NoError(t, err, "Failed to read downloaded data")
			assert.Equal(t, tc.data, string(downloadedData), "Downloaded data doesn't match original")

			getResp.Body.Close()
		})
	}
}

func TestSSECHeadObject(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	objectKey := "test-head-object"
	testData := "This is test data for HEAD operation"

	// Upload object with SSE-C
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 strings.NewReader(testData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
		ContentType:          aws.String("text/plain"),
	})
	require.NoError(t, err, "Failed to upload object with SSE-C")

	// HEAD object without SSE-C headers (should fail)
	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	assert.Error(t, err, "HEAD without SSE-C headers should fail")

	// HEAD object with correct SSE-C headers (should succeed)
	headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "HEAD with correct SSE-C headers should succeed")

	// Verify response headers
	assert.Equal(t, "AES256", aws.ToString(headResp.SSECustomerAlgorithm))
	assert.Equal(t, testSSEKeyMD5, aws.ToString(headResp.SSECustomerKeyMD5))
	assert.Equal(t, "text/plain", aws.ToString(headResp.ContentType))
	assert.Equal(t, int64(len(testData)), aws.ToInt64(headResp.ContentLength))
}

func TestSSECWrongKey(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	objectKey := "test-wrong-key"
	testData := "This data should not be accessible with wrong key"

	// Upload object with SSE-C
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 strings.NewReader(testData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to upload object with SSE-C")

	// Try to download with wrong key (should fail)
	_, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(wrongSSEKey),
		SSECustomerKeyMD5:    aws.String(wrongSSEKeyMD5),
	})
	assert.Error(t, err, "GET with wrong SSE-C key should fail")

	// Try to HEAD with wrong key (should fail)
	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(wrongSSEKey),
		SSECustomerKeyMD5:    aws.String(wrongSSEKeyMD5),
	})
	assert.Error(t, err, "HEAD with wrong SSE-C key should fail")
}

func TestSSECMissingKey(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	objectKey := "test-missing-key"
	testData := "This encrypted data requires a key"

	// Upload object with SSE-C
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 strings.NewReader(testData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to upload object with SSE-C")

	// Try to download without SSE-C headers (should fail)
	_, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	assert.Error(t, err, "GET without SSE-C headers should fail for encrypted object")

	// Try to HEAD without SSE-C headers (should fail)
	_, err = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	assert.Error(t, err, "HEAD without SSE-C headers should fail for encrypted object")
}

func TestSSECUnnecessaryKey(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	objectKey := "test-unnecessary-key"
	testData := "This is unencrypted data"

	// Upload object without SSE-C
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(testData),
	})
	require.NoError(t, err, "Failed to upload unencrypted object")

	// Download without SSE-C headers (should succeed)
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "GET without SSE-C headers should succeed for unencrypted object")
	getResp.Body.Close()

	// Try to download with SSE-C headers (should fail)
	_, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	assert.Error(t, err, "GET with SSE-C headers should fail for unencrypted object")
}

func TestSSECInvalidHeaders(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	objectKey := "test-invalid-headers"
	testData := "Test data"

	testCases := []struct {
		name      string
		algorithm string
		key       string
		keyMD5    string
	}{
		{"invalid_algorithm", "AES128", testSSEKey, testSSEKeyMD5},
		{"invalid_key_format", "AES256", "not-base64!", testSSEKeyMD5},
		{"key_too_short", "AES256", base64.StdEncoding.EncodeToString(make([]byte, 16)), fmt.Sprintf("%x", md5.Sum(make([]byte, 16)))},
		{"wrong_md5", "AES256", testSSEKey, "wrongmd5hash"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Try to upload with invalid SSE-C headers (should fail)
			_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(objectKey),
				Body:                 strings.NewReader(testData),
				SSECustomerAlgorithm: aws.String(tc.algorithm),
				SSECustomerKey:       aws.String(tc.key),
				SSECustomerKeyMD5:    aws.String(tc.keyMD5),
			})
			assert.Error(t, err, "PUT with invalid SSE-C headers should fail")
		})
	}
}

func TestSSECLargeObject(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	objectKey := "test-large-object"

	// Create 1MB of test data
	testData := strings.Repeat("A", 1024*1024)

	// Upload large object with SSE-C
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 strings.NewReader(testData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to upload large object with SSE-C")

	// Download and verify
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to download large object with SSE-C")

	downloadedData, err := io.ReadAll(getResp.Body)
	require.NoError(t, err, "Failed to read downloaded large object")
	getResp.Body.Close()

	assert.Equal(t, len(testData), len(downloadedData), "Downloaded large object size mismatch")
	assert.Equal(t, testData, string(downloadedData), "Downloaded large object content mismatch")
}

func TestSSECPartialContent(t *testing.T) {
	t.Skip("Range requests with SSE-C are not yet fully implemented - requires full object decryption")

	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	objectKey := "test-partial-content"
	testData := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

	// Upload object with SSE-C
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Body:                 strings.NewReader(testData),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to upload object with SSE-C")

	// Download partial content (bytes 10-19)
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:               aws.String(bucketName),
		Key:                  aws.String(objectKey),
		Range:                aws.String("bytes=10-19"),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(testSSEKey),
		SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
	})
	require.NoError(t, err, "Failed to download partial content with SSE-C")

	partialData, err := io.ReadAll(getResp.Body)
	require.NoError(t, err, "Failed to read partial content")
	getResp.Body.Close()

	expectedPartial := testData[10:20]
	assert.Equal(t, expectedPartial, string(partialData), "Partial content doesn't match expected range")
}

func TestSSECRoundTripIntegrity(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)

	// Test different data patterns
	testCases := []struct {
		name string
		data []byte
	}{
		{"binary_data", bytes.Repeat([]byte{0x00, 0x01, 0x02, 0xFF}, 256)},
		{"random_pattern", func() []byte {
			data := make([]byte, 1000)
			for i := range data {
				data[i] = byte(i%256 ^ i>>8)
			}
			return data
		}()},
		{"all_zeros", make([]byte, 1000)},
		{"all_ones", bytes.Repeat([]byte{0xFF}, 1000)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			objectKey := fmt.Sprintf("test-integrity-%s", tc.name)

			// Upload with SSE-C
			_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(objectKey),
				Body:                 bytes.NewReader(tc.data),
				SSECustomerAlgorithm: aws.String("AES256"),
				SSECustomerKey:       aws.String(testSSEKey),
				SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
			})
			require.NoError(t, err, "Failed to upload %s with SSE-C", tc.name)

			// Download and verify
			getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
				Bucket:               aws.String(bucketName),
				Key:                  aws.String(objectKey),
				SSECustomerAlgorithm: aws.String("AES256"),
				SSECustomerKey:       aws.String(testSSEKey),
				SSECustomerKeyMD5:    aws.String(testSSEKeyMD5),
			})
			require.NoError(t, err, "Failed to download %s with SSE-C", tc.name)

			downloadedData, err := io.ReadAll(getResp.Body)
			require.NoError(t, err, "Failed to read downloaded %s", tc.name)
			getResp.Body.Close()

			assert.Equal(t, tc.data, downloadedData, "Data integrity check failed for %s", tc.name)
		})
	}
}
