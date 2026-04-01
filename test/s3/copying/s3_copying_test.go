package copying_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mathrand "math/rand"
	"net/url"
	"os"
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

// Default test configuration - should match test_config.json
var defaultConfig = &S3TestConfig{
	Endpoint:      "http://127.0.0.1:8000", // Use explicit IPv4 address
	AccessKey:     "some_access_key1",
	SecretKey:     "some_secret_key1",
	Region:        "us-east-1",
	BucketPrefix:  "test-copying-",
	UseSSL:        false,
	SkipVerifySSL: true,
}

func init() {
	mathrand.Seed(time.Now().UnixNano())
	if endpoint := os.Getenv("S3_ENDPOINT"); endpoint != "" {
		defaultConfig.Endpoint = endpoint
	}
}

// getS3Client creates an AWS S3 client for testing
func getS3Client(t *testing.T) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(defaultConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			defaultConfig.AccessKey,
			defaultConfig.SecretKey,
			"",
		)),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               defaultConfig.Endpoint,
					SigningRegion:     defaultConfig.Region,
					HostnameImmutable: true,
				}, nil
			})),
	)
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true // Important for SeaweedFS
	})
}

// waitForS3Service waits for the S3 service to be ready
func waitForS3Service(t *testing.T, client *s3.Client, timeout time.Duration) {
	start := time.Now()
	for time.Since(start) < timeout {
		_, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
		if err == nil {
			return
		}
		t.Logf("Waiting for S3 service to be ready... (error: %v)", err)
		time.Sleep(time.Second)
	}
	t.Fatalf("S3 service not ready after %v", timeout)
}

// getNewBucketName generates a unique bucket name
func getNewBucketName() string {
	timestamp := time.Now().UnixNano()
	// Add random suffix to prevent collisions when tests run quickly
	randomSuffix := mathrand.Intn(100000)
	return fmt.Sprintf("%s%d-%d", defaultConfig.BucketPrefix, timestamp, randomSuffix)
}

// cleanupTestBuckets removes any leftover test buckets from previous runs
func cleanupTestBuckets(t *testing.T, client *s3.Client) {
	resp, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		t.Logf("Warning: failed to list buckets for cleanup: %v", err)
		return
	}

	for _, bucket := range resp.Buckets {
		bucketName := *bucket.Name
		// Only delete buckets that match our test prefix
		if strings.HasPrefix(bucketName, defaultConfig.BucketPrefix) {
			t.Logf("Cleaning up leftover test bucket: %s", bucketName)
			deleteBucket(t, client, bucketName)
		}
	}
}

// createBucket creates a new bucket for testing
func createBucket(t *testing.T, client *s3.Client, bucketName string) {
	// First, try to delete the bucket if it exists (cleanup from previous failed tests)
	deleteBucket(t, client, bucketName)

	// Create the bucket
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
}

// deleteBucket deletes a bucket and all its contents
func deleteBucket(t *testing.T, client *s3.Client, bucketName string) {
	// First, delete all objects
	deleteAllObjects(t, client, bucketName)

	// Then delete the bucket
	_, err := client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		// Only log warnings for actual errors, not "bucket doesn't exist"
		if !strings.Contains(err.Error(), "NoSuchBucket") {
			t.Logf("Warning: failed to delete bucket %s: %v", bucketName, err)
		}
	}
}

// deleteAllObjects deletes all objects in a bucket
func deleteAllObjects(t *testing.T, client *s3.Client, bucketName string) {
	// List all objects
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			// Only log warnings for actual errors, not "bucket doesn't exist"
			if !strings.Contains(err.Error(), "NoSuchBucket") {
				t.Logf("Warning: failed to list objects in bucket %s: %v", bucketName, err)
			}
			return
		}

		if len(page.Contents) == 0 {
			break
		}

		var objectsToDelete []types.ObjectIdentifier
		for _, obj := range page.Contents {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		// Delete objects in batches
		if len(objectsToDelete) > 0 {
			_, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
				Bucket: aws.String(bucketName),
				Delete: &types.Delete{
					Objects: objectsToDelete,
					Quiet:   aws.Bool(true),
				},
			})
			if err != nil {
				t.Logf("Warning: failed to delete objects in bucket %s: %v", bucketName, err)
			}
		}
	}
}

// putObject puts an object into a bucket
func putObject(t *testing.T, client *s3.Client, bucketName, key, content string) *s3.PutObjectOutput {
	resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	})
	require.NoError(t, err)
	return resp
}

// putObjectWithMetadata puts an object with metadata into a bucket
func putObjectWithMetadata(t *testing.T, client *s3.Client, bucketName, key, content string, metadata map[string]string, contentType string) *s3.PutObjectOutput {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	}

	if metadata != nil {
		input.Metadata = metadata
	}

	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}

	resp, err := client.PutObject(context.TODO(), input)
	require.NoError(t, err)
	return resp
}

// getObject gets an object from a bucket
func getObject(t *testing.T, client *s3.Client, bucketName, key string) *s3.GetObjectOutput {
	resp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	return resp
}

// getObjectBody gets the body content of an object
func getObjectBody(t *testing.T, resp *s3.GetObjectOutput) string {
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()
	return string(body)
}

// generateRandomData generates random data of specified size
func generateRandomData(size int) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		panic(err)
	}
	return data
}

// createCopySource creates a properly URL-encoded copy source string
func createCopySource(bucketName, key string) string {
	// URL encode the key to handle special characters like spaces
	encodedKey := url.PathEscape(key)
	return fmt.Sprintf("%s/%s", bucketName, encodedKey)
}

// TestBasicPutGet tests basic S3 put and get operations
func TestBasicPutGet(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Test 1: Put and get a simple text object
	t.Run("Simple text object", func(t *testing.T) {
		key := "test-simple.txt"
		content := "Hello, SeaweedFS S3!"

		// Put object
		putResp := putObject(t, client, bucketName, key, content)
		assert.NotNil(t, putResp.ETag)

		// Get object
		getResp := getObject(t, client, bucketName, key)
		body := getObjectBody(t, getResp)
		assert.Equal(t, content, body)
		assert.Equal(t, putResp.ETag, getResp.ETag)
	})

	// Test 2: Put and get an empty object
	t.Run("Empty object", func(t *testing.T) {
		key := "test-empty.txt"
		content := ""

		putResp := putObject(t, client, bucketName, key, content)
		assert.NotNil(t, putResp.ETag)

		getResp := getObject(t, client, bucketName, key)
		body := getObjectBody(t, getResp)
		assert.Equal(t, content, body)
		assert.Equal(t, putResp.ETag, getResp.ETag)
	})

	// Test 3: Put and get a binary object
	t.Run("Binary object", func(t *testing.T) {
		key := "test-binary.bin"
		content := string(generateRandomData(1024)) // 1KB of random data

		putResp := putObject(t, client, bucketName, key, content)
		assert.NotNil(t, putResp.ETag)

		getResp := getObject(t, client, bucketName, key)
		body := getObjectBody(t, getResp)
		assert.Equal(t, content, body)
		assert.Equal(t, putResp.ETag, getResp.ETag)
	})

	// Test 4: Put and get object with metadata
	t.Run("Object with metadata", func(t *testing.T) {
		key := "test-metadata.txt"
		content := "Content with metadata"
		metadata := map[string]string{
			"author":      "test",
			"description": "test object with metadata",
		}
		contentType := "text/plain"

		putResp := putObjectWithMetadata(t, client, bucketName, key, content, metadata, contentType)
		assert.NotNil(t, putResp.ETag)

		getResp := getObject(t, client, bucketName, key)
		body := getObjectBody(t, getResp)
		assert.Equal(t, content, body)
		assert.Equal(t, putResp.ETag, getResp.ETag)
		assert.Equal(t, contentType, *getResp.ContentType)
		assert.Equal(t, metadata["author"], getResp.Metadata["author"])
		assert.Equal(t, metadata["description"], getResp.Metadata["description"])
	})
}

// TestBasicBucketOperations tests basic bucket operations
func TestBasicBucketOperations(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Test 1: Create bucket
	t.Run("Create bucket", func(t *testing.T) {
		createBucket(t, client, bucketName)

		// Verify bucket exists by listing buckets
		resp, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
		require.NoError(t, err)

		found := false
		for _, bucket := range resp.Buckets {
			if *bucket.Name == bucketName {
				found = true
				break
			}
		}
		assert.True(t, found, "Bucket should exist after creation")
	})

	// Test 2: Put objects and list them
	t.Run("List objects", func(t *testing.T) {
		// Put multiple objects
		objects := []string{"test1.txt", "test2.txt", "dir/test3.txt"}
		for _, key := range objects {
			putObject(t, client, bucketName, key, fmt.Sprintf("content of %s", key))
		}

		// List objects
		resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err)

		assert.Equal(t, len(objects), len(resp.Contents))

		// Verify each object exists
		for _, obj := range resp.Contents {
			found := false
			for _, expected := range objects {
				if *obj.Key == expected {
					found = true
					break
				}
			}
			assert.True(t, found, "Object %s should be in list", *obj.Key)
		}
	})

	// Test 3: Delete bucket (cleanup)
	t.Run("Delete bucket", func(t *testing.T) {
		deleteBucket(t, client, bucketName)

		// Verify bucket is deleted by trying to list its contents
		_, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		assert.Error(t, err, "Bucket should not exist after deletion")
	})
}

// TestBasicLargeObject tests handling of larger objects (up to volume limit)
func TestBasicLargeObject(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Test with progressively larger objects
	sizes := []int{
		1024,             // 1KB
		1024 * 10,        // 10KB
		1024 * 100,       // 100KB
		1024 * 1024,      // 1MB
		1024 * 1024 * 5,  // 5MB
		1024 * 1024 * 10, // 10MB
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%dMB", size/(1024*1024)), func(t *testing.T) {
			key := fmt.Sprintf("large-object-%d.bin", size)
			content := string(generateRandomData(size))

			putResp := putObject(t, client, bucketName, key, content)
			assert.NotNil(t, putResp.ETag)

			getResp := getObject(t, client, bucketName, key)
			body := getObjectBody(t, getResp)
			assert.Equal(t, len(content), len(body))
			assert.Equal(t, content, body)
			assert.Equal(t, putResp.ETag, getResp.ETag)
		})
	}
}

// TestObjectCopySameBucket tests copying an object within the same bucket
func TestObjectCopySameBucket(t *testing.T) {
	client := getS3Client(t)

	// Wait for S3 service to be ready
	waitForS3Service(t, client, 30*time.Second)

	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Put source object
	sourceKey := "foo123bar"
	sourceContent := "foo"
	putObject(t, client, bucketName, sourceKey, sourceContent)

	// Copy object within the same bucket
	destKey := "bar321foo"
	copySource := createCopySource(bucketName, sourceKey)
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		CopySource: aws.String(copySource),
	})
	require.NoError(t, err, "Failed to copy object within same bucket")

	// Verify the copied object
	resp := getObject(t, client, bucketName, destKey)
	body := getObjectBody(t, resp)
	assert.Equal(t, sourceContent, body)
}

// TestObjectCopyDiffBucket tests copying an object to a different bucket
func TestObjectCopyDiffBucket(t *testing.T) {
	client := getS3Client(t)
	sourceBucketName := getNewBucketName()
	destBucketName := getNewBucketName()

	// Create buckets
	createBucket(t, client, sourceBucketName)
	defer deleteBucket(t, client, sourceBucketName)
	createBucket(t, client, destBucketName)
	defer deleteBucket(t, client, destBucketName)

	// Put source object
	sourceKey := "foo123bar"
	sourceContent := "foo"
	putObject(t, client, sourceBucketName, sourceKey, sourceContent)

	// Copy object to different bucket
	destKey := "bar321foo"
	copySource := createCopySource(sourceBucketName, sourceKey)
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(destBucketName),
		Key:        aws.String(destKey),
		CopySource: aws.String(copySource),
	})
	require.NoError(t, err)

	// Verify the copied object
	resp := getObject(t, client, destBucketName, destKey)
	body := getObjectBody(t, resp)
	assert.Equal(t, sourceContent, body)
}

// TestObjectCopyCannedAcl tests copying with ACL settings
func TestObjectCopyCannedAcl(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Put source object
	sourceKey := "foo123bar"
	sourceContent := "foo"
	putObject(t, client, bucketName, sourceKey, sourceContent)

	// Copy object with public-read ACL
	destKey := "bar321foo"
	copySource := createCopySource(bucketName, sourceKey)
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(destKey),
		CopySource: aws.String(copySource),
		ACL:        types.ObjectCannedACLPublicRead,
	})
	require.NoError(t, err)

	// Verify the copied object
	resp := getObject(t, client, bucketName, destKey)
	body := getObjectBody(t, resp)
	assert.Equal(t, sourceContent, body)

	// Test metadata replacement with ACL
	metadata := map[string]string{"abc": "def"}
	destKey2 := "foo123bar2"
	copySource2 := createCopySource(bucketName, destKey)
	_, err = client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(destKey2),
		CopySource:        aws.String(copySource2),
		ACL:               types.ObjectCannedACLPublicRead,
		Metadata:          metadata,
		MetadataDirective: types.MetadataDirectiveReplace,
	})
	require.NoError(t, err)

	// Verify the copied object with metadata
	resp2 := getObject(t, client, bucketName, destKey2)
	body2 := getObjectBody(t, resp2)
	assert.Equal(t, sourceContent, body2)
	assert.Equal(t, metadata, resp2.Metadata)
}

// TestObjectCopyRetainingMetadata tests copying while retaining metadata
func TestObjectCopyRetainingMetadata(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Test with different sizes
	sizes := []int{3, 1024 * 1024} // 3 bytes and 1MB
	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			sourceKey := fmt.Sprintf("foo123bar_%d", size)
			sourceContent := string(generateRandomData(size))
			contentType := "audio/ogg"
			metadata := map[string]string{"key1": "value1", "key2": "value2"}

			// Put source object with metadata
			putObjectWithMetadata(t, client, bucketName, sourceKey, sourceContent, metadata, contentType)

			// Copy object (should retain metadata)
			destKey := fmt.Sprintf("bar321foo_%d", size)
			copySource := createCopySource(bucketName, sourceKey)
			_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
				Bucket:     aws.String(bucketName),
				Key:        aws.String(destKey),
				CopySource: aws.String(copySource),
			})
			require.NoError(t, err)

			// Verify the copied object
			resp := getObject(t, client, bucketName, destKey)
			body := getObjectBody(t, resp)
			assert.Equal(t, sourceContent, body)
			assert.Equal(t, contentType, *resp.ContentType)
			assert.Equal(t, metadata, resp.Metadata)
			require.NotNil(t, resp.ContentLength)
			assert.Equal(t, int64(size), *resp.ContentLength)
		})
	}
}

// TestMultipartCopySmall tests multipart copying of small files
func TestMultipartCopySmall(t *testing.T) {
	client := getS3Client(t)

	// Clean up any leftover buckets from previous test runs
	cleanupTestBuckets(t, client)

	sourceBucketName := getNewBucketName()
	destBucketName := getNewBucketName()

	// Create buckets
	createBucket(t, client, sourceBucketName)
	defer deleteBucket(t, client, sourceBucketName)
	createBucket(t, client, destBucketName)
	defer deleteBucket(t, client, destBucketName)

	// Put source object
	sourceKey := "foo"
	sourceContent := "x" // 1 byte
	putObject(t, client, sourceBucketName, sourceKey, sourceContent)

	// Create multipart upload
	destKey := "mymultipart"
	createResp, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(destBucketName),
		Key:    aws.String(destKey),
	})
	require.NoError(t, err)
	uploadID := *createResp.UploadId

	// Upload part copy
	copySource := createCopySource(sourceBucketName, sourceKey)
	copyResp, err := client.UploadPartCopy(context.TODO(), &s3.UploadPartCopyInput{
		Bucket:          aws.String(destBucketName),
		Key:             aws.String(destKey),
		UploadId:        aws.String(uploadID),
		PartNumber:      aws.Int32(1),
		CopySource:      aws.String(copySource),
		CopySourceRange: aws.String("bytes=0-0"),
	})
	require.NoError(t, err)

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(destBucketName),
		Key:      aws.String(destKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					ETag:       copyResp.CopyPartResult.ETag,
					PartNumber: aws.Int32(1),
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify the copied object
	resp := getObject(t, client, destBucketName, destKey)
	body := getObjectBody(t, resp)
	assert.Equal(t, sourceContent, body)
	require.NotNil(t, resp.ContentLength)
	assert.Equal(t, int64(1), *resp.ContentLength)
}

// TestMultipartCopyWithoutRange tests multipart copying without range specification
func TestMultipartCopyWithoutRange(t *testing.T) {
	client := getS3Client(t)

	// Clean up any leftover buckets from previous test runs
	cleanupTestBuckets(t, client)

	sourceBucketName := getNewBucketName()
	destBucketName := getNewBucketName()

	// Create buckets
	createBucket(t, client, sourceBucketName)
	defer deleteBucket(t, client, sourceBucketName)
	createBucket(t, client, destBucketName)
	defer deleteBucket(t, client, destBucketName)

	// Put source object
	sourceKey := "source"
	sourceContent := string(generateRandomData(10))
	putObject(t, client, sourceBucketName, sourceKey, sourceContent)

	// Create multipart upload
	destKey := "mymultipartcopy"
	createResp, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(destBucketName),
		Key:    aws.String(destKey),
	})
	require.NoError(t, err)
	uploadID := *createResp.UploadId

	// Upload part copy without range (should copy entire object)
	copySource := createCopySource(sourceBucketName, sourceKey)
	copyResp, err := client.UploadPartCopy(context.TODO(), &s3.UploadPartCopyInput{
		Bucket:     aws.String(destBucketName),
		Key:        aws.String(destKey),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(1),
		CopySource: aws.String(copySource),
	})
	require.NoError(t, err)

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(destBucketName),
		Key:      aws.String(destKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					ETag:       copyResp.CopyPartResult.ETag,
					PartNumber: aws.Int32(1),
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify the copied object
	resp := getObject(t, client, destBucketName, destKey)
	body := getObjectBody(t, resp)
	assert.Equal(t, sourceContent, body)
	require.NotNil(t, resp.ContentLength)
	assert.Equal(t, int64(10), *resp.ContentLength)
}

// TestMultipartCopySpecialNames tests multipart copying with special character names
func TestMultipartCopySpecialNames(t *testing.T) {
	client := getS3Client(t)

	// Clean up any leftover buckets from previous test runs
	cleanupTestBuckets(t, client)

	sourceBucketName := getNewBucketName()
	destBucketName := getNewBucketName()

	// Create buckets
	createBucket(t, client, sourceBucketName)
	defer deleteBucket(t, client, sourceBucketName)
	createBucket(t, client, destBucketName)
	defer deleteBucket(t, client, destBucketName)

	// Test with special key names
	specialKeys := []string{" ", "_", "__", "?versionId"}
	sourceContent := "x" // 1 byte
	destKey := "mymultipart"

	for i, sourceKey := range specialKeys {
		t.Run(fmt.Sprintf("special_key_%d", i), func(t *testing.T) {
			// Put source object
			putObject(t, client, sourceBucketName, sourceKey, sourceContent)

			// Create multipart upload
			createResp, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
				Bucket: aws.String(destBucketName),
				Key:    aws.String(destKey),
			})
			require.NoError(t, err)
			uploadID := *createResp.UploadId

			// Upload part copy
			copySource := createCopySource(sourceBucketName, sourceKey)
			copyResp, err := client.UploadPartCopy(context.TODO(), &s3.UploadPartCopyInput{
				Bucket:          aws.String(destBucketName),
				Key:             aws.String(destKey),
				UploadId:        aws.String(uploadID),
				PartNumber:      aws.Int32(1),
				CopySource:      aws.String(copySource),
				CopySourceRange: aws.String("bytes=0-0"),
			})
			require.NoError(t, err)

			// Complete multipart upload
			_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
				Bucket:   aws.String(destBucketName),
				Key:      aws.String(destKey),
				UploadId: aws.String(uploadID),
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{
							ETag:       copyResp.CopyPartResult.ETag,
							PartNumber: aws.Int32(1),
						},
					},
				},
			})
			require.NoError(t, err)

			// Verify the copied object
			resp := getObject(t, client, destBucketName, destKey)
			body := getObjectBody(t, resp)
			assert.Equal(t, sourceContent, body)
			require.NotNil(t, resp.ContentLength)
			assert.Equal(t, int64(1), *resp.ContentLength)
		})
	}
}

// TestMultipartCopyMultipleSizes tests multipart copying with various file sizes
func TestMultipartCopyMultipleSizes(t *testing.T) {
	client := getS3Client(t)

	// Clean up any leftover buckets from previous test runs
	cleanupTestBuckets(t, client)

	sourceBucketName := getNewBucketName()
	destBucketName := getNewBucketName()

	// Create buckets
	createBucket(t, client, sourceBucketName)
	defer deleteBucket(t, client, sourceBucketName)
	createBucket(t, client, destBucketName)
	defer deleteBucket(t, client, destBucketName)

	// Put source object (12MB)
	sourceKey := "foo"
	sourceSize := 12 * 1024 * 1024
	sourceContent := generateRandomData(sourceSize)
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(sourceBucketName),
		Key:    aws.String(sourceKey),
		Body:   bytes.NewReader(sourceContent),
	})
	require.NoError(t, err)

	destKey := "mymultipart"
	partSize := 5 * 1024 * 1024 // 5MB parts

	// Test different copy sizes
	testSizes := []int{
		5 * 1024 * 1024,         // 5MB
		5*1024*1024 + 100*1024,  // 5MB + 100KB
		5*1024*1024 + 600*1024,  // 5MB + 600KB
		10*1024*1024 + 100*1024, // 10MB + 100KB
		10*1024*1024 + 600*1024, // 10MB + 600KB
		10 * 1024 * 1024,        // 10MB
	}

	for _, size := range testSizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			// Create multipart upload
			createResp, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
				Bucket: aws.String(destBucketName),
				Key:    aws.String(destKey),
			})
			require.NoError(t, err)
			uploadID := *createResp.UploadId

			// Upload parts
			var parts []types.CompletedPart
			copySource := createCopySource(sourceBucketName, sourceKey)

			for i := 0; i < size; i += partSize {
				partNum := int32(len(parts) + 1)
				endOffset := i + partSize - 1
				if endOffset >= size {
					endOffset = size - 1
				}

				copyRange := fmt.Sprintf("bytes=%d-%d", i, endOffset)
				copyResp, err := client.UploadPartCopy(context.TODO(), &s3.UploadPartCopyInput{
					Bucket:          aws.String(destBucketName),
					Key:             aws.String(destKey),
					UploadId:        aws.String(uploadID),
					PartNumber:      aws.Int32(partNum),
					CopySource:      aws.String(copySource),
					CopySourceRange: aws.String(copyRange),
				})
				require.NoError(t, err)

				parts = append(parts, types.CompletedPart{
					ETag:       copyResp.CopyPartResult.ETag,
					PartNumber: aws.Int32(partNum),
				})
			}

			// Complete multipart upload
			_, err = client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
				Bucket:   aws.String(destBucketName),
				Key:      aws.String(destKey),
				UploadId: aws.String(uploadID),
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: parts,
				},
			})
			require.NoError(t, err)

			// Verify the copied object
			resp := getObject(t, client, destBucketName, destKey)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			resp.Body.Close()

			require.NotNil(t, resp.ContentLength)
			assert.Equal(t, int64(size), *resp.ContentLength)
			assert.Equal(t, sourceContent[:size], body)
		})
	}
}

// TestCopyObjectIfMatchGood tests copying with matching ETag condition
func TestCopyObjectIfMatchGood(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Put source object
	sourceKey := "foo"
	sourceContent := "bar"
	putResp := putObject(t, client, bucketName, sourceKey, sourceContent)

	// Copy object with matching ETag
	destKey := "bar"
	copySource := createCopySource(bucketName, sourceKey)
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(destKey),
		CopySource:        aws.String(copySource),
		CopySourceIfMatch: putResp.ETag,
	})
	require.NoError(t, err)

	// Verify the copied object
	resp := getObject(t, client, bucketName, destKey)
	body := getObjectBody(t, resp)
	assert.Equal(t, sourceContent, body)
}

// TestCopyObjectIfNoneMatchFailed tests copying with non-matching ETag condition
func TestCopyObjectIfNoneMatchFailed(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Put source object
	sourceKey := "foo"
	sourceContent := "bar"
	putObject(t, client, bucketName, sourceKey, sourceContent)

	// Copy object with non-matching ETag (should succeed)
	destKey := "bar"
	copySource := createCopySource(bucketName, sourceKey)
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:                aws.String(bucketName),
		Key:                   aws.String(destKey),
		CopySource:            aws.String(copySource),
		CopySourceIfNoneMatch: aws.String("ABCORZ"),
	})
	require.NoError(t, err)

	// Verify the copied object
	resp := getObject(t, client, bucketName, destKey)
	body := getObjectBody(t, resp)
	assert.Equal(t, sourceContent, body)
}

// TestCopyObjectIfMatchFailed tests copying with non-matching ETag condition (should fail)
func TestCopyObjectIfMatchFailed(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Put source object
	sourceKey := "foo"
	sourceContent := "bar"
	putObject(t, client, bucketName, sourceKey, sourceContent)

	// Copy object with non-matching ETag (should fail)
	destKey := "bar"
	copySource := createCopySource(bucketName, sourceKey)
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:            aws.String(bucketName),
		Key:               aws.String(destKey),
		CopySource:        aws.String(copySource),
		CopySourceIfMatch: aws.String("ABCORZ"),
	})

	// Should fail with precondition failed
	require.Error(t, err)
	// Note: We could check for specific error types, but SeaweedFS might return different error codes
}

// TestCopyObjectIfNoneMatchGood tests copying with matching ETag condition (should fail)
func TestCopyObjectIfNoneMatchGood(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Put source object
	sourceKey := "foo"
	sourceContent := "bar"
	putResp := putObject(t, client, bucketName, sourceKey, sourceContent)

	// Copy object with matching ETag for IfNoneMatch (should fail)
	destKey := "bar"
	copySource := createCopySource(bucketName, sourceKey)
	_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:                aws.String(bucketName),
		Key:                   aws.String(destKey),
		CopySource:            aws.String(copySource),
		CopySourceIfNoneMatch: putResp.ETag,
	})

	// Should fail with precondition failed
	require.Error(t, err)
}
