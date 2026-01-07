// Package etag_test provides integration tests for S3 ETag format validation.
//
// These tests verify that SeaweedFS returns correct ETag formats for different
// upload scenarios, ensuring compatibility with AWS S3 SDKs that validate ETags.
//
// Background (GitHub Issue #7768):
// AWS S3 SDK for Java v2 validates ETags as hexadecimal MD5 hashes for PutObject
// responses. SeaweedFS was incorrectly returning composite ETags ("<md5>-<count>")
// for regular PutObject when files were internally auto-chunked (>8MB), causing
// the SDK to fail with "Invalid base 16 character: '-'".
//
// Per AWS S3 specification:
// - Regular PutObject: ETag is always a pure MD5 hex string (32 chars)
// - Multipart Upload (CompleteMultipartUpload): ETag is "<md5>-<partcount>"
//
// These tests ensure this behavior is maintained.
package etag_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	mathrand "math/rand"
	"regexp"
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
	Endpoint     string
	AccessKey    string
	SecretKey    string
	Region       string
	BucketPrefix string
}

// Default test configuration
var defaultConfig = &S3TestConfig{
	Endpoint:     "http://127.0.0.1:8333",
	AccessKey:    "some_access_key1",
	SecretKey:    "some_secret_key1",
	Region:       "us-east-1",
	BucketPrefix: "test-etag-",
}

// Constants for auto-chunking thresholds (must match s3api_object_handlers_put.go)
const (
	// SeaweedFS auto-chunks files larger than 8MB
	autoChunkSize = 8 * 1024 * 1024

	// Test sizes
	smallFileSize  = 1 * 1024         // 1KB - single chunk
	mediumFileSize = 256 * 1024       // 256KB - single chunk (at threshold)
	largeFileSize  = 10 * 1024 * 1024 // 10MB - triggers auto-chunking (2 chunks)
	xlFileSize     = 25 * 1024 * 1024 // 25MB - triggers auto-chunking (4 chunks)
	multipartSize  = 5 * 1024 * 1024  // 5MB per part for multipart uploads
)

// ETag format patterns
var (
	// Pure MD5 ETag: 32 hex characters (with or without quotes)
	pureMD5Pattern = regexp.MustCompile(`^"?[a-f0-9]{32}"?$`)

	// Composite ETag for multipart: 32 hex chars, hyphen, part count (with or without quotes)
	compositePattern = regexp.MustCompile(`^"?[a-f0-9]{32}-\d+"?$`)
)

func init() {
	mathrand.Seed(time.Now().UnixNano())
}

// getS3Client creates an AWS S3 v2 client for testing
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
		o.UsePathStyle = true
	})
}

// getNewBucketName generates a unique bucket name
func getNewBucketName() string {
	timestamp := time.Now().UnixNano()
	randomSuffix := mathrand.Intn(100000)
	return fmt.Sprintf("%s%d-%d", defaultConfig.BucketPrefix, timestamp, randomSuffix)
}

// generateRandomData generates random test data of specified size
func generateRandomData(size int) []byte {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		panic(fmt.Sprintf("failed to generate random test data: %v", err))
	}
	return data
}

// calculateMD5 calculates the MD5 hash of data and returns hex string
func calculateMD5(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// cleanETag removes quotes from ETag
func cleanETag(etag string) string {
	return strings.Trim(etag, `"`)
}

// isPureMD5ETag checks if ETag is a pure MD5 hex string (no composite format)
func isPureMD5ETag(etag string) bool {
	return pureMD5Pattern.MatchString(etag)
}

// isCompositeETag checks if ETag is in composite format (md5-count)
func isCompositeETag(etag string) bool {
	return compositePattern.MatchString(etag)
}

// createTestBucket creates a new bucket for testing
func createTestBucket(ctx context.Context, client *s3.Client, bucketName string) error {
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	return err
}

// cleanupTestBucket deletes all objects and the bucket
func cleanupTestBucket(ctx context.Context, client *s3.Client, bucketName string) {
	// Delete all objects
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			break
		}
		for _, obj := range page.Contents {
			client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			})
		}
	}

	// Abort any in-progress multipart uploads
	mpPaginator := s3.NewListMultipartUploadsPaginator(client, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucketName),
	})
	for mpPaginator.HasMorePages() {
		page, err := mpPaginator.NextPage(ctx)
		if err != nil {
			break
		}
		for _, upload := range page.Uploads {
			client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucketName),
				Key:      upload.Key,
				UploadId: upload.UploadId,
			})
		}
	}

	// Delete bucket
	client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
}

// TestPutObjectETagFormat_SmallFile verifies ETag format for small files (single chunk)
func TestPutObjectETagFormat_SmallFile(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)

	bucketName := getNewBucketName()
	err := createTestBucket(ctx, client, bucketName)
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	testData := generateRandomData(smallFileSize)
	expectedMD5 := calculateMD5(testData)
	objectKey := "small-file.bin"

	// Upload small file
	putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err, "Failed to upload small file")

	// Verify ETag format
	etag := aws.ToString(putResp.ETag)
	t.Logf("Small file (%d bytes) ETag: %s", smallFileSize, etag)

	assert.True(t, isPureMD5ETag(etag),
		"Small file ETag should be pure MD5, got: %s", etag)
	assert.False(t, isCompositeETag(etag),
		"Small file ETag should NOT be composite format, got: %s", etag)
	assert.Equal(t, expectedMD5, cleanETag(etag),
		"ETag should match calculated MD5")
}

// TestPutObjectETagFormat_LargeFile verifies ETag format for large files that trigger auto-chunking
// This is the critical test for GitHub Issue #7768
func TestPutObjectETagFormat_LargeFile(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)

	bucketName := getNewBucketName()
	err := createTestBucket(ctx, client, bucketName)
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	testData := generateRandomData(largeFileSize)
	expectedMD5 := calculateMD5(testData)
	objectKey := "large-file.bin"

	t.Logf("Uploading large file (%d bytes, > %d byte auto-chunk threshold)...",
		largeFileSize, autoChunkSize)

	// Upload large file (triggers auto-chunking internally)
	putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err, "Failed to upload large file")

	// Verify ETag format - MUST be pure MD5, NOT composite
	etag := aws.ToString(putResp.ETag)
	t.Logf("Large file (%d bytes, ~%d internal chunks) ETag: %s",
		largeFileSize, (largeFileSize/autoChunkSize)+1, etag)

	assert.True(t, isPureMD5ETag(etag),
		"Large file PutObject ETag MUST be pure MD5 (not composite), got: %s", etag)
	assert.False(t, isCompositeETag(etag),
		"Large file PutObject ETag should NOT contain '-' (composite format), got: %s", etag)
	assert.False(t, strings.Contains(cleanETag(etag), "-"),
		"ETag should not contain hyphen for regular PutObject, got: %s", etag)
	assert.Equal(t, expectedMD5, cleanETag(etag),
		"ETag should match calculated MD5 of entire content")

	// Verify we can read back the object correctly
	getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to get large file")
	defer getResp.Body.Close()

	downloadedData, err := io.ReadAll(getResp.Body)
	require.NoError(t, err, "Failed to read large file content")
	assert.Equal(t, testData, downloadedData, "Downloaded content should match uploaded content")
}

// TestPutObjectETagFormat_ExtraLargeFile tests even larger files with multiple internal chunks
func TestPutObjectETagFormat_ExtraLargeFile(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)

	bucketName := getNewBucketName()
	err := createTestBucket(ctx, client, bucketName)
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	testData := generateRandomData(xlFileSize)
	expectedMD5 := calculateMD5(testData)
	objectKey := "xl-file.bin"

	expectedChunks := (xlFileSize / autoChunkSize) + 1
	t.Logf("Uploading XL file (%d bytes, expected ~%d internal chunks)...",
		xlFileSize, expectedChunks)

	// Upload extra large file
	putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err, "Failed to upload XL file")

	// Verify ETag format
	etag := aws.ToString(putResp.ETag)
	t.Logf("XL file (%d bytes) ETag: %s", xlFileSize, etag)

	assert.True(t, isPureMD5ETag(etag),
		"XL file PutObject ETag MUST be pure MD5, got: %s", etag)
	assert.False(t, isCompositeETag(etag),
		"XL file PutObject ETag should NOT be composite, got: %s", etag)
	assert.Equal(t, expectedMD5, cleanETag(etag),
		"ETag should match calculated MD5")
}

// TestMultipartUploadETagFormat verifies that ONLY multipart uploads get composite ETags
func TestMultipartUploadETagFormat(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)

	bucketName := getNewBucketName()
	err := createTestBucket(ctx, client, bucketName)
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Create test data for multipart upload (15MB = 3 parts of 5MB each)
	totalSize := 15 * 1024 * 1024
	testData := generateRandomData(totalSize)
	objectKey := "multipart-file.bin"

	expectedPartCount := (totalSize + multipartSize - 1) / multipartSize // ceiling division
	t.Logf("Performing multipart upload (%d bytes, %d parts)...",
		totalSize, expectedPartCount)

	// Initiate multipart upload
	createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Failed to create multipart upload")

	uploadId := createResp.UploadId
	var completedParts []types.CompletedPart
	partNumber := int32(1)

	// Upload parts
	for offset := 0; offset < totalSize; offset += multipartSize {
		end := offset + multipartSize
		if end > totalSize {
			end = totalSize
		}
		partData := testData[offset:end]

		uploadResp, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			UploadId:   uploadId,
			PartNumber: aws.Int32(partNumber),
			Body:       bytes.NewReader(partData),
		})
		require.NoError(t, err, "Failed to upload part %d", partNumber)

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int32(partNumber),
		})
		partNumber++
	}

	// Complete multipart upload
	completeResp, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: uploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	require.NoError(t, err, "Failed to complete multipart upload")

	// Verify ETag format - SHOULD be composite for multipart
	etag := aws.ToString(completeResp.ETag)
	t.Logf("Multipart upload ETag: %s", etag)

	assert.True(t, isCompositeETag(etag),
		"Multipart upload ETag SHOULD be composite format (md5-count), got: %s", etag)
	assert.True(t, strings.Contains(cleanETag(etag), "-"),
		"Multipart ETag should contain hyphen, got: %s", etag)

	// Verify the part count in the ETag matches
	parts := strings.Split(cleanETag(etag), "-")
	require.Len(t, parts, 2, "Composite ETag should have format 'hash-count'")
	assert.Equal(t, fmt.Sprintf("%d", len(completedParts)), parts[1],
		"Part count in ETag should match number of parts uploaded")
}

// TestPutObjectETagConsistency verifies ETag consistency between PUT and GET
func TestPutObjectETagConsistency(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)

	bucketName := getNewBucketName()
	err := createTestBucket(ctx, client, bucketName)
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	testCases := []struct {
		name string
		size int
	}{
		{"tiny", 100},
		{"small", smallFileSize},
		{"medium", mediumFileSize},
		{"large", largeFileSize},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testData := generateRandomData(tc.size)
			objectKey := fmt.Sprintf("consistency-test-%s.bin", tc.name)

			// PUT object
			putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
				Body:   bytes.NewReader(testData),
			})
			require.NoError(t, err)
			putETag := aws.ToString(putResp.ETag)

			// HEAD object
			headResp, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
			})
			require.NoError(t, err)
			headETag := aws.ToString(headResp.ETag)

			// GET object
			getResp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
			})
			require.NoError(t, err)
			getETag := aws.ToString(getResp.ETag)
			getResp.Body.Close()

			// All ETags should match
			t.Logf("%s (%d bytes): PUT=%s, HEAD=%s, GET=%s",
				tc.name, tc.size, putETag, headETag, getETag)

			assert.Equal(t, putETag, headETag,
				"PUT and HEAD ETags should match")
			assert.Equal(t, putETag, getETag,
				"PUT and GET ETags should match")

			// All should be pure MD5 (not composite) for regular PutObject
			assert.True(t, isPureMD5ETag(putETag),
				"PutObject ETag should be pure MD5, got: %s", putETag)
		})
	}
}

// TestETagHexValidation simulates the AWS SDK v2 validation that caused issue #7768
func TestETagHexValidation(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)

	bucketName := getNewBucketName()
	err := createTestBucket(ctx, client, bucketName)
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Test with a file large enough to trigger auto-chunking
	testData := generateRandomData(largeFileSize)
	objectKey := "hex-validation-test.bin"

	putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(testData),
	})
	require.NoError(t, err)

	etag := cleanETag(aws.ToString(putResp.ETag))

	// Simulate AWS SDK v2's hex validation (Base16Codec.decode)
	// This is what fails in issue #7768 when ETag contains '-'
	t.Logf("Validating ETag as hex: %s", etag)

	_, err = hex.DecodeString(etag)
	assert.NoError(t, err,
		"ETag should be valid hexadecimal (AWS SDK v2 validation). "+
			"Got ETag: %s. If this fails with 'invalid byte', the ETag contains non-hex chars like '-'",
		etag)
}

// TestMultipleLargeFileUploads verifies ETag format across multiple large uploads
func TestMultipleLargeFileUploads(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)

	bucketName := getNewBucketName()
	err := createTestBucket(ctx, client, bucketName)
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	numFiles := 5
	for i := 0; i < numFiles; i++ {
		testData := generateRandomData(largeFileSize)
		expectedMD5 := calculateMD5(testData)
		objectKey := fmt.Sprintf("large-file-%d.bin", i)

		putResp, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   bytes.NewReader(testData),
		})
		require.NoError(t, err, "Failed to upload file %d", i)

		etag := aws.ToString(putResp.ETag)
		t.Logf("File %d ETag: %s (expected MD5: %s)", i, etag, expectedMD5)

		assert.True(t, isPureMD5ETag(etag),
			"File %d ETag should be pure MD5, got: %s", i, etag)
		assert.Equal(t, expectedMD5, cleanETag(etag),
			"File %d ETag should match MD5", i)

		// Validate as hex (AWS SDK v2 check)
		_, err = hex.DecodeString(cleanETag(etag))
		assert.NoError(t, err, "File %d ETag should be valid hex", i)
	}
}
