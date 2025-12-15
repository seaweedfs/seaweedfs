package retention

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutObjectWithLockHeaders tests that object lock headers in PUT requests
// are properly stored and returned in HEAD responses
func TestPutObjectWithLockHeaders(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket with object lock enabled and versioning
	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	key := "test-object-lock-headers"
	content := "test content with object lock headers"
	retainUntilDate := time.Now().Add(24 * time.Hour)

	// Test 1: PUT with COMPLIANCE mode and retention date
	t.Run("PUT with COMPLIANCE mode", func(t *testing.T) {
		testKey := key + "-compliance"

		// PUT object with lock headers
		putResp := putObjectWithLockHeaders(t, client, bucketName, testKey, content,
			"COMPLIANCE", retainUntilDate, "")
		require.NotNil(t, putResp.VersionId)

		// HEAD object and verify lock headers are returned
		headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(testKey),
		})
		require.NoError(t, err)

		// Verify object lock metadata is present in response
		assert.Equal(t, types.ObjectLockModeCompliance, headResp.ObjectLockMode)
		assert.NotNil(t, headResp.ObjectLockRetainUntilDate)
		assert.WithinDuration(t, retainUntilDate, *headResp.ObjectLockRetainUntilDate, 5*time.Second)
	})

	// Test 2: PUT with GOVERNANCE mode and retention date
	t.Run("PUT with GOVERNANCE mode", func(t *testing.T) {
		testKey := key + "-governance"

		putResp := putObjectWithLockHeaders(t, client, bucketName, testKey, content,
			"GOVERNANCE", retainUntilDate, "")
		require.NotNil(t, putResp.VersionId)

		headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(testKey),
		})
		require.NoError(t, err)

		assert.Equal(t, types.ObjectLockModeGovernance, headResp.ObjectLockMode)
		assert.NotNil(t, headResp.ObjectLockRetainUntilDate)
		assert.WithinDuration(t, retainUntilDate, *headResp.ObjectLockRetainUntilDate, 5*time.Second)
	})

	// Test 3: PUT with legal hold
	t.Run("PUT with legal hold", func(t *testing.T) {
		testKey := key + "-legal-hold"

		putResp := putObjectWithLockHeaders(t, client, bucketName, testKey, content,
			"", time.Time{}, "ON")
		require.NotNil(t, putResp.VersionId)

		headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(testKey),
		})
		require.NoError(t, err)

		assert.Equal(t, types.ObjectLockLegalHoldStatusOn, headResp.ObjectLockLegalHoldStatus)
	})

	// Test 4: PUT with both retention and legal hold
	t.Run("PUT with both retention and legal hold", func(t *testing.T) {
		testKey := key + "-both"

		putResp := putObjectWithLockHeaders(t, client, bucketName, testKey, content,
			"GOVERNANCE", retainUntilDate, "ON")
		require.NotNil(t, putResp.VersionId)

		headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(testKey),
		})
		require.NoError(t, err)

		assert.Equal(t, types.ObjectLockModeGovernance, headResp.ObjectLockMode)
		assert.NotNil(t, headResp.ObjectLockRetainUntilDate)
		assert.Equal(t, types.ObjectLockLegalHoldStatusOn, headResp.ObjectLockLegalHoldStatus)
	})
}

// TestGetObjectWithLockHeaders verifies that GET requests also return object lock metadata
func TestGetObjectWithLockHeaders(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	key := "test-get-object-lock"
	content := "test content for GET with lock headers"
	retainUntilDate := time.Now().Add(24 * time.Hour)

	// PUT object with lock headers
	putResp := putObjectWithLockHeaders(t, client, bucketName, key, content,
		"COMPLIANCE", retainUntilDate, "ON")
	require.NotNil(t, putResp.VersionId)

	// GET object and verify lock headers are returned
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	// Verify object lock metadata is present in GET response
	assert.Equal(t, types.ObjectLockModeCompliance, getResp.ObjectLockMode)
	assert.NotNil(t, getResp.ObjectLockRetainUntilDate)
	assert.WithinDuration(t, retainUntilDate, *getResp.ObjectLockRetainUntilDate, 5*time.Second)
	assert.Equal(t, types.ObjectLockLegalHoldStatusOn, getResp.ObjectLockLegalHoldStatus)
}

// TestVersionedObjectLockHeaders tests object lock headers work with versioned objects
func TestVersionedObjectLockHeaders(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	key := "test-versioned-lock"
	content1 := "version 1 content"
	content2 := "version 2 content"
	retainUntilDate1 := time.Now().Add(12 * time.Hour)
	retainUntilDate2 := time.Now().Add(24 * time.Hour)

	// PUT first version with GOVERNANCE mode
	putResp1 := putObjectWithLockHeaders(t, client, bucketName, key, content1,
		"GOVERNANCE", retainUntilDate1, "")
	require.NotNil(t, putResp1.VersionId)

	// PUT second version with COMPLIANCE mode
	putResp2 := putObjectWithLockHeaders(t, client, bucketName, key, content2,
		"COMPLIANCE", retainUntilDate2, "ON")
	require.NotNil(t, putResp2.VersionId)
	require.NotEqual(t, *putResp1.VersionId, *putResp2.VersionId)

	// HEAD latest version (version 2)
	headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockModeCompliance, headResp.ObjectLockMode)
	assert.Equal(t, types.ObjectLockLegalHoldStatusOn, headResp.ObjectLockLegalHoldStatus)

	// HEAD specific version 1
	headResp1, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: putResp1.VersionId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockModeGovernance, headResp1.ObjectLockMode)
	assert.NotEqual(t, types.ObjectLockLegalHoldStatusOn, headResp1.ObjectLockLegalHoldStatus)
}

// TestObjectLockHeadersErrorCases tests various error scenarios
func TestObjectLockHeadersErrorCases(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	key := "test-error-cases"
	content := "test content for error cases"

	// Test 1: Invalid retention mode should be rejected
	t.Run("Invalid retention mode", func(t *testing.T) {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:                    aws.String(bucketName),
			Key:                       aws.String(key + "-invalid-mode"),
			Body:                      strings.NewReader(content),
			ObjectLockMode:            "INVALID_MODE", // Invalid mode
			ObjectLockRetainUntilDate: aws.Time(time.Now().Add(24 * time.Hour)),
		})
		require.Error(t, err)
	})

	// Test 2: Retention date in the past should be rejected
	t.Run("Past retention date", func(t *testing.T) {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:                    aws.String(bucketName),
			Key:                       aws.String(key + "-past-date"),
			Body:                      strings.NewReader(content),
			ObjectLockMode:            "GOVERNANCE",
			ObjectLockRetainUntilDate: aws.Time(time.Now().Add(-24 * time.Hour)), // Past date
		})
		require.Error(t, err)
	})

	// Test 3: Mode without date should be rejected
	t.Run("Mode without retention date", func(t *testing.T) {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:         aws.String(bucketName),
			Key:            aws.String(key + "-no-date"),
			Body:           strings.NewReader(content),
			ObjectLockMode: "GOVERNANCE",
			// Missing ObjectLockRetainUntilDate
		})
		require.Error(t, err)
	})
}

// TestObjectLockHeadersNonVersionedBucket tests that object lock fails on non-versioned buckets
func TestObjectLockHeadersNonVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create regular bucket without object lock/versioning
	createBucketWithoutObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	key := "test-non-versioned"
	content := "test content"
	retainUntilDate := time.Now().Add(24 * time.Hour)

	// Attempting to PUT with object lock headers should fail
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		Body:                      strings.NewReader(content),
		ObjectLockMode:            "GOVERNANCE",
		ObjectLockRetainUntilDate: aws.Time(retainUntilDate),
	})
	require.Error(t, err)
}

// Helper Functions

// putObjectWithLockHeaders puts an object with object lock headers
func putObjectWithLockHeaders(t *testing.T, client *s3.Client, bucketName, key, content string,
	mode string, retainUntilDate time.Time, legalHold string) *s3.PutObjectOutput {

	input := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
	}

	// Add retention mode and date if specified
	if mode != "" {
		switch mode {
		case "COMPLIANCE":
			input.ObjectLockMode = types.ObjectLockModeCompliance
		case "GOVERNANCE":
			input.ObjectLockMode = types.ObjectLockModeGovernance
		}
		if !retainUntilDate.IsZero() {
			input.ObjectLockRetainUntilDate = aws.Time(retainUntilDate)
		}
	}

	// Add legal hold if specified
	if legalHold != "" {
		switch legalHold {
		case "ON":
			input.ObjectLockLegalHoldStatus = types.ObjectLockLegalHoldStatusOn
		case "OFF":
			input.ObjectLockLegalHoldStatus = types.ObjectLockLegalHoldStatusOff
		}
	}

	resp, err := client.PutObject(context.TODO(), input)
	require.NoError(t, err)
	return resp
}

// createBucketWithObjectLock creates a bucket with object lock enabled
func createBucketWithObjectLock(t *testing.T, client *s3.Client, bucketName string) {
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket:                     aws.String(bucketName),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	require.NoError(t, err)

	// Enable versioning (required for object lock)
	enableVersioning(t, client, bucketName)
}
