package s3api

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

// TestVersioningWithObjectLockHeaders ensures that versioned objects properly
// handle object lock headers in PUT requests and return them in HEAD/GET responses.
// This test would have caught the bug where object lock metadata was not returned
// in HEAD/GET responses.
func TestVersioningWithObjectLockHeaders(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket with object lock and versioning enabled
	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	key := "versioned-object-with-lock"
	content1 := "version 1 content"
	content2 := "version 2 content"

	// PUT first version with object lock headers
	retainUntilDate1 := time.Now().Add(12 * time.Hour)
	putResp1, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		Body:                      strings.NewReader(content1),
		ObjectLockMode:            types.ObjectLockModeGovernance,
		ObjectLockRetainUntilDate: aws.Time(retainUntilDate1),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp1.VersionId)

	// PUT second version with different object lock settings
	retainUntilDate2 := time.Now().Add(24 * time.Hour)
	putResp2, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		Body:                      strings.NewReader(content2),
		ObjectLockMode:            types.ObjectLockModeCompliance,
		ObjectLockRetainUntilDate: aws.Time(retainUntilDate2),
		ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
	})
	require.NoError(t, err)
	require.NotNil(t, putResp2.VersionId)
	require.NotEqual(t, *putResp1.VersionId, *putResp2.VersionId)

	// Test HEAD latest version returns correct object lock metadata
	t.Run("HEAD latest version", func(t *testing.T) {
		headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		// Should return metadata for version 2 (latest)
		assert.Equal(t, types.ObjectLockModeCompliance, headResp.ObjectLockMode)
		assert.NotNil(t, headResp.ObjectLockRetainUntilDate)
		assert.WithinDuration(t, retainUntilDate2, *headResp.ObjectLockRetainUntilDate, 5*time.Second)
		assert.Equal(t, types.ObjectLockLegalHoldStatusOn, headResp.ObjectLockLegalHoldStatus)
	})

	// Test HEAD specific version returns correct object lock metadata
	t.Run("HEAD specific version", func(t *testing.T) {
		headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(key),
			VersionId: putResp1.VersionId,
		})
		require.NoError(t, err)

		// Should return metadata for version 1
		assert.Equal(t, types.ObjectLockModeGovernance, headResp.ObjectLockMode)
		assert.NotNil(t, headResp.ObjectLockRetainUntilDate)
		assert.WithinDuration(t, retainUntilDate1, *headResp.ObjectLockRetainUntilDate, 5*time.Second)
		// Version 1 was created without legal hold, so AWS S3 defaults it to "OFF"
		assert.Equal(t, types.ObjectLockLegalHoldStatusOff, headResp.ObjectLockLegalHoldStatus)
	})

	// Test GET latest version returns correct object lock metadata
	t.Run("GET latest version", func(t *testing.T) {
		getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		// Should return metadata for version 2 (latest)
		assert.Equal(t, types.ObjectLockModeCompliance, getResp.ObjectLockMode)
		assert.NotNil(t, getResp.ObjectLockRetainUntilDate)
		assert.WithinDuration(t, retainUntilDate2, *getResp.ObjectLockRetainUntilDate, 5*time.Second)
		assert.Equal(t, types.ObjectLockLegalHoldStatusOn, getResp.ObjectLockLegalHoldStatus)
	})

	// Test GET specific version returns correct object lock metadata
	t.Run("GET specific version", func(t *testing.T) {
		getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(key),
			VersionId: putResp1.VersionId,
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		// Should return metadata for version 1
		assert.Equal(t, types.ObjectLockModeGovernance, getResp.ObjectLockMode)
		assert.NotNil(t, getResp.ObjectLockRetainUntilDate)
		assert.WithinDuration(t, retainUntilDate1, *getResp.ObjectLockRetainUntilDate, 5*time.Second)
		// Version 1 was created without legal hold, so AWS S3 defaults it to "OFF"
		assert.Equal(t, types.ObjectLockLegalHoldStatusOff, getResp.ObjectLockLegalHoldStatus)
	})
}

// waitForVersioningToBeEnabled polls the bucket versioning status until it's enabled
// This helps avoid race conditions where object lock is configured but versioning
// isn't immediately available
func waitForVersioningToBeEnabled(t *testing.T, client *s3.Client, bucketName string) {
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		resp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil && resp.Status == types.BucketVersioningStatusEnabled {
			return // Versioning is enabled
		}

		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Timeout waiting for versioning to be enabled on bucket %s", bucketName)
}

// Helper function for creating buckets with object lock enabled
func createBucketWithObjectLock(t *testing.T, client *s3.Client, bucketName string) {
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket:                     aws.String(bucketName),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	require.NoError(t, err)

	// Wait for versioning to be automatically enabled by object lock
	waitForVersioningToBeEnabled(t, client, bucketName)

	// Verify that object lock was actually enabled
	t.Logf("Verifying object lock configuration for bucket %s", bucketName)
	_, err = client.GetObjectLockConfiguration(context.TODO(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "Object lock should be configured for bucket %s", bucketName)
}
