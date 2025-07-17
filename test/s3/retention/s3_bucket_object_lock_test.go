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

// TestBucketCreationWithObjectLockEnabled tests creating a bucket with the
// x-amz-bucket-object-lock-enabled header, which is required for S3 Object Lock compatibility
func TestBucketCreationWithObjectLockEnabled(t *testing.T) {
	// This test verifies that bucket creation with
	// x-amz-bucket-object-lock-enabled header should automatically enable Object Lock

	client := getS3Client(t)
	bucketName := getNewBucketName()
	defer func() {
		// Best effort cleanup
		deleteBucket(t, client, bucketName)
	}()

	// Test 1: Create bucket with Object Lock enabled header using custom HTTP client
	t.Run("CreateBucketWithObjectLockHeader", func(t *testing.T) {
		// Create bucket with x-amz-bucket-object-lock-enabled header
		// This simulates what S3 clients do when testing Object Lock support
		createResp, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
			Bucket:                     aws.String(bucketName),
			ObjectLockEnabledForBucket: aws.Bool(true), // This should set x-amz-bucket-object-lock-enabled header
		})
		require.NoError(t, err)
		require.NotNil(t, createResp)

		// Verify bucket was created
		_, err = client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err)
	})

	// Test 2: Verify that Object Lock is automatically enabled for the bucket
	t.Run("VerifyObjectLockAutoEnabled", func(t *testing.T) {
		// Try to get the Object Lock configuration
		// If the header was processed correctly, this should return an enabled configuration
		configResp, err := client.GetObjectLockConfiguration(context.TODO(), &s3.GetObjectLockConfigurationInput{
			Bucket: aws.String(bucketName),
		})

		require.NoError(t, err, "GetObjectLockConfiguration should not fail if Object Lock is enabled")
		require.NotNil(t, configResp.ObjectLockConfiguration, "ObjectLockConfiguration should not be nil")
		assert.Equal(t, types.ObjectLockEnabledEnabled, configResp.ObjectLockConfiguration.ObjectLockEnabled, "Object Lock should be enabled")
	})

	// Test 3: Verify versioning is automatically enabled (required for Object Lock)
	t.Run("VerifyVersioningAutoEnabled", func(t *testing.T) {
		// Object Lock requires versioning to be enabled
		// When Object Lock is enabled via header, versioning should also be enabled automatically
		versioningResp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err)

		// Versioning should be automatically enabled for Object Lock
		assert.Equal(t, types.BucketVersioningStatusEnabled, versioningResp.Status, "Versioning should be automatically enabled for Object Lock")
	})
}

// TestBucketCreationWithoutObjectLockHeader tests normal bucket creation
// to ensure we don't break existing functionality
func TestBucketCreationWithoutObjectLockHeader(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	defer deleteBucket(t, client, bucketName)

	// Create bucket without Object Lock header
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Verify bucket was created
	_, err = client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Object Lock should NOT be enabled
	_, err = client.GetObjectLockConfiguration(context.TODO(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(bucketName),
	})
	// This should fail since Object Lock is not enabled
	require.Error(t, err)
	t.Logf("GetObjectLockConfiguration correctly failed for bucket without Object Lock: %v", err)

	// Versioning should not be enabled by default
	versioningResp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Should be either empty/unset or Suspended, but not Enabled
	if versioningResp.Status != types.BucketVersioningStatusEnabled {
		t.Logf("Versioning correctly not enabled: %v", versioningResp.Status)
	} else {
		t.Errorf("Versioning should not be enabled for bucket without Object Lock header")
	}
}

// TestS3ObjectLockWorkflow tests the complete Object Lock workflow that S3 clients would use
func TestS3ObjectLockWorkflow(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()
	defer deleteBucket(t, client, bucketName)

	// Step 1: Client creates bucket with Object Lock enabled
	t.Run("ClientCreatesBucket", func(t *testing.T) {
		_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
			Bucket:                     aws.String(bucketName),
			ObjectLockEnabledForBucket: aws.Bool(true),
		})
		require.NoError(t, err)
	})

	// Step 2: Client checks if Object Lock is supported by getting the configuration
	t.Run("ClientChecksObjectLockSupport", func(t *testing.T) {
		configResp, err := client.GetObjectLockConfiguration(context.TODO(), &s3.GetObjectLockConfigurationInput{
			Bucket: aws.String(bucketName),
		})

		require.NoError(t, err, "Object Lock configuration check should succeed")

		// S3 clients should see Object Lock is enabled
		require.NotNil(t, configResp.ObjectLockConfiguration)
		assert.Equal(t, types.ObjectLockEnabledEnabled, configResp.ObjectLockConfiguration.ObjectLockEnabled)
		t.Log("Object Lock configuration retrieved successfully - S3 clients would see this as supported")
	})

	// Step 3: Client would then configure retention policies and use Object Lock
	t.Run("ClientConfiguresRetention", func(t *testing.T) {
		// Verify versioning is automatically enabled (required for Object Lock)
		versioningResp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err)
		require.Equal(t, types.BucketVersioningStatusEnabled, versioningResp.Status, "Versioning should be automatically enabled")

		// Create an object
		key := "protected-backup-object"
		content := "Backup data with Object Lock protection"
		putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err)
		require.NotNil(t, putResp.VersionId)

		// Set Object Lock retention (what backup clients do to protect data)
		retentionUntil := time.Now().Add(24 * time.Hour)
		_, err = client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Retention: &types.ObjectLockRetention{
				Mode:            types.ObjectLockRetentionModeCompliance,
				RetainUntilDate: aws.Time(retentionUntil),
			},
		})
		require.NoError(t, err)

		// Verify object is protected
		_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.Error(t, err, "Object should be protected by retention policy")

		t.Log("Object Lock retention successfully applied - data is immutable")
	})
}
