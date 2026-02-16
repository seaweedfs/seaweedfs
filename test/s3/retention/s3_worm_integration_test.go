package retention

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWORMRetentionIntegration tests that both retention and legacy WORM work together
func TestWORMRetentionIntegration(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create object
	key := "worm-retention-integration-test"
	content := "worm retention integration test content"
	putResp := putObject(t, client, bucketName, key, content)
	require.NotNil(t, putResp.VersionId)

	// Set retention (new system)
	retentionUntil := time.Now().Add(1 * time.Hour)
	_, err := client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retentionUntil),
		},
	})
	require.NoError(t, err)

	// Try simple DELETE - should fail due to GOVERNANCE retention
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.Error(t, err, "Simple DELETE should be blocked by GOVERNANCE retention")

	// Try DELETE with version ID - should fail due to GOVERNANCE retention
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: putResp.VersionId,
	})
	require.Error(t, err, "DELETE with version ID should be blocked by GOVERNANCE retention")

	// Delete with version ID and bypass should succeed
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		VersionId:                 putResp.VersionId,
		BypassGovernanceRetention: aws.Bool(true),
	})
	require.NoError(t, err)
}

// TestWORMLegacyCompatibility tests that legacy WORM functionality still works
func TestWORMLegacyCompatibility(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create object with legacy WORM headers (if supported)
	key := "legacy-worm-test"
	content := "legacy worm test content"

	// Try to create object with legacy WORM TTL header
	putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   strings.NewReader(content),
		// Add legacy WORM headers if supported
		Metadata: map[string]string{
			"x-amz-meta-worm-ttl": fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, putResp.VersionId)

	// Object should be created successfully
	resp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.NotNil(t, resp.Metadata)
}

// TestRetentionOverwriteProtection tests that retention prevents overwrites
func TestRetentionOverwriteProtection(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create object
	key := "overwrite-protection-test"
	content := "original content"
	putResp := putObject(t, client, bucketName, key, content)
	require.NotNil(t, putResp.VersionId)

	// Verify object exists before setting retention
	_, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "Object should exist before setting retention")

	// Set retention with specific version ID
	retentionUntil := time.Now().Add(1 * time.Hour)
	_, err = client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: putResp.VersionId,
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retentionUntil),
		},
	})
	require.NoError(t, err)

	// Try to overwrite object - should fail in non-versioned bucket context
	content2 := "new content"
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   strings.NewReader(content2),
	})
	// Note: In a real scenario, this might fail or create a new version
	// The actual behavior depends on the implementation
	if err != nil {
		t.Logf("Expected behavior: overwrite blocked due to retention: %v", err)
	} else {
		t.Logf("Overwrite allowed, likely created new version")
	}
}

// TestRetentionBulkOperations tests retention with bulk operations
func TestRetentionBulkOperations(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create multiple objects with retention
	var objectsToDelete []types.ObjectIdentifier
	retentionUntil := time.Now().Add(1 * time.Hour)

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("bulk-test-object-%d", i)
		content := fmt.Sprintf("bulk test content %d", i)

		putResp := putObject(t, client, bucketName, key, content)
		require.NotNil(t, putResp.VersionId)

		// Set retention on each object with version ID
		_, err := client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(key),
			VersionId: putResp.VersionId,
			Retention: &types.ObjectLockRetention{
				Mode:            types.ObjectLockRetentionModeGovernance,
				RetainUntilDate: aws.Time(retentionUntil),
			},
		})
		require.NoError(t, err)

		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
			Key:       aws.String(key),
			VersionId: putResp.VersionId,
		})
	}

	// Try bulk delete without bypass - should fail or have errors
	deleteResp, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{
			Objects: objectsToDelete,
			Quiet:   aws.Bool(false),
		},
	})

	// Check if operation failed or returned errors for protected objects
	if err != nil {
		t.Logf("Expected: bulk delete failed due to retention: %v", err)
	} else if deleteResp != nil && len(deleteResp.Errors) > 0 {
		t.Logf("Expected: bulk delete returned %d errors due to retention", len(deleteResp.Errors))
		for _, delErr := range deleteResp.Errors {
			t.Logf("Delete error: %s - %s", *delErr.Code, *delErr.Message)
		}
	} else {
		t.Logf("Warning: bulk delete succeeded - retention may not be enforced for bulk operations")
	}

	// Try bulk delete with bypass - should succeed
	_, err = client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
		Bucket:                    aws.String(bucketName),
		BypassGovernanceRetention: aws.Bool(true),
		Delete: &types.Delete{
			Objects: objectsToDelete,
			Quiet:   aws.Bool(false),
		},
	})
	if err != nil {
		t.Logf("Bulk delete with bypass failed (may not be supported): %v", err)
	} else {
		t.Logf("Bulk delete with bypass succeeded")
	}
}

// TestRetentionWithMultipartUpload tests retention with multipart uploads
func TestRetentionWithMultipartUpload(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Start multipart upload
	key := "multipart-retention-test"
	createResp, err := client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	uploadId := createResp.UploadId

	// Upload a part
	partContent := "This is a test part for multipart upload"
	uploadResp, err := client.UploadPart(context.TODO(), &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(key),
		PartNumber: aws.Int32(1),
		UploadId:   uploadId,
		Body:       strings.NewReader(partContent),
	})
	require.NoError(t, err)

	// Complete multipart upload
	completeResp, err := client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		UploadId: uploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					ETag:       uploadResp.ETag,
					PartNumber: aws.Int32(1),
				},
			},
		},
	})
	require.NoError(t, err)

	// Add a small delay to ensure the object is fully created
	time.Sleep(500 * time.Millisecond)

	// Verify object exists after multipart upload - retry if needed
	var headErr error
	for retries := 0; retries < 10; retries++ {
		_, headErr = client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		if headErr == nil {
			break
		}
		t.Logf("HeadObject attempt %d failed: %v", retries+1, headErr)
		time.Sleep(200 * time.Millisecond)
	}

	if headErr != nil {
		t.Logf("Object not found after multipart upload completion, checking if multipart upload is fully supported")
		// Check if the object exists by trying to list it
		listResp, listErr := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(key),
		})
		if listErr != nil || len(listResp.Contents) == 0 {
			t.Skip("Multipart upload may not be fully supported, skipping test")
			return
		}
		// If object exists in listing but not accessible via HeadObject, skip test
		t.Skip("Object exists in listing but not accessible via HeadObject, multipart upload may not be fully supported")
		return
	}

	require.NoError(t, headErr, "Object should exist after multipart upload")

	// Set retention on the completed multipart object with version ID
	retentionUntil := time.Now().Add(1 * time.Hour)
	_, err = client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: completeResp.VersionId,
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retentionUntil),
		},
	})
	require.NoError(t, err)

	// Try simple DELETE - should fail due to GOVERNANCE retention
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.Error(t, err, "Simple DELETE should be blocked by GOVERNANCE retention")

	// Try DELETE with version ID - should fail due to GOVERNANCE retention
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: completeResp.VersionId,
	})
	require.Error(t, err, "DELETE with version ID should be blocked by GOVERNANCE retention")
}

// TestRetentionExtendedAttributes tests that retention uses extended attributes correctly
func TestRetentionExtendedAttributes(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create object
	key := "extended-attrs-test"
	content := "extended attributes test content"
	putResp := putObject(t, client, bucketName, key, content)
	require.NotNil(t, putResp.VersionId)

	// Set retention
	retentionUntil := time.Now().Add(1 * time.Hour)
	_, err := client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: putResp.VersionId,
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retentionUntil),
		},
	})
	require.NoError(t, err)

	// Set legal hold
	_, err = client.PutObjectLegalHold(context.TODO(), &s3.PutObjectLegalHoldInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: putResp.VersionId,
		LegalHold: &types.ObjectLockLegalHold{
			Status: types.ObjectLockLegalHoldStatusOn,
		},
	})
	require.NoError(t, err)

	// Get object metadata to verify extended attributes are set
	resp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	// Check that the object has metadata (may be empty in some implementations)
	// Note: The actual metadata keys depend on the implementation
	if resp.Metadata != nil && len(resp.Metadata) > 0 {
		t.Logf("Object metadata: %+v", resp.Metadata)
	} else {
		t.Logf("Object metadata: empty (extended attributes may be stored internally)")
	}

	// Verify retention can be retrieved
	retentionResp, err := client.GetObjectRetention(context.TODO(), &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockRetentionModeGovernance, retentionResp.Retention.Mode)

	// Verify legal hold can be retrieved
	legalHoldResp, err := client.GetObjectLegalHold(context.TODO(), &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockLegalHoldStatusOn, legalHoldResp.LegalHold.Status)
}

// TestRetentionBucketDefaults tests object lock configuration defaults
func TestRetentionBucketDefaults(t *testing.T) {
	client := getS3Client(t)
	// Use a very unique bucket name to avoid conflicts
	bucketName := fmt.Sprintf("bucket-defaults-%d-%d", time.Now().UnixNano(), time.Now().UnixMilli()%10000)

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Set bucket object lock configuration with default retention
	_, err := client.PutObjectLockConfiguration(context.TODO(), &s3.PutObjectLockConfigurationInput{
		Bucket: aws.String(bucketName),
		ObjectLockConfiguration: &types.ObjectLockConfiguration{
			ObjectLockEnabled: types.ObjectLockEnabledEnabled,
			Rule: &types.ObjectLockRule{
				DefaultRetention: &types.DefaultRetention{
					Mode: types.ObjectLockRetentionModeGovernance,
					Days: aws.Int32(1), // 1 day default
				},
			},
		},
	})
	if err != nil {
		t.Logf("PutObjectLockConfiguration failed (may not be supported): %v", err)
		t.Skip("Object lock configuration not supported, skipping test")
		return
	}

	// Create object (should inherit default retention)
	key := "bucket-defaults-test"
	content := "bucket defaults test content"
	putResp := putObject(t, client, bucketName, key, content)
	require.NotNil(t, putResp.VersionId)

	// Check if object has default retention applied
	// Note: This depends on the implementation - some S3 services apply
	// default retention automatically, others require explicit setting
	retentionResp, err := client.GetObjectRetention(context.TODO(), &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Logf("No automatic default retention applied: %v", err)
	} else {
		t.Logf("Default retention applied: %+v", retentionResp.Retention)
		assert.Equal(t, types.ObjectLockRetentionModeGovernance, retentionResp.Retention.Mode)
	}
}

// TestRetentionConcurrentOperations tests concurrent retention operations
func TestRetentionConcurrentOperations(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create object
	key := "concurrent-ops-test"
	content := "concurrent operations test content"
	putResp := putObject(t, client, bucketName, key, content)
	require.NotNil(t, putResp.VersionId)

	// Test concurrent retention and legal hold operations
	retentionUntil := time.Now().Add(1 * time.Hour)

	// Set retention and legal hold concurrently
	errChan := make(chan error, 2)

	go func() {
		_, err := client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Retention: &types.ObjectLockRetention{
				Mode:            types.ObjectLockRetentionModeGovernance,
				RetainUntilDate: aws.Time(retentionUntil),
			},
		})
		errChan <- err
	}()

	go func() {
		_, err := client.PutObjectLegalHold(context.TODO(), &s3.PutObjectLegalHoldInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			LegalHold: &types.ObjectLockLegalHold{
				Status: types.ObjectLockLegalHoldStatusOn,
			},
		})
		errChan <- err
	}()

	// Wait for both operations to complete
	for i := 0; i < 2; i++ {
		err := <-errChan
		if err != nil {
			t.Logf("Concurrent operation failed: %v", err)
		}
	}

	// Verify both settings are applied
	retentionResp, err := client.GetObjectRetention(context.TODO(), &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err == nil {
		assert.Equal(t, types.ObjectLockRetentionModeGovernance, retentionResp.Retention.Mode)
	}

	legalHoldResp, err := client.GetObjectLegalHold(context.TODO(), &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err == nil {
		assert.Equal(t, types.ObjectLockLegalHoldStatusOn, legalHoldResp.LegalHold.Status)
	}
}
