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

// TestBucketDeletionWithObjectLock tests that buckets with object lock enabled
// cannot be deleted if they contain objects with active retention or legal hold
func TestBucketDeletionWithObjectLock(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket with object lock enabled
	createBucketWithObjectLock(t, client, bucketName)

	// Table-driven test for retention modes
	retentionTestCases := []struct {
		name     string
		lockMode types.ObjectLockMode
		key      string
		content  string
	}{
		{
			name:     "ComplianceRetention",
			lockMode: types.ObjectLockModeCompliance,
			key:      "test-compliance-retention",
			content:  "test content for compliance retention",
		},
		{
			name:     "GovernanceRetention",
			lockMode: types.ObjectLockModeGovernance,
			key:      "test-governance-retention",
			content:  "test content for governance retention",
		},
	}

	for _, tc := range retentionTestCases {
		t.Run(fmt.Sprintf("CannotDeleteBucketWith%s", tc.name), func(t *testing.T) {
			retainUntilDate := time.Now().Add(10 * time.Second) // 10 seconds in future

			// Upload object with retention
			_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:                    aws.String(bucketName),
				Key:                       aws.String(tc.key),
				Body:                      strings.NewReader(tc.content),
				ObjectLockMode:            tc.lockMode,
				ObjectLockRetainUntilDate: aws.Time(retainUntilDate),
			})
			require.NoError(t, err, "PutObject with %s should succeed", tc.name)

			// Try to delete bucket - should fail because object has active retention
			_, err = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: aws.String(bucketName),
			})
			require.Error(t, err, "DeleteBucket should fail when objects have active retention")
			assert.Contains(t, err.Error(), "BucketNotEmpty", "Error should be BucketNotEmpty")
			t.Logf("Expected error: %v", err)

			// Wait for retention to expire with dynamic sleep based on actual retention time
			t.Logf("Waiting for %s to expire...", tc.name)
			time.Sleep(time.Until(retainUntilDate) + time.Second)

			// Delete the object
			_, err = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(tc.key),
			})
			require.NoError(t, err, "DeleteObject should succeed after retention expires")

			// Clean up versions
			deleteAllObjectVersions(t, client, bucketName)
		})
	}

	// Test 3: Bucket deletion with legal hold should fail
	t.Run("CannotDeleteBucketWithLegalHold", func(t *testing.T) {
		key := "test-legal-hold"
		content := "test content for legal hold"

		// Upload object first
		_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err, "PutObject should succeed")

		// Set legal hold on the object
		_, err = client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(key),
			LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn},
		})
		require.NoError(t, err, "PutObjectLegalHold should succeed")

		// Try to delete bucket - should fail because object has active legal hold
		_, err = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		require.Error(t, err, "DeleteBucket should fail when objects have active legal hold")
		assert.Contains(t, err.Error(), "BucketNotEmpty", "Error should be BucketNotEmpty")
		t.Logf("Expected error: %v", err)

		// Remove legal hold
		_, err = client.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(key),
			LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOff},
		})
		require.NoError(t, err, "Removing legal hold should succeed")

		// Delete the object
		_, err = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		require.NoError(t, err, "DeleteObject should succeed after legal hold is removed")

		// Clean up versions
		deleteAllObjectVersions(t, client, bucketName)
	})

	// Test 4: Bucket deletion should succeed when no objects have active locks
	t.Run("CanDeleteBucketWithoutActiveLocks", func(t *testing.T) {
		// Make sure all objects are deleted
		deleteAllObjectVersions(t, client, bucketName)

		// Use retry mechanism for eventual consistency instead of fixed sleep
		require.Eventually(t, func() bool {
			_, err := client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: aws.String(bucketName),
			})
			if err != nil {
				t.Logf("Retrying DeleteBucket due to: %v", err)
				return false
			}
			return true
		}, 5*time.Second, 500*time.Millisecond, "DeleteBucket should succeed when no objects have active locks")

		t.Logf("Successfully deleted bucket without active locks")
	})
}

// TestBucketDeletionWithVersionedLocks tests deletion with versioned objects under lock
func TestBucketDeletionWithVersionedLocks(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket with object lock enabled
	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName) // Best effort cleanup

	key := "test-versioned-locks"
	content1 := "version 1 content"
	content2 := "version 2 content"
	retainUntilDate := time.Now().Add(10 * time.Second)

	// Upload first version with retention
	putResp1, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		Body:                      strings.NewReader(content1),
		ObjectLockMode:            types.ObjectLockModeGovernance,
		ObjectLockRetainUntilDate: aws.Time(retainUntilDate),
	})
	require.NoError(t, err)
	version1 := *putResp1.VersionId

	// Upload second version with retention
	putResp2, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		Body:                      strings.NewReader(content2),
		ObjectLockMode:            types.ObjectLockModeGovernance,
		ObjectLockRetainUntilDate: aws.Time(retainUntilDate),
	})
	require.NoError(t, err)
	version2 := *putResp2.VersionId

	t.Logf("Created two versions: %s, %s", version1, version2)

	// Try to delete bucket - should fail because versions have active retention
	_, err = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.Error(t, err, "DeleteBucket should fail when object versions have active retention")
	assert.Contains(t, err.Error(), "BucketNotEmpty", "Error should be BucketNotEmpty")
	t.Logf("Expected error: %v", err)

	// Wait for retention to expire with dynamic sleep based on actual retention time
	t.Logf("Waiting for retention to expire on all versions...")
	time.Sleep(time.Until(retainUntilDate) + time.Second)

	// Clean up all versions
	deleteAllObjectVersions(t, client, bucketName)

	// Wait for eventual consistency and attempt to delete the bucket with retry
	require.Eventually(t, func() bool {
		_, err := client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Logf("Retrying DeleteBucket due to: %v", err)
			return false
		}
		return true
	}, 5*time.Second, 500*time.Millisecond, "DeleteBucket should succeed after all locks expire")

	t.Logf("Successfully deleted bucket after locks expired")
}

// TestBucketDeletionWithoutObjectLock tests that buckets without object lock can be deleted normally
func TestBucketDeletionWithoutObjectLock(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create regular bucket without object lock
	createBucket(t, client, bucketName)

	// Upload some objects
	for i := 0; i < 3; i++ {
		_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fmt.Sprintf("test-object-%d", i)),
			Body:   strings.NewReader("test content"),
		})
		require.NoError(t, err)
	}

	// Delete all objects
	deleteAllObjectVersions(t, client, bucketName)

	// Delete bucket should succeed
	_, err := client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "DeleteBucket should succeed for regular bucket")
	t.Logf("Successfully deleted regular bucket without object lock")
}
