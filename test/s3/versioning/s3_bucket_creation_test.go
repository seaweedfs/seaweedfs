package s3api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBucketCreationBehavior tests the S3-compliant bucket creation behavior
func TestBucketCreationBehavior(t *testing.T) {
	client := getS3Client(t)
	ctx := context.Background()

	// Test cases for bucket creation behavior
	testCases := []struct {
		name               string
		setupFunc          func(t *testing.T, bucketName string) // Setup before test
		bucketName         string
		objectLockEnabled  *bool
		expectedStatusCode int
		expectedError      string
		cleanupFunc        func(t *testing.T, bucketName string) // Cleanup after test
	}{
		{
			name:               "Create new bucket - should succeed",
			bucketName:         "test-new-bucket-" + fmt.Sprintf("%d", time.Now().Unix()),
			objectLockEnabled:  nil,
			expectedStatusCode: 200,
			expectedError:      "",
		},
		{
			name: "Create existing bucket with same owner - should return BucketAlreadyExists",
			setupFunc: func(t *testing.T, bucketName string) {
				// Create bucket first
				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err, "Setup: failed to create initial bucket")
			},
			bucketName:         "test-same-owner-same-settings-" + fmt.Sprintf("%d", time.Now().Unix()),
			objectLockEnabled:  nil,
			expectedStatusCode: 409, // SeaweedFS now returns BucketAlreadyExists in all cases
			expectedError:      "BucketAlreadyExists",
		},
		{
			name: "Create bucket with same owner but different Object Lock settings - should fail",
			setupFunc: func(t *testing.T, bucketName string) {
				// Create bucket without Object Lock first
				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				})
				require.NoError(t, err, "Setup: failed to create initial bucket")
			},
			bucketName:         "test-same-owner-diff-settings-" + fmt.Sprintf("%d", time.Now().Unix()),
			objectLockEnabled:  aws.Bool(true), // Try to enable Object Lock on existing bucket
			expectedStatusCode: 409,
			expectedError:      "BucketAlreadyExists",
		},
		{
			name:               "Create bucket with Object Lock enabled - should succeed",
			bucketName:         "test-object-lock-new-" + fmt.Sprintf("%d", time.Now().Unix()),
			objectLockEnabled:  aws.Bool(true),
			expectedStatusCode: 200,
			expectedError:      "",
		},
		{
			name: "Create bucket with Object Lock enabled twice - should fail",
			setupFunc: func(t *testing.T, bucketName string) {
				// Create bucket with Object Lock first
				_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket:                     aws.String(bucketName),
					ObjectLockEnabledForBucket: aws.Bool(true),
				})
				require.NoError(t, err, "Setup: failed to create initial bucket with Object Lock")
			},
			bucketName:         "test-object-lock-duplicate-" + fmt.Sprintf("%d", time.Now().Unix()),
			objectLockEnabled:  aws.Bool(true),
			expectedStatusCode: 409,
			expectedError:      "BucketAlreadyExists",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			if tc.setupFunc != nil {
				tc.setupFunc(t, tc.bucketName)
			}

			// Cleanup function to ensure bucket is deleted after test
			defer func() {
				if tc.cleanupFunc != nil {
					tc.cleanupFunc(t, tc.bucketName)
				} else {
					// Default cleanup - delete bucket and all objects
					cleanupBucketForCreationTest(t, client, tc.bucketName)
				}
			}()

			// Execute the test - attempt to create bucket
			input := &s3.CreateBucketInput{
				Bucket: aws.String(tc.bucketName),
			}
			if tc.objectLockEnabled != nil {
				input.ObjectLockEnabledForBucket = tc.objectLockEnabled
			}

			_, err := client.CreateBucket(ctx, input)

			// Verify results
			if tc.expectedError == "" {
				// Should succeed
				assert.NoError(t, err, "Expected bucket creation to succeed")
			} else {
				// Should fail with specific error
				assert.Error(t, err, "Expected bucket creation to fail")
				if err != nil {
					assert.Contains(t, err.Error(), tc.expectedError,
						"Expected error to contain '%s', got: %v", tc.expectedError, err)
				}
			}
		})
	}
}

// TestBucketCreationWithDifferentUsers tests bucket creation with different identity contexts
func TestBucketCreationWithDifferentUsers(t *testing.T) {
	// This test would require setting up different S3 credentials/identities
	// For now, we'll skip this as it requires more complex setup
	t.Skip("Different user testing requires IAM setup - implement when IAM is configured")

	// TODO: Implement when we have proper IAM/user management in test setup
	// Should test:
	// 1. User A creates bucket
	// 2. User B tries to create same bucket -> should fail with BucketAlreadyExists
}

// TestBucketCreationVersioningInteraction tests interaction between bucket creation and versioning
func TestBucketCreationVersioningInteraction(t *testing.T) {
	client := getS3Client(t)
	ctx := context.Background()
	bucketName := "test-versioning-interaction-" + fmt.Sprintf("%d", time.Now().Unix())

	defer cleanupBucketForCreationTest(t, client, bucketName)

	// Create bucket with Object Lock (which enables versioning)
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(bucketName),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	require.NoError(t, err, "Failed to create bucket with Object Lock")

	// Verify versioning is enabled
	versioningOutput, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "Failed to get bucket versioning status")
	assert.Equal(t, types.BucketVersioningStatusEnabled, versioningOutput.Status,
		"Expected versioning to be enabled when Object Lock is enabled")

	// Try to create the same bucket again - should fail
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket:                     aws.String(bucketName),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	assert.Error(t, err, "Expected second bucket creation to fail")
	assert.Contains(t, err.Error(), "BucketAlreadyExists",
		"Expected BucketAlreadyExists error, got: %v", err)
}

// TestBucketCreationErrorMessages tests that proper error messages are returned
func TestBucketCreationErrorMessages(t *testing.T) {
	client := getS3Client(t)
	ctx := context.Background()
	bucketName := "test-error-messages-" + fmt.Sprintf("%d", time.Now().Unix())

	defer cleanupBucketForCreationTest(t, client, bucketName)

	// Create bucket first
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "Failed to create initial bucket")

	// Try to create again and check error details
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	require.Error(t, err, "Expected bucket creation to fail")

	// Check that it's the right type of error
	assert.Contains(t, err.Error(), "BucketAlreadyExists",
		"Expected BucketAlreadyExists error, got: %v", err)
}

// cleanupBucketForCreationTest removes a bucket and all its contents
func cleanupBucketForCreationTest(t *testing.T, client *s3.Client, bucketName string) {
	ctx := context.Background()

	// List and delete all objects (including versions)
	listInput := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	}

	for {
		listOutput, err := client.ListObjectVersions(ctx, listInput)
		if err != nil {
			// Bucket might not exist, which is fine
			break
		}

		if len(listOutput.Versions) == 0 && len(listOutput.DeleteMarkers) == 0 {
			break
		}

		// Delete all versions
		var objectsToDelete []types.ObjectIdentifier
		for _, version := range listOutput.Versions {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key:       version.Key,
				VersionId: version.VersionId,
			})
		}
		for _, marker := range listOutput.DeleteMarkers {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key:       marker.Key,
				VersionId: marker.VersionId,
			})
		}

		if len(objectsToDelete) > 0 {
			_, err = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucketName),
				Delete: &types.Delete{
					Objects: objectsToDelete,
				},
			})
			if err != nil {
				t.Logf("Warning: failed to delete objects from bucket %s: %v", bucketName, err)
			}
		}

		// Check if there are more objects
		if !aws.ToBool(listOutput.IsTruncated) {
			break
		}
		listInput.KeyMarker = listOutput.NextKeyMarker
		listInput.VersionIdMarker = listOutput.NextVersionIdMarker
	}

	// Delete the bucket
	_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("Warning: failed to delete bucket %s: %v", bucketName, err)
	}
}
