package s3api

import (
	"context"
	"fmt"
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
	Endpoint:      "http://localhost:8333", // Default SeaweedFS S3 port
	AccessKey:     "some_access_key1",
	SecretKey:     "some_secret_key1",
	Region:        "us-east-1",
	BucketPrefix:  "test-retention-",
	UseSSL:        false,
	SkipVerifySSL: true,
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

// getNewBucketName generates a unique bucket name
func getNewBucketName() string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s%d", defaultConfig.BucketPrefix, timestamp)
}

// createBucket creates a new bucket for testing
func createBucket(t *testing.T, client *s3.Client, bucketName string) {
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
}

// deleteBucket deletes a bucket and all its contents
func deleteBucket(t *testing.T, client *s3.Client, bucketName string) {
	// First, try to delete all objects and versions
	err := deleteAllObjectVersions(t, client, bucketName)
	if err != nil {
		t.Logf("Warning: failed to delete all object versions in first attempt: %v", err)
		// Try once more in case of transient errors
		time.Sleep(500 * time.Millisecond)
		err = deleteAllObjectVersions(t, client, bucketName)
		if err != nil {
			t.Logf("Warning: failed to delete all object versions in second attempt: %v", err)
		}
	}

	// Wait a bit for eventual consistency
	time.Sleep(100 * time.Millisecond)

	// Try to delete the bucket multiple times in case of eventual consistency issues
	for retries := 0; retries < 3; retries++ {
		_, err = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Logf("Successfully deleted bucket %s", bucketName)
			return
		}

		t.Logf("Warning: failed to delete bucket %s (attempt %d): %v", bucketName, retries+1, err)
		if retries < 2 {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// deleteAllObjectVersions deletes all object versions in a bucket
func deleteAllObjectVersions(t *testing.T, client *s3.Client, bucketName string) error {
	// List all object versions
	paginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return err
		}

		var objectsToDelete []types.ObjectIdentifier

		// Add versions - first try to remove retention/legal hold
		for _, version := range page.Versions {
			// Try to remove legal hold if present
			_, err := client.PutObjectLegalHold(context.TODO(), &s3.PutObjectLegalHoldInput{
				Bucket:    aws.String(bucketName),
				Key:       version.Key,
				VersionId: version.VersionId,
				LegalHold: &types.ObjectLockLegalHold{
					Status: types.ObjectLockLegalHoldStatusOff,
				},
			})
			if err != nil {
				// Legal hold might not be set, ignore error
				t.Logf("Note: could not remove legal hold for %s@%s: %v", *version.Key, *version.VersionId, err)
			}

			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key:       version.Key,
				VersionId: version.VersionId,
			})
		}

		// Add delete markers
		for _, deleteMarker := range page.DeleteMarkers {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key:       deleteMarker.Key,
				VersionId: deleteMarker.VersionId,
			})
		}

		// Delete objects in batches with bypass governance retention
		if len(objectsToDelete) > 0 {
			_, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
				Bucket:                    aws.String(bucketName),
				BypassGovernanceRetention: true,
				Delete: &types.Delete{
					Objects: objectsToDelete,
					Quiet:   true,
				},
			})
			if err != nil {
				t.Logf("Warning: batch delete failed, trying individual deletion: %v", err)
				// Try individual deletion for each object
				for _, obj := range objectsToDelete {
					_, delErr := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
						Bucket:                    aws.String(bucketName),
						Key:                       obj.Key,
						VersionId:                 obj.VersionId,
						BypassGovernanceRetention: true,
					})
					if delErr != nil {
						t.Logf("Warning: failed to delete object %s@%s: %v", *obj.Key, *obj.VersionId, delErr)
					}
				}
			}
		}
	}

	return nil
}

// enableVersioning enables versioning on a bucket
func enableVersioning(t *testing.T, client *s3.Client, bucketName string) {
	_, err := client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)
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

// cleanupAllTestBuckets cleans up any leftover test buckets
func cleanupAllTestBuckets(t *testing.T, client *s3.Client) {
	// List all buckets
	listResp, err := client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		t.Logf("Warning: failed to list buckets for cleanup: %v", err)
		return
	}

	// Delete buckets that match our test prefix
	for _, bucket := range listResp.Buckets {
		if bucket.Name != nil && strings.HasPrefix(*bucket.Name, defaultConfig.BucketPrefix) {
			t.Logf("Cleaning up leftover test bucket: %s", *bucket.Name)
			deleteBucket(t, client, *bucket.Name)
		}
	}
}

// TestBasicRetentionWorkflow tests the basic retention functionality
func TestBasicRetentionWorkflow(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Enable versioning (required for retention)
	enableVersioning(t, client, bucketName)

	// Create object
	key := "test-object"
	content := "test content for retention"
	putResp := putObject(t, client, bucketName, key, content)
	require.NotNil(t, putResp.VersionId)

	// Set retention with GOVERNANCE mode
	retentionUntil := time.Now().Add(24 * time.Hour)
	_, err := client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retentionUntil),
		},
	})
	require.NoError(t, err)

	// Get retention and verify it was set correctly
	retentionResp, err := client.GetObjectRetention(context.TODO(), &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockRetentionModeGovernance, retentionResp.Retention.Mode)
	assert.WithinDuration(t, retentionUntil, *retentionResp.Retention.RetainUntilDate, time.Second)

	// Try to delete object without bypass - should fail
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.Error(t, err)

	// Delete object with bypass governance - should succeed
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		BypassGovernanceRetention: true,
	})
	require.NoError(t, err)
}

// TestRetentionModeCompliance tests COMPLIANCE mode retention
func TestRetentionModeCompliance(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create object
	key := "compliance-test-object"
	content := "compliance test content"
	putResp := putObject(t, client, bucketName, key, content)
	require.NotNil(t, putResp.VersionId)

	// Set retention with COMPLIANCE mode
	retentionUntil := time.Now().Add(1 * time.Hour)
	_, err := client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeCompliance,
			RetainUntilDate: aws.Time(retentionUntil),
		},
	})
	require.NoError(t, err)

	// Get retention and verify
	retentionResp, err := client.GetObjectRetention(context.TODO(), &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockRetentionModeCompliance, retentionResp.Retention.Mode)

	// Try to delete object with bypass - should still fail (compliance mode)
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		BypassGovernanceRetention: true,
	})
	require.Error(t, err)

	// Try to delete object without bypass - should also fail
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.Error(t, err)
}

// TestLegalHoldWorkflow tests legal hold functionality
func TestLegalHoldWorkflow(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create object
	key := "legal-hold-test-object"
	content := "legal hold test content"
	putResp := putObject(t, client, bucketName, key, content)
	require.NotNil(t, putResp.VersionId)

	// Set legal hold ON
	_, err := client.PutObjectLegalHold(context.TODO(), &s3.PutObjectLegalHoldInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		LegalHold: &types.ObjectLockLegalHold{
			Status: types.ObjectLockLegalHoldStatusOn,
		},
	})
	require.NoError(t, err)

	// Get legal hold and verify
	legalHoldResp, err := client.GetObjectLegalHold(context.TODO(), &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockLegalHoldStatusOn, legalHoldResp.LegalHold.Status)

	// Try to delete object - should fail due to legal hold
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.Error(t, err)

	// Remove legal hold
	_, err = client.PutObjectLegalHold(context.TODO(), &s3.PutObjectLegalHoldInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		LegalHold: &types.ObjectLockLegalHold{
			Status: types.ObjectLockLegalHoldStatusOff,
		},
	})
	require.NoError(t, err)

	// Verify legal hold is off
	legalHoldResp, err = client.GetObjectLegalHold(context.TODO(), &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockLegalHoldStatusOff, legalHoldResp.LegalHold.Status)

	// Now delete should succeed
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
}

// TestObjectLockConfiguration tests bucket object lock configuration
func TestObjectLockConfiguration(t *testing.T) {
	client := getS3Client(t)
	// Use a more unique bucket name to avoid conflicts
	bucketName := fmt.Sprintf("object-lock-config-%d-%d", time.Now().UnixNano(), time.Now().UnixMilli()%10000)

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Set object lock configuration
	_, err := client.PutObjectLockConfiguration(context.TODO(), &s3.PutObjectLockConfigurationInput{
		Bucket: aws.String(bucketName),
		ObjectLockConfiguration: &types.ObjectLockConfiguration{
			ObjectLockEnabled: types.ObjectLockEnabledEnabled,
			Rule: &types.ObjectLockRule{
				DefaultRetention: &types.DefaultRetention{
					Mode: types.ObjectLockRetentionModeGovernance,
					Days: 30,
				},
			},
		},
	})
	if err != nil {
		t.Logf("PutObjectLockConfiguration failed (may not be supported): %v", err)
		t.Skip("Object lock configuration not supported, skipping test")
		return
	}

	// Get object lock configuration and verify
	configResp, err := client.GetObjectLockConfiguration(context.TODO(), &s3.GetObjectLockConfigurationInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockEnabledEnabled, configResp.ObjectLockConfiguration.ObjectLockEnabled)
	assert.Equal(t, types.ObjectLockRetentionModeGovernance, configResp.ObjectLockConfiguration.Rule.DefaultRetention.Mode)
	assert.Equal(t, int32(30), configResp.ObjectLockConfiguration.Rule.DefaultRetention.Days)
}

// TestRetentionWithVersions tests retention with specific object versions
func TestRetentionWithVersions(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create multiple versions of the same object
	key := "versioned-retention-test"
	content1 := "version 1 content"
	content2 := "version 2 content"

	putResp1 := putObject(t, client, bucketName, key, content1)
	require.NotNil(t, putResp1.VersionId)

	putResp2 := putObject(t, client, bucketName, key, content2)
	require.NotNil(t, putResp2.VersionId)

	// Set retention on first version only
	retentionUntil := time.Now().Add(1 * time.Hour)
	_, err := client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: putResp1.VersionId,
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retentionUntil),
		},
	})
	require.NoError(t, err)

	// Get retention for first version
	retentionResp, err := client.GetObjectRetention(context.TODO(), &s3.GetObjectRetentionInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: putResp1.VersionId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockRetentionModeGovernance, retentionResp.Retention.Mode)

	// Try to get retention for second version - should fail (no retention set)
	_, err = client.GetObjectRetention(context.TODO(), &s3.GetObjectRetentionInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: putResp2.VersionId,
	})
	require.Error(t, err)

	// Delete second version should succeed (no retention)
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: putResp2.VersionId,
	})
	require.NoError(t, err)

	// Delete first version should fail (has retention)
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: putResp1.VersionId,
	})
	require.Error(t, err)

	// Delete first version with bypass should succeed
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		VersionId:                 putResp1.VersionId,
		BypassGovernanceRetention: true,
	})
	require.NoError(t, err)
}

// TestRetentionAndLegalHoldCombination tests retention and legal hold together
func TestRetentionAndLegalHoldCombination(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create object
	key := "combined-protection-test"
	content := "combined protection test content"
	putResp := putObject(t, client, bucketName, key, content)
	require.NotNil(t, putResp.VersionId)

	// Set both retention and legal hold
	retentionUntil := time.Now().Add(1 * time.Hour)

	// Set retention
	_, err := client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retentionUntil),
		},
	})
	require.NoError(t, err)

	// Set legal hold
	_, err = client.PutObjectLegalHold(context.TODO(), &s3.PutObjectLegalHoldInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		LegalHold: &types.ObjectLockLegalHold{
			Status: types.ObjectLockLegalHoldStatusOn,
		},
	})
	require.NoError(t, err)

	// Try to delete with bypass governance - should still fail due to legal hold
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		BypassGovernanceRetention: true,
	})
	require.Error(t, err)

	// Remove legal hold
	_, err = client.PutObjectLegalHold(context.TODO(), &s3.PutObjectLegalHoldInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		LegalHold: &types.ObjectLockLegalHold{
			Status: types.ObjectLockLegalHoldStatusOff,
		},
	})
	require.NoError(t, err)

	// Now delete with bypass governance should succeed
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(key),
		BypassGovernanceRetention: true,
	})
	require.NoError(t, err)
}

// TestExpiredRetention tests that objects can be deleted after retention expires
func TestExpiredRetention(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create object
	key := "expired-retention-test"
	content := "expired retention test content"
	putResp := putObject(t, client, bucketName, key, content)
	require.NotNil(t, putResp.VersionId)

	// Set retention for a very short time (2 seconds)
	retentionUntil := time.Now().Add(2 * time.Second)
	_, err := client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retentionUntil),
		},
	})
	require.NoError(t, err)

	// Try to delete immediately - should fail
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.Error(t, err)

	// Wait for retention to expire
	time.Sleep(3 * time.Second)

	// Now delete should succeed
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
}

// TestRetentionErrorCases tests various error conditions
func TestRetentionErrorCases(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Test setting retention on non-existent object
	_, err := client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("non-existent-key"),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(time.Now().Add(1 * time.Hour)),
		},
	})
	require.Error(t, err)

	// Test getting retention on non-existent object
	_, err = client.GetObjectRetention(context.TODO(), &s3.GetObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("non-existent-key"),
	})
	require.Error(t, err)

	// Test setting legal hold on non-existent object
	_, err = client.PutObjectLegalHold(context.TODO(), &s3.PutObjectLegalHoldInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("non-existent-key"),
		LegalHold: &types.ObjectLockLegalHold{
			Status: types.ObjectLockLegalHoldStatusOn,
		},
	})
	require.Error(t, err)

	// Test getting legal hold on non-existent object
	_, err = client.GetObjectLegalHold(context.TODO(), &s3.GetObjectLegalHoldInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("non-existent-key"),
	})
	require.Error(t, err)

	// Test setting retention with past date
	key := "retention-past-date-test"
	content := "test content"
	putObject(t, client, bucketName, key, content)

	pastDate := time.Now().Add(-1 * time.Hour)
	_, err = client.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(pastDate),
		},
	})
	require.Error(t, err)
}
