package s3api

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
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

// TestListObjectVersionsIncludesDirectories tests that directories are included in list-object-versions response
// This ensures compatibility with Minio and AWS S3 behavior
func TestListObjectVersionsIncludesDirectories(t *testing.T) {
	bucketName := "test-versioning-directories"

	client := setupS3Client(t)

	// Create bucket
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Clean up
	defer func() {
		cleanupBucket(t, client, bucketName)
	}()

	// Enable versioning
	_, err = client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// First create explicit directory objects (keys ending with "/")
	// These are the directories that should appear in list-object-versions
	explicitDirectories := []string{
		"Veeam/",
		"Veeam/Archive/",
		"Veeam/Archive/vbr/",
		"Veeam/Backup/",
		"Veeam/Backup/vbr/",
		"Veeam/Backup/vbr/Clients/",
	}

	// Create explicit directory objects
	for _, dirKey := range explicitDirectories {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(dirKey),
			Body:   strings.NewReader(""), // Empty content for directories
		})
		require.NoError(t, err, "Failed to create directory object %s", dirKey)
	}

	// Now create some test files
	testFiles := []string{
		"Veeam/test-file.txt",
		"Veeam/Archive/test-file2.txt",
		"Veeam/Archive/vbr/test-file3.txt",
		"Veeam/Backup/test-file4.txt",
		"Veeam/Backup/vbr/test-file5.txt",
		"Veeam/Backup/vbr/Clients/test-file6.txt",
	}

	// Upload test files
	for _, objectKey := range testFiles {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader("test content"),
		})
		require.NoError(t, err, "Failed to create file %s", objectKey)
	}

	// List object versions
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Extract all keys from versions
	var allKeys []string
	for _, version := range listResp.Versions {
		allKeys = append(allKeys, *version.Key)
	}

	// Expected directories that should be included (with trailing slash)
	expectedDirectories := []string{
		"Veeam/",
		"Veeam/Archive/",
		"Veeam/Archive/vbr/",
		"Veeam/Backup/",
		"Veeam/Backup/vbr/",
		"Veeam/Backup/vbr/Clients/",
	}

	// Verify that directories are included in the response
	t.Logf("Found %d total versions", len(listResp.Versions))
	t.Logf("All keys: %v", allKeys)

	for _, expectedDir := range expectedDirectories {
		found := false
		for _, version := range listResp.Versions {
			if *version.Key == expectedDir {
				found = true
				// Verify directory properties
				assert.Equal(t, "null", *version.VersionId, "Directory %s should have VersionId 'null'", expectedDir)
				assert.Equal(t, int64(0), *version.Size, "Directory %s should have size 0", expectedDir)
				assert.True(t, *version.IsLatest, "Directory %s should be marked as latest", expectedDir)
				assert.Equal(t, "\"d41d8cd98f00b204e9800998ecf8427e\"", *version.ETag, "Directory %s should have MD5 of empty string as ETag", expectedDir)
				assert.Equal(t, types.ObjectStorageClassStandard, version.StorageClass, "Directory %s should have STANDARD storage class", expectedDir)
				break
			}
		}
		assert.True(t, found, "Directory %s should be included in list-object-versions response", expectedDir)
	}

	// Also verify that actual files are included
	for _, objectKey := range testFiles {
		found := false
		for _, version := range listResp.Versions {
			if *version.Key == objectKey {
				found = true
				assert.NotEqual(t, "null", *version.VersionId, "File %s should have a real version ID", objectKey)
				assert.Greater(t, *version.Size, int64(0), "File %s should have size > 0", objectKey)
				break
			}
		}
		assert.True(t, found, "File %s should be included in list-object-versions response", objectKey)
	}

	// Count directories vs files
	directoryCount := 0
	fileCount := 0
	for _, version := range listResp.Versions {
		if strings.HasSuffix(*version.Key, "/") && *version.Size == 0 && *version.VersionId == "null" {
			directoryCount++
		} else {
			fileCount++
		}
	}

	t.Logf("Found %d directories and %d files", directoryCount, fileCount)
	assert.Equal(t, len(expectedDirectories), directoryCount, "Should find exactly %d directories", len(expectedDirectories))
	assert.Equal(t, len(testFiles), fileCount, "Should find exactly %d files", len(testFiles))
}

// TestListObjectVersionsDeleteMarkers tests that delete markers are properly separated from versions
// This test verifies the fix for the issue where delete markers were incorrectly categorized as versions
func TestListObjectVersionsDeleteMarkers(t *testing.T) {
	bucketName := "test-delete-markers"

	client := setupS3Client(t)

	// Create bucket
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Clean up
	defer func() {
		cleanupBucket(t, client, bucketName)
	}()

	// Enable versioning
	_, err = client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	objectKey := "test1/a"

	// 1. Create one version of the file
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("test content"),
	})
	require.NoError(t, err)

	// 2. Delete the object 3 times to create 3 delete markers
	for i := 0; i < 3; i++ {
		_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err)
	}

	// 3. List object versions and verify the response structure
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// 4. Verify that we have exactly 1 version and 3 delete markers
	assert.Len(t, listResp.Versions, 1, "Should have exactly 1 file version")
	assert.Len(t, listResp.DeleteMarkers, 3, "Should have exactly 3 delete markers")

	// 5. Verify the version is for our test file
	version := listResp.Versions[0]
	assert.Equal(t, objectKey, *version.Key, "Version should be for our test file")
	assert.NotEqual(t, "null", *version.VersionId, "File version should have a real version ID")
	assert.Greater(t, *version.Size, int64(0), "File version should have size > 0")

	// 6. Verify all delete markers are for our test file
	for i, deleteMarker := range listResp.DeleteMarkers {
		assert.Equal(t, objectKey, *deleteMarker.Key, "Delete marker %d should be for our test file", i)
		assert.NotEqual(t, "null", *deleteMarker.VersionId, "Delete marker %d should have a real version ID", i)
	}

	t.Logf("Successfully verified: 1 version + 3 delete markers for object %s", objectKey)
}

// TestVersionedObjectAcl tests that ACL operations work correctly on objects in versioned buckets
// This test verifies the fix for the NoSuchKey error when getting ACLs for objects in versioned buckets
func TestVersionedObjectAcl(t *testing.T) {
	bucketName := "test-versioned-acl"

	client := setupS3Client(t)

	// Create bucket
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Clean up
	defer func() {
		cleanupBucket(t, client, bucketName)
	}()

	// Enable versioning
	_, err = client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	objectKey := "test-acl-object"

	// Create an object in the versioned bucket
	putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("test content for ACL"),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp.VersionId, "Object should have a version ID")

	// Test 1: Get ACL for the object (without specifying version ID - should get latest version)
	getAclResp, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get ACL for object in versioned bucket")
	require.NotNil(t, getAclResp.Owner, "ACL response should have owner information")

	// Test 2: Get ACL for specific version ID
	getAclVersionResp, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: putResp.VersionId,
	})
	require.NoError(t, err, "Should be able to get ACL for specific version")
	require.NotNil(t, getAclVersionResp.Owner, "Versioned ACL response should have owner information")

	// Test 3: Verify both ACL responses are the same (same object, same version)
	assert.Equal(t, getAclResp.Owner.ID, getAclVersionResp.Owner.ID, "Owner ID should match for latest and specific version")

	// Test 4: Create another version of the same object
	putResp2, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("updated content for ACL"),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp2.VersionId, "Second object version should have a version ID")
	require.NotEqual(t, putResp.VersionId, putResp2.VersionId, "Version IDs should be different")

	// Test 5: Get ACL for latest version (should be the second version)
	getAclLatestResp, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get ACL for latest version after update")
	require.NotNil(t, getAclLatestResp.Owner, "Latest ACL response should have owner information")

	// Test 6: Get ACL for the first version specifically
	getAclFirstResp, err := client.GetObjectAcl(context.TODO(), &s3.GetObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: putResp.VersionId,
	})
	require.NoError(t, err, "Should be able to get ACL for first version specifically")
	require.NotNil(t, getAclFirstResp.Owner, "First version ACL response should have owner information")

	// Test 7: Verify we can put ACL on versioned objects
	_, err = client.PutObjectAcl(context.TODO(), &s3.PutObjectAclInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		ACL:    types.ObjectCannedACLPrivate,
	})
	require.NoError(t, err, "Should be able to put ACL on versioned object")

	t.Logf("Successfully verified ACL operations on versioned object %s with versions %s and %s",
		objectKey, *putResp.VersionId, *putResp2.VersionId)
}

// TestConcurrentMultiObjectDelete tests that concurrent delete operations work correctly without race conditions
// This test verifies the fix for the race condition in deleteSpecificObjectVersion
func TestConcurrentMultiObjectDelete(t *testing.T) {
	bucketName := "test-concurrent-delete"
	numObjects := 5
	numThreads := 5

	client := setupS3Client(t)

	// Create bucket
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Clean up
	defer func() {
		cleanupBucket(t, client, bucketName)
	}()

	// Enable versioning
	_, err = client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// Create objects
	var objectKeys []string
	var versionIds []string

	for i := 0; i < numObjects; i++ {
		objectKey := fmt.Sprintf("key_%d", i)
		objectKeys = append(objectKeys, objectKey)

		putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(fmt.Sprintf("content for key_%d", i)),
		})
		require.NoError(t, err)
		require.NotNil(t, putResp.VersionId)
		versionIds = append(versionIds, *putResp.VersionId)
	}

	// Verify objects were created
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Len(t, listResp.Versions, numObjects, "Should have created %d objects", numObjects)

	// Create delete objects request
	var objectsToDelete []types.ObjectIdentifier
	for i, objectKey := range objectKeys {
		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
			Key:       aws.String(objectKey),
			VersionId: aws.String(versionIds[i]),
		})
	}

	// Run concurrent delete operations
	results := make([]*s3.DeleteObjectsOutput, numThreads)
	var wg sync.WaitGroup

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadIdx int) {
			defer wg.Done()
			deleteResp, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
				Bucket: aws.String(bucketName),
				Delete: &types.Delete{
					Objects: objectsToDelete,
					Quiet:   aws.Bool(false),
				},
			})
			if err != nil {
				t.Errorf("Thread %d: delete objects failed: %v", threadIdx, err)
				return
			}
			results[threadIdx] = deleteResp
		}(i)
	}

	wg.Wait()

	// Verify results
	for i, result := range results {
		require.NotNil(t, result, "Thread %d should have a result", i)
		assert.Len(t, result.Deleted, numObjects, "Thread %d should have deleted all %d objects", i, numObjects)

		if len(result.Errors) > 0 {
			for _, deleteError := range result.Errors {
				t.Errorf("Thread %d delete error: %s - %s (Key: %s, VersionId: %s)",
					i, *deleteError.Code, *deleteError.Message, *deleteError.Key,
					func() string {
						if deleteError.VersionId != nil {
							return *deleteError.VersionId
						} else {
							return "nil"
						}
					}())
			}
		}
		assert.Empty(t, result.Errors, "Thread %d should have no delete errors", i)
	}

	// Verify objects are deleted (bucket should be empty)
	finalListResp, err := client.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Nil(t, finalListResp.Contents, "Bucket should be empty after all deletions")

	t.Logf("Successfully verified concurrent deletion of %d objects from %d threads", numObjects, numThreads)
}

// TestSuspendedVersioningDeleteBehavior tests that delete operations during suspended versioning
// actually delete the "null" version object rather than creating delete markers
func TestSuspendedVersioningDeleteBehavior(t *testing.T) {
	bucketName := "test-suspended-versioning-delete"
	objectKey := "testobj"

	client := setupS3Client(t)

	// Create bucket
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Clean up
	defer func() {
		cleanupBucket(t, client, bucketName)
	}()

	// Enable versioning and create some versions
	_, err = client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// Create 3 versions
	var versionIds []string
	for i := 0; i < 3; i++ {
		putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(fmt.Sprintf("content version %d", i+1)),
		})
		require.NoError(t, err)
		require.NotNil(t, putResp.VersionId)
		versionIds = append(versionIds, *putResp.VersionId)
	}

	// Verify 3 versions exist
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Len(t, listResp.Versions, 3, "Should have 3 versions initially")

	// Suspend versioning
	_, err = client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusSuspended,
		},
	})
	require.NoError(t, err)

	// Create a new object during suspended versioning (this should be a "null" version)
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("null version content"),
	})
	require.NoError(t, err)

	// Verify we still have 3 versions + 1 null version = 4 total
	listResp, err = client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Len(t, listResp.Versions, 4, "Should have 3 versions + 1 null version")

	// Find the null version
	var nullVersionFound bool
	for _, version := range listResp.Versions {
		if *version.VersionId == "null" {
			nullVersionFound = true
			assert.True(t, *version.IsLatest, "Null version should be marked as latest during suspended versioning")
			break
		}
	}
	assert.True(t, nullVersionFound, "Should have found a null version")

	// Delete the object during suspended versioning (should actually delete the null version)
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		// No VersionId specified - should delete the "null" version during suspended versioning
	})
	require.NoError(t, err)

	// Verify the null version was actually deleted (not a delete marker created)
	listResp, err = client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Len(t, listResp.Versions, 3, "Should be back to 3 versions after deleting null version")
	assert.Empty(t, listResp.DeleteMarkers, "Should have no delete markers during suspended versioning delete")

	// Verify null version is gone
	nullVersionFound = false
	for _, version := range listResp.Versions {
		if *version.VersionId == "null" {
			nullVersionFound = true
			break
		}
	}
	assert.False(t, nullVersionFound, "Null version should be deleted, not present")

	// Create another null version and delete it multiple times to test idempotency
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("another null version"),
	})
	require.NoError(t, err)

	// Delete it twice to test idempotency
	for i := 0; i < 2; i++ {
		_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		require.NoError(t, err, "Delete should be idempotent - iteration %d", i+1)
	}

	// Re-enable versioning
	_, err = client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// Create a new version with versioning enabled
	putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("new version after re-enabling"),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp.VersionId)

	// Now delete without version ID (should create delete marker)
	deleteResp, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.Equal(t, "true", deleteResp.DeleteMarker, "Should create delete marker when versioning is enabled")

	// Verify final state
	listResp, err = client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Len(t, listResp.Versions, 4, "Should have 3 original versions + 1 new version")
	assert.Len(t, listResp.DeleteMarkers, 1, "Should have 1 delete marker")

	t.Logf("Successfully verified suspended versioning delete behavior")
}

// TestVersionedObjectListBehavior tests that list operations show logical object names for versioned objects
// and that owner information is properly extracted from S3 metadata
func TestVersionedObjectListBehavior(t *testing.T) {
	bucketName := "test-versioned-list"
	objectKey := "testfile"

	client := setupS3Client(t)

	// Create bucket with object lock enabled (which enables versioning)
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket:                     aws.String(bucketName),
		ObjectLockEnabledForBucket: aws.Bool(true),
	})
	require.NoError(t, err)

	// Clean up
	defer func() {
		cleanupBucket(t, client, bucketName)
	}()

	// Verify versioning is enabled
	versioningResp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Equal(t, types.BucketVersioningStatusEnabled, versioningResp.Status, "Bucket versioning should be enabled")

	// Create a versioned object
	content := "test content for versioned object"
	putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(content),
	})
	require.NoError(t, err)
	require.NotNil(t, putResp.VersionId)

	versionId := *putResp.VersionId
	t.Logf("Created versioned object with version ID: %s", versionId)

	// Test list-objects operation - should show logical object name, not internal versioned path
	listResp, err := client.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	require.Len(t, listResp.Contents, 1, "Should list exactly one object")

	listedObject := listResp.Contents[0]

	// Verify the object key is the logical name, not the internal versioned path
	assert.Equal(t, objectKey, *listedObject.Key, "Should show logical object name, not internal versioned path")
	assert.NotContains(t, *listedObject.Key, ".versions", "Object key should not contain .versions")
	assert.NotContains(t, *listedObject.Key, versionId, "Object key should not contain version ID")

	// Verify object properties
	assert.Equal(t, int64(len(content)), listedObject.Size, "Object size should match")
	assert.NotNil(t, listedObject.ETag, "Object should have ETag")
	assert.NotNil(t, listedObject.LastModified, "Object should have LastModified")

	// Verify owner information is present (even if anonymous)
	require.NotNil(t, listedObject.Owner, "Object should have Owner information")
	assert.NotEmpty(t, listedObject.Owner.ID, "Owner ID should not be empty")
	assert.NotEmpty(t, listedObject.Owner.DisplayName, "Owner DisplayName should not be empty")

	t.Logf("Listed object: Key=%s, Size=%d, Owner.ID=%s, Owner.DisplayName=%s",
		*listedObject.Key, listedObject.Size, *listedObject.Owner.ID, *listedObject.Owner.DisplayName)

	// Test list-objects-v2 operation as well
	listV2Resp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:     aws.String(bucketName),
		FetchOwner: aws.Bool(true), // Explicitly request owner information
	})
	require.NoError(t, err)
	require.Len(t, listV2Resp.Contents, 1, "ListObjectsV2 should also list exactly one object")

	listedObjectV2 := listV2Resp.Contents[0]
	assert.Equal(t, objectKey, *listedObjectV2.Key, "ListObjectsV2 should also show logical object name")
	assert.NotNil(t, listedObjectV2.Owner, "ListObjectsV2 should include owner when FetchOwner=true")

	// Create another version to ensure multiple versions don't appear in regular list
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("updated content"),
	})
	require.NoError(t, err)

	// List again - should still show only one logical object (the latest version)
	listRespAfterUpdate, err := client.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Len(t, listRespAfterUpdate.Contents, 1, "Should still list exactly one object after creating second version")

	// Compare with list-object-versions which should show both versions
	versionsResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Len(t, versionsResp.Versions, 2, "list-object-versions should show both versions")

	t.Logf("Successfully verified versioned object list behavior")
}

// TestPrefixFilteringLogic tests the prefix filtering logic fix for list object versions
// This addresses the issue raised by gemini-code-assist bot where files could be incorrectly included
func TestPrefixFilteringLogic(t *testing.T) {
	s3Client := setupS3Client(t)
	bucketName := "test-bucket-" + fmt.Sprintf("%d", time.Now().UnixNano())

	// Create bucket
	_, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	defer cleanupBucket(t, s3Client, bucketName)

	// Enable versioning
	_, err = s3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// Create test files that could trigger the edge case:
	// - File "a" (which should NOT be included when searching for prefix "a/b")
	// - File "a/b" (which SHOULD be included when searching for prefix "a/b")
	_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("a"),
		Body:   strings.NewReader("content of file a"),
	})
	require.NoError(t, err)

	_, err = s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("a/b"),
		Body:   strings.NewReader("content of file a/b"),
	})
	require.NoError(t, err)

	// Test list-object-versions with prefix "a/b" - should NOT include file "a"
	versionsResponse, err := s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("a/b"),
	})
	require.NoError(t, err)

	// Verify that only "a/b" is returned, not "a"
	require.Len(t, versionsResponse.Versions, 1, "Should only find one version matching prefix 'a/b'")
	assert.Equal(t, "a/b", aws.ToString(versionsResponse.Versions[0].Key), "Should only return 'a/b', not 'a'")

	// Test list-object-versions with prefix "a/" - should include "a/b" but not "a"
	versionsResponse, err = s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("a/"),
	})
	require.NoError(t, err)

	// Verify that only "a/b" is returned, not "a"
	require.Len(t, versionsResponse.Versions, 1, "Should only find one version matching prefix 'a/'")
	assert.Equal(t, "a/b", aws.ToString(versionsResponse.Versions[0].Key), "Should only return 'a/b', not 'a'")

	// Test list-object-versions with prefix "a" - should include both "a" and "a/b"
	versionsResponse, err = s3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("a"),
	})
	require.NoError(t, err)

	// Should find both files
	require.Len(t, versionsResponse.Versions, 2, "Should find both versions matching prefix 'a'")

	// Extract keys and sort them for predictable comparison
	var keys []string
	for _, version := range versionsResponse.Versions {
		keys = append(keys, aws.ToString(version.Key))
	}
	sort.Strings(keys)

	assert.Equal(t, []string{"a", "a/b"}, keys, "Should return both 'a' and 'a/b'")

	t.Logf("Prefix filtering logic correctly handles edge cases")
}

// Helper function to setup S3 client
func setupS3Client(t *testing.T) *s3.Client {
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
	defaultConfig := &S3TestConfig{
		Endpoint:      "http://localhost:8333", // Default SeaweedFS S3 port
		AccessKey:     "some_access_key1",
		SecretKey:     "some_secret_key1",
		Region:        "us-east-1",
		BucketPrefix:  "test-versioning-",
		UseSSL:        false,
		SkipVerifySSL: true,
	}

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

// Helper function to clean up bucket
func cleanupBucket(t *testing.T, client *s3.Client, bucketName string) {
	// First, delete all objects and versions
	err := deleteAllObjectVersions(t, client, bucketName)
	if err != nil {
		t.Logf("Warning: failed to delete all object versions: %v", err)
	}

	// Then delete the bucket
	_, err = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("Warning: failed to delete bucket %s: %v", bucketName, err)
	}
}
