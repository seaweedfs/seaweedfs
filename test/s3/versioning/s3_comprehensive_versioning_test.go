package s3api

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVersioningCreateObjectsInOrder tests the exact pattern from Python s3tests
func TestVersioningCreateObjectsInOrder(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Step 1: Create bucket (equivalent to get_new_bucket())
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Step 2: Enable versioning (equivalent to check_configure_versioning_retry)
	enableVersioning(t, client, bucketName)
	checkVersioningStatus(t, client, bucketName, types.BucketVersioningStatusEnabled)

	// Step 3: Create objects (equivalent to _create_objects with specific keys)
	keyNames := []string{"bar", "baz", "foo"}

	// This mirrors the exact logic from _create_objects function
	for _, keyName := range keyNames {
		putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(keyName),
			Body:   strings.NewReader(keyName), // content = key name
		})
		require.NoError(t, err)
		require.NotNil(t, putResp.VersionId)
		require.NotEmpty(t, *putResp.VersionId)

		t.Logf("Created object %s with version %s", keyName, *putResp.VersionId)
	}

	// Step 4: Verify all objects exist and have correct versioning data
	objectMetadata := make(map[string]map[string]interface{})

	for _, keyName := range keyNames {
		// Get object metadata (equivalent to head_object)
		headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(keyName),
		})
		require.NoError(t, err)
		require.NotNil(t, headResp.VersionId)

		// Store metadata for later comparison
		objectMetadata[keyName] = map[string]interface{}{
			"ETag":          *headResp.ETag,
			"LastModified":  *headResp.LastModified,
			"ContentLength": headResp.ContentLength,
			"VersionId":     *headResp.VersionId,
		}
	}

	// Step 5: List object versions (equivalent to list_object_versions)
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Verify results match Python test expectations
	assert.Len(t, listResp.Versions, len(keyNames), "Should have one version per object")
	assert.Empty(t, listResp.DeleteMarkers, "Should have no delete markers")

	// Create map for easy lookup
	versionsByKey := make(map[string]types.ObjectVersion)
	for _, version := range listResp.Versions {
		versionsByKey[*version.Key] = version
	}

	// Step 6: Verify each object's version data matches head_object data
	for _, keyName := range keyNames {
		version, exists := versionsByKey[keyName]
		require.True(t, exists, "Version should exist for key %s", keyName)

		expectedData := objectMetadata[keyName]

		// These assertions mirror the Python test logic
		assert.Equal(t, expectedData["ETag"], *version.ETag, "ETag mismatch for %s", keyName)
		assert.Equal(t, expectedData["ContentLength"], version.Size, "Size mismatch for %s", keyName)
		assert.Equal(t, expectedData["VersionId"], *version.VersionId, "VersionId mismatch for %s", keyName)
		assert.True(t, *version.IsLatest, "Should be marked as latest version for %s", keyName)

		// Time comparison with tolerance (Python uses _compare_dates)
		expectedTime := expectedData["LastModified"].(time.Time)
		actualTime := *version.LastModified
		timeDiff := actualTime.Sub(expectedTime)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}
		assert.True(t, timeDiff < time.Minute, "LastModified times should be close for %s", keyName)
	}

	t.Logf("Successfully verified versioning data for %d objects matching Python s3tests expectations", len(keyNames))
}

// TestVersioningMultipleVersionsSameObject tests creating multiple versions of the same object
func TestVersioningMultipleVersionsSameObject(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	objectKey := "test-multi-version"
	numVersions := 5
	versionIds := make([]string, numVersions)

	// Create multiple versions of the same object
	for i := 0; i < numVersions; i++ {
		content := fmt.Sprintf("content-version-%d", i+1)
		putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err)
		require.NotNil(t, putResp.VersionId)
		versionIds[i] = *putResp.VersionId
	}

	// Verify all versions exist
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Len(t, listResp.Versions, numVersions)

	// Verify only the latest is marked as latest
	latestCount := 0
	for _, version := range listResp.Versions {
		if *version.IsLatest {
			latestCount++
			assert.Equal(t, versionIds[numVersions-1], *version.VersionId, "Latest version should be the last one created")
		}
	}
	assert.Equal(t, 1, latestCount, "Only one version should be marked as latest")

	// Verify all version IDs are unique
	versionIdSet := make(map[string]bool)
	for _, version := range listResp.Versions {
		versionId := *version.VersionId
		assert.False(t, versionIdSet[versionId], "Version ID should be unique: %s", versionId)
		versionIdSet[versionId] = true
	}
}

// TestVersioningDeleteAndRecreate tests deleting and recreating objects with versioning
func TestVersioningDeleteAndRecreate(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	objectKey := "test-delete-recreate"

	// Create initial object
	putResp1, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("initial-content"),
	})
	require.NoError(t, err)
	originalVersionId := *putResp1.VersionId

	// Delete the object (creates delete marker)
	deleteResp, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	deleteMarkerVersionId := *deleteResp.VersionId

	// Recreate the object
	putResp2, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("recreated-content"),
	})
	require.NoError(t, err)
	newVersionId := *putResp2.VersionId

	// List versions
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Should have 2 object versions and 1 delete marker
	assert.Len(t, listResp.Versions, 2)
	assert.Len(t, listResp.DeleteMarkers, 1)

	// Verify the new version is marked as latest
	latestVersionCount := 0
	for _, version := range listResp.Versions {
		if *version.IsLatest {
			latestVersionCount++
			assert.Equal(t, newVersionId, *version.VersionId)
		} else {
			assert.Equal(t, originalVersionId, *version.VersionId)
		}
	}
	assert.Equal(t, 1, latestVersionCount)

	// Verify delete marker is not marked as latest (since we recreated the object)
	deleteMarker := listResp.DeleteMarkers[0]
	assert.False(t, *deleteMarker.IsLatest)
	assert.Equal(t, deleteMarkerVersionId, *deleteMarker.VersionId)
}

// TestVersioningListWithPagination tests versioning with pagination parameters
func TestVersioningListWithPagination(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create multiple objects with multiple versions each
	numObjects := 3
	versionsPerObject := 3
	totalExpectedVersions := numObjects * versionsPerObject

	for i := 0; i < numObjects; i++ {
		objectKey := fmt.Sprintf("test-object-%d", i)
		for j := 0; j < versionsPerObject; j++ {
			content := fmt.Sprintf("content-obj%d-ver%d", i, j)
			_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
				Body:   strings.NewReader(content),
			})
			require.NoError(t, err)
		}
	}

	// Test listing with max-keys parameter
	maxKeys := 5
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket:  aws.String(bucketName),
		MaxKeys: aws.Int32(int32(maxKeys)),
	})
	require.NoError(t, err)

	if totalExpectedVersions > maxKeys {
		assert.True(t, *listResp.IsTruncated)
		assert.LessOrEqual(t, len(listResp.Versions), maxKeys)
	} else {
		assert.Len(t, listResp.Versions, totalExpectedVersions)
	}

	// Test listing all versions without pagination
	allListResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Len(t, allListResp.Versions, totalExpectedVersions)

	// Verify each object has exactly one latest version
	latestVersionsByKey := make(map[string]int)
	for _, version := range allListResp.Versions {
		if *version.IsLatest {
			latestVersionsByKey[*version.Key]++
		}
	}
	assert.Len(t, latestVersionsByKey, numObjects)
	for objectKey, count := range latestVersionsByKey {
		assert.Equal(t, 1, count, "Object %s should have exactly one latest version", objectKey)
	}
}

// TestVersioningSpecificVersionRetrieval tests retrieving specific versions of objects
func TestVersioningSpecificVersionRetrieval(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	objectKey := "test-version-retrieval"
	contents := []string{"version1", "version2", "version3"}
	versionIds := make([]string, len(contents))

	// Create multiple versions
	for i, content := range contents {
		putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err)
		versionIds[i] = *putResp.VersionId
	}

	// Test retrieving each specific version
	for i, expectedContent := range contents {
		getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(objectKey),
			VersionId: aws.String(versionIds[i]),
		})
		require.NoError(t, err)

		// Read and verify content - read all available data, not just expected length
		body, err := io.ReadAll(getResp.Body)
		if err != nil {
			t.Logf("Error reading response body for version %d: %v", i+1, err)
			if getResp.ContentLength != nil {
				t.Logf("Content length: %d", *getResp.ContentLength)
			}
			if getResp.VersionId != nil {
				t.Logf("Version ID: %s", *getResp.VersionId)
			}
			require.NoError(t, err)
		}
		getResp.Body.Close()

		actualContent := string(body)
		t.Logf("Expected: %s, Actual: %s", expectedContent, actualContent)
		assert.Equal(t, expectedContent, actualContent, "Content mismatch for version %d", i+1)
		assert.Equal(t, versionIds[i], *getResp.VersionId, "Version ID mismatch")
	}

	// Test retrieving without version ID (should get latest)
	getLatestResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)

	body, err := io.ReadAll(getLatestResp.Body)
	require.NoError(t, err)
	getLatestResp.Body.Close()

	latestContent := string(body)
	assert.Equal(t, contents[len(contents)-1], latestContent)
	assert.Equal(t, versionIds[len(versionIds)-1], *getLatestResp.VersionId)
}

// TestVersioningErrorCases tests error scenarios with versioning
func TestVersioningErrorCases(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	objectKey := "test-error-cases"

	// Create an object to work with
	putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("test content"),
	})
	require.NoError(t, err)
	validVersionId := *putResp.VersionId

	// Test getting a non-existent version
	_, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String("non-existent-version-id"),
	})
	assert.Error(t, err, "Should get error for non-existent version")

	// Test deleting a specific version (should succeed)
	_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(validVersionId),
	})
	assert.NoError(t, err, "Should be able to delete specific version")

	// Verify the object is gone (since we deleted the only version)
	_, err = client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	assert.Error(t, err, "Should get error after deleting the only version")
}
