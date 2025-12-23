package s3api

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVersioningPaginationOver1000Versions tests that ListObjectVersions correctly
// handles objects with more than 1000 versions using pagination.
// This is a stress test that creates 1500 versions and verifies all are listed.
//
// Run with: ENABLE_STRESS_TESTS=true go test -v -run TestVersioningPaginationOver1000Versions -timeout 30m
func TestVersioningPaginationOver1000Versions(t *testing.T) {
	if os.Getenv("ENABLE_STRESS_TESTS") != "true" {
		t.Skip("Skipping stress test. Set ENABLE_STRESS_TESTS=true to run.")
	}

	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Enable versioning
	enableVersioning(t, client, bucketName)
	checkVersioningStatus(t, client, bucketName, types.BucketVersioningStatusEnabled)

	objectKey := "test-many-versions"
	// Use 1100 for CI (tests pagination boundary), can override with env var for quick local testing
	numVersions := 1100
	if quickTest := os.Getenv("QUICK_TEST"); quickTest == "true" {
		numVersions = 50
	}
	versionIds := make([]string, 0, numVersions)

	t.Logf("Creating %d versions of object %s...", numVersions, objectKey)
	startTime := time.Now()

	// Create many versions of the same object
	for i := 0; i < numVersions; i++ {
		content := fmt.Sprintf("content-version-%d", i+1)
		putResp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err, "Failed to create version %d", i+1)
		require.NotNil(t, putResp.VersionId)
		versionIds = append(versionIds, *putResp.VersionId)

		if (i+1)%100 == 0 {
			t.Logf("Created %d/%d versions...", i+1, numVersions)
		}
	}

	createDuration := time.Since(startTime)
	t.Logf("Created %d versions in %v", numVersions, createDuration)
	t.Logf("Version IDs collected: %d", len(versionIds))

	// Quick sanity check - list versions to see what we have
	quickResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(objectKey),
	})
	require.NoError(t, err)
	t.Logf("Quick check: got %d versions, %d delete markers, truncated=%v",
		len(quickResp.Versions), len(quickResp.DeleteMarkers),
		quickResp.IsTruncated != nil && *quickResp.IsTruncated)

	// Test 1: List all versions without pagination limit
	t.Run("ListAllVersionsNoPagination", func(t *testing.T) {
		allVersions := listAllVersions(t, client, bucketName, objectKey)
		t.Logf("Got %d versions total", len(allVersions))
		assert.Len(t, allVersions, numVersions, "Should have all %d versions", numVersions)

		// Verify all version IDs are unique
		versionIdSet := make(map[string]bool)
		for _, v := range allVersions {
			assert.False(t, versionIdSet[*v.VersionId], "Duplicate version ID: %s", *v.VersionId)
			versionIdSet[*v.VersionId] = true
		}

		// Verify only one version is marked as latest
		latestCount := 0
		for _, v := range allVersions {
			if *v.IsLatest {
				latestCount++
				assert.Equal(t, versionIds[numVersions-1], *v.VersionId, "Latest should be the last created")
			}
		}
		assert.Equal(t, 1, latestCount, "Only one version should be marked as latest")
	})

	// Test 2: List with explicit small max-keys to force multiple pages
	t.Run("ListWithSmallMaxKeys", func(t *testing.T) {
		maxKeys := int32(100)
		allVersions := make([]types.ObjectVersion, 0, numVersions)
		var keyMarker, versionIdMarker *string
		pageCount := 0

		for {
			resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
				Bucket:          aws.String(bucketName),
				Prefix:          aws.String(objectKey),
				MaxKeys:         aws.Int32(maxKeys),
				KeyMarker:       keyMarker,
				VersionIdMarker: versionIdMarker,
			})
			require.NoError(t, err)
			pageCount++

			allVersions = append(allVersions, resp.Versions...)

			if resp.IsTruncated == nil || !*resp.IsTruncated {
				break
			}
			keyMarker = resp.NextKeyMarker
			versionIdMarker = resp.NextVersionIdMarker
		}

		t.Logf("Listed %d versions in %d pages (max-keys=%d)", len(allVersions), pageCount, maxKeys)
		assert.Len(t, allVersions, numVersions, "Should have all %d versions", numVersions)
		if numVersions > int(maxKeys) {
			assert.Greater(t, pageCount, 1, "Should require multiple pages when versions > max-keys")
		}
	})

	// Test 3: List with max-keys=1000 (S3 default) to test the boundary
	t.Run("ListWithDefaultMaxKeys", func(t *testing.T) {
		maxKeys := int32(1000)
		allVersions := make([]types.ObjectVersion, 0, numVersions)
		var keyMarker, versionIdMarker *string
		pageCount := 0

		for {
			resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
				Bucket:          aws.String(bucketName),
				Prefix:          aws.String(objectKey),
				MaxKeys:         aws.Int32(maxKeys),
				KeyMarker:       keyMarker,
				VersionIdMarker: versionIdMarker,
			})
			require.NoError(t, err)
			pageCount++

			allVersions = append(allVersions, resp.Versions...)
			t.Logf("Page %d: got %d versions, truncated=%v", pageCount, len(resp.Versions), resp.IsTruncated != nil && *resp.IsTruncated)

			if resp.IsTruncated == nil || !*resp.IsTruncated {
				break
			}
			keyMarker = resp.NextKeyMarker
			versionIdMarker = resp.NextVersionIdMarker
		}

		t.Logf("Listed %d versions in %d pages (max-keys=%d)", len(allVersions), pageCount, maxKeys)
		assert.Len(t, allVersions, numVersions, "Should have all %d versions", numVersions)
		if numVersions > 1000 {
			assert.GreaterOrEqual(t, pageCount, 2, "Should require at least 2 pages for >1000 versions with max-keys=1000")
		}
	})

	// Test 4: Verify we can retrieve specific versions
	t.Run("RetrieveSpecificVersions", func(t *testing.T) {
		// Test first, middle, and last versions
		testIndices := []int{0, numVersions / 2, numVersions - 1}
		for _, idx := range testIndices {
			expectedContent := fmt.Sprintf("content-version-%d", idx+1)
			getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
				Bucket:    aws.String(bucketName),
				Key:       aws.String(objectKey),
				VersionId: aws.String(versionIds[idx]),
			})
			require.NoError(t, err, "Failed to get version at index %d", idx)

			buf := make([]byte, len(expectedContent))
			_, err = getResp.Body.Read(buf)
			getResp.Body.Close()

			assert.Equal(t, expectedContent, string(buf), "Content mismatch for version %d", idx+1)
		}
	})
}

// TestVersioningPaginationMultipleObjectsManyVersions tests pagination with multiple objects
// each having many versions, ensuring correct handling of key-marker and version-id-marker.
//
// Run with: ENABLE_STRESS_TESTS=true go test -v -run TestVersioningPaginationMultipleObjectsManyVersions -timeout 30m
func TestVersioningPaginationMultipleObjectsManyVersions(t *testing.T) {
	if os.Getenv("ENABLE_STRESS_TESTS") != "true" {
		t.Skip("Skipping stress test. Set ENABLE_STRESS_TESTS=true to run.")
	}

	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	enableVersioning(t, client, bucketName)
	checkVersioningStatus(t, client, bucketName, types.BucketVersioningStatusEnabled)

	numObjects := 5
	versionsPerObject := 300
	totalExpectedVersions := numObjects * versionsPerObject

	t.Logf("Creating %d objects with %d versions each (%d total)...", numObjects, versionsPerObject, totalExpectedVersions)
	startTime := time.Now()

	// Create multiple objects with many versions each
	for i := 0; i < numObjects; i++ {
		objectKey := fmt.Sprintf("object-%03d", i)
		for j := 0; j < versionsPerObject; j++ {
			content := fmt.Sprintf("obj%d-ver%d", i, j+1)
			_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
				Body:   strings.NewReader(content),
			})
			require.NoError(t, err)
		}
		t.Logf("Created object %s with %d versions", objectKey, versionsPerObject)
	}

	createDuration := time.Since(startTime)
	t.Logf("Created %d total versions in %v", totalExpectedVersions, createDuration)

	// List all versions using pagination
	t.Run("ListAllWithPagination", func(t *testing.T) {
		maxKeys := int32(500)
		allVersions := make([]types.ObjectVersion, 0, totalExpectedVersions)
		var keyMarker, versionIdMarker *string
		pageCount := 0

		for {
			resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
				Bucket:          aws.String(bucketName),
				MaxKeys:         aws.Int32(maxKeys),
				KeyMarker:       keyMarker,
				VersionIdMarker: versionIdMarker,
			})
			require.NoError(t, err)
			pageCount++

			allVersions = append(allVersions, resp.Versions...)
			t.Logf("Page %d: got %d versions, truncated=%v", pageCount, len(resp.Versions), resp.IsTruncated != nil && *resp.IsTruncated)

			if resp.IsTruncated == nil || !*resp.IsTruncated {
				break
			}
			keyMarker = resp.NextKeyMarker
			versionIdMarker = resp.NextVersionIdMarker
		}

		t.Logf("Listed %d versions in %d pages", len(allVersions), pageCount)
		assert.Len(t, allVersions, totalExpectedVersions, "Should have all versions")

		// Verify each object has correct number of versions
		versionsByKey := make(map[string]int)
		for _, v := range allVersions {
			versionsByKey[*v.Key]++
		}
		assert.Len(t, versionsByKey, numObjects, "Should have %d unique objects", numObjects)
		for key, count := range versionsByKey {
			assert.Equal(t, versionsPerObject, count, "Object %s should have %d versions", key, versionsPerObject)
		}

		// Verify each object has exactly one latest version
		latestByKey := make(map[string]int)
		for _, v := range allVersions {
			if *v.IsLatest {
				latestByKey[*v.Key]++
			}
		}
		for key, count := range latestByKey {
			assert.Equal(t, 1, count, "Object %s should have exactly one latest version", key)
		}
	})
}

// listAllVersions is a helper to list all versions of a specific object using pagination
func listAllVersions(t *testing.T, client *s3.Client, bucketName, objectKey string) []types.ObjectVersion {
	var allVersions []types.ObjectVersion
	var keyMarker, versionIdMarker *string
	pageCount := 0

	for {
		resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket:          aws.String(bucketName),
			Prefix:          aws.String(objectKey),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionIdMarker,
		})
		require.NoError(t, err)
		pageCount++

		t.Logf("Page %d: got %d versions, truncated=%v", pageCount, len(resp.Versions), resp.IsTruncated != nil && *resp.IsTruncated)
		allVersions = append(allVersions, resp.Versions...)

		// Check if truncated (handle nil pointer)
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		keyMarker = resp.NextKeyMarker
		versionIdMarker = resp.NextVersionIdMarker
	}

	t.Logf("Total: %d versions in %d pages", len(allVersions), pageCount)
	return allVersions
}

