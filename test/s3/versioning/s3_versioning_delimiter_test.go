package s3api

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListObjectVersionsDelimiter tests delimiter functionality in ListObjectVersions
func TestListObjectVersionsDelimiter(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create test structure:
	// - folder1/file1.txt
	// - folder1/file2.txt
	// - folder2/file3.txt
	// - root-file.txt
	testObjects := []string{
		"folder1/file1.txt",
		"folder1/file2.txt",
		"folder2/file3.txt",
		"root-file.txt",
	}

	for _, key := range testObjects {
		putObject(t, client, bucketName, key, fmt.Sprintf("Content of %s", key))
	}

	t.Run("Delimiter groups folders correctly", func(t *testing.T) {
		// List with delimiter='/' and no prefix
		// Should return: root-file.txt and CommonPrefixes: folder1/, folder2/
		resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucketName),
			Delimiter: aws.String("/"),
		})
		require.NoError(t, err)

		// Extract keys and prefixes
		versionKeys := make([]string, 0)
		for _, v := range resp.Versions {
			versionKeys = append(versionKeys, *v.Key)
		}

		prefixValues := make([]string, 0)
		for _, p := range resp.CommonPrefixes {
			prefixValues = append(prefixValues, *p.Prefix)
		}

		// Verify results
		assert.Contains(t, versionKeys, "root-file.txt", "Should include root-file.txt")
		assert.Contains(t, prefixValues, "folder1/", "Should include folder1/ prefix")
		assert.Contains(t, prefixValues, "folder2/", "Should include folder2/ prefix")
		assert.NotContains(t, versionKeys, "folder1/file1.txt", "folder1/file1.txt should be grouped under folder1/")
		assert.NotContains(t, versionKeys, "folder2/file3.txt", "folder2/file3.txt should be grouped under folder2/")

		t.Logf("✓ Versions: %v", versionKeys)
		t.Logf("✓ CommonPrefixes: %v", prefixValues)
	})

	t.Run("Prefix filtering with delimiter", func(t *testing.T) {
		// List with delimiter='/' and prefix='folder1/'
		// Should return: folder1/file1.txt, folder1/file2.txt
		resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucketName),
			Prefix:    aws.String("folder1/"),
			Delimiter: aws.String("/"),
		})
		require.NoError(t, err)

		versionKeys := make([]string, 0)
		for _, v := range resp.Versions {
			versionKeys = append(versionKeys, *v.Key)
		}

		assert.Len(t, versionKeys, 2, "Should have 2 versions")
		assert.Contains(t, versionKeys, "folder1/file1.txt")
		assert.Contains(t, versionKeys, "folder1/file2.txt")
		assert.Empty(t, resp.CommonPrefixes, "Should have no common prefixes")

		t.Logf("✓ Prefix filtering works: %v", versionKeys)
	})

	t.Run("Without delimiter returns all versions", func(t *testing.T) {
		// List without delimiter - should return all files
		resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucketName),
		})
		require.NoError(t, err)

		assert.Len(t, resp.Versions, 4, "Should have all 4 versions")
		assert.Empty(t, resp.CommonPrefixes, "Should have no common prefixes without delimiter")

		t.Logf("✓ Without delimiter returns all %d versions", len(resp.Versions))
	})
}

// TestListObjectVersionsDelimiterTruncation tests MaxKeys with delimiter
func TestListObjectVersionsDelimiterTruncation(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create multiple folders to test truncation
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("folder%d/file.txt", i)
		putObject(t, client, bucketName, key, fmt.Sprintf("Content %d", i))
	}
	// Add a root file
	putObject(t, client, bucketName, "root.txt", "Root content")

	t.Run("MaxKeys limits total items", func(t *testing.T) {
		resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucketName),
			Delimiter: aws.String("/"),
			MaxKeys:   aws.Int32(3),
		})
		require.NoError(t, err)

		totalItems := len(resp.Versions) + len(resp.CommonPrefixes)
		assert.LessOrEqual(t, totalItems, 3, "Total items should not exceed MaxKeys")
		assert.True(t, *resp.IsTruncated, "Should be truncated")
		assert.NotNil(t, resp.NextKeyMarker, "Should have NextKeyMarker for pagination")

		t.Logf("✓ MaxKeys truncation: %d items (versions: %d, prefixes: %d)",
			totalItems, len(resp.Versions), len(resp.CommonPrefixes))
	})

	t.Run("Pagination with delimiter", func(t *testing.T) {
		// Collect all items through pagination
		allKeys := make([]string, 0)
		allPrefixes := make([]string, 0)
		var keyMarker *string
		var versionMarker *string

		for {
			input := &s3.ListObjectVersionsInput{
				Bucket:    aws.String(bucketName),
				Delimiter: aws.String("/"),
				MaxKeys:   aws.Int32(2),
			}
			if keyMarker != nil {
				input.KeyMarker = keyMarker
			}
			if versionMarker != nil {
				input.VersionIdMarker = versionMarker
			}

			resp, err := client.ListObjectVersions(context.TODO(), input)
			require.NoError(t, err)

			// Collect versions
			for _, v := range resp.Versions {
				allKeys = append(allKeys, *v.Key)
			}

			// Collect prefixes
			for _, p := range resp.CommonPrefixes {
				allPrefixes = append(allPrefixes, *p.Prefix)
			}

			if !*resp.IsTruncated {
				break
			}

			keyMarker = resp.NextKeyMarker
			versionMarker = resp.NextVersionIdMarker
		}

		// Should have collected all items
		totalItems := len(allKeys) + len(allPrefixes)
		assert.GreaterOrEqual(t, totalItems, 6, "Should collect all items through pagination")

		t.Logf("✓ Pagination collected %d total items (keys: %d, prefixes: %d)",
			totalItems, len(allKeys), len(allPrefixes))
	})
}

// TestListObjectVersionsDelimiterWithMultipleVersions tests delimiter with multiple versions of same object
func TestListObjectVersionsDelimiterWithMultipleVersions(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create multiple versions of objects in different folders
	for i := 1; i <= 3; i++ {
		putObject(t, client, bucketName, "folder1/file.txt", fmt.Sprintf("Version %d", i))
		putObject(t, client, bucketName, "folder2/file.txt", fmt.Sprintf("Version %d", i))
	}

	t.Run("Delimiter groups all versions under prefix", func(t *testing.T) {
		resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucketName),
			Delimiter: aws.String("/"),
		})
		require.NoError(t, err)

		// Should have no versions at root level, only common prefixes
		assert.Empty(t, resp.Versions, "Should have no versions at root")
		assert.Len(t, resp.CommonPrefixes, 2, "Should have 2 common prefixes")

		prefixValues := make([]string, 0)
		for _, p := range resp.CommonPrefixes {
			prefixValues = append(prefixValues, *p.Prefix)
		}
		assert.Contains(t, prefixValues, "folder1/")
		assert.Contains(t, prefixValues, "folder2/")

		t.Logf("✓ All versions grouped under prefixes: %v", prefixValues)
	})

	t.Run("Listing within prefix shows all versions", func(t *testing.T) {
		resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucketName),
			Prefix:    aws.String("folder1/"),
			Delimiter: aws.String("/"),
		})
		require.NoError(t, err)

		assert.Len(t, resp.Versions, 3, "Should have 3 versions of folder1/file.txt")
		assert.Empty(t, resp.CommonPrefixes, "Should have no common prefixes")

		// Verify all versions are for the same key
		for _, v := range resp.Versions {
			assert.Equal(t, "folder1/file.txt", *v.Key)
		}

		t.Logf("✓ Found %d versions within prefix", len(resp.Versions))
	})
}
