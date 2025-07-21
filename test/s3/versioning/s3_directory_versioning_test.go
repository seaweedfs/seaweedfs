package s3api

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListObjectVersionsIncludesDirectories tests that directories are included in list-object-versions response
// This ensures compatibility with Minio and AWS S3 behavior
func TestListObjectVersionsIncludesDirectories(t *testing.T) {
	bucketName := "test-versioning-directories"

	client := setupS3Client()

	// Create bucket
	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Clean up
	defer func() {
		cleanupBucket(client, bucketName)
	}()

	// Enable versioning
	_, err = client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// Create directory structure by putting objects (which creates directories implicitly)
	testObjects := []string{
		"Veeam/test-file.txt",
		"Veeam/Archive/test-file2.txt",
		"Veeam/Archive/vbr/test-file3.txt",
		"Veeam/Backup/test-file4.txt",
		"Veeam/Backup/vbr/test-file5.txt",
		"Veeam/Backup/vbr/Clients/test-file6.txt",
	}

	// Upload test objects to create directory structure
	for _, objectKey := range testObjects {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader("test content"),
		})
		require.NoError(t, err, "Failed to create object %s", objectKey)
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
	for _, objectKey := range testObjects {
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
	assert.Equal(t, len(testObjects), fileCount, "Should find exactly %d files", len(testObjects))
}

// Helper function to setup S3 client (assumes this exists in the test package)
func setupS3Client() *s3.Client {
	// This should be implemented based on existing test setup
	// Usually involves setting up endpoint, credentials, etc.
	return nil // Placeholder - implement based on existing pattern
}

// Helper function to clean up bucket (assumes this exists in the test package)
func cleanupBucket(client *s3.Client, bucketName string) {
	// This should be implemented based on existing test cleanup
	// Usually involves deleting all objects and then the bucket
}
