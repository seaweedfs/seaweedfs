package s3api

import (
	"context"
	"strings"
	"testing"

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

		// Add versions
		for _, version := range page.Versions {
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

		// Delete objects in batches
		if len(objectsToDelete) > 0 {
			_, err := client.DeleteObjects(context.TODO(), &s3.DeleteObjectsInput{
				Bucket: aws.String(bucketName),
				Delete: &types.Delete{
					Objects: objectsToDelete,
					Quiet:   aws.Bool(true),
				},
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}
