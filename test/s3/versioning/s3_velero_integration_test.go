package s3api

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVeleroKopiaVersionedObjectListing tests the exact access patterns used by Velero/Kopia
// when backing up to a versioned S3 bucket. This test validates the fix for:
// https://github.com/seaweedfs/seaweedfs/discussions/7573
//
// The bug was that when listing versioned objects with nested paths like
// "kopia/logpaste/kopia.blobcfg", the returned Key would be doubled:
// "kopia/logpaste/kopia/logpaste/kopia.blobcfg" instead of the correct path.
//
// This was caused by getLatestVersionEntryForListOperation setting the entry's
// Name to the full path instead of just the base filename.
func TestVeleroKopiaVersionedObjectListing(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Step 1: Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Step 2: Enable versioning (required to reproduce the bug)
	enableVersioning(t, client, bucketName)

	// Step 3: Create objects matching Velero/Kopia's typical path structure
	// These are the actual paths Kopia uses when storing repository data
	testObjects := map[string]string{
		"kopia/logpaste/kopia.blobcfg":    "blob-config-content",
		"kopia/logpaste/kopia.repository": "repository-content",
		"kopia/logpaste/.storageconfig":   "storage-config-content",
		"backups/namespace1/backup.tar":   "backup-content",
		"restic/locks/lock-001.json":      "lock-content",
		"restic/index/index-001.json":     "index-content",
		"data/a/b/c/deeply-nested.dat":    "nested-content",
		"single-file.txt":                 "single-file-content",
	}

	// Upload all test objects
	for key, content := range testObjects {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err, "Failed to upload object %s", key)
	}

	// Step 4: Test listing with various prefixes (mimicking Kopia's ListObjects calls)
	testCases := []struct {
		name         string
		prefix       string
		expectedKeys []string
		description  string
	}{
		{
			name:   "kopia repository prefix",
			prefix: "kopia/logpaste/",
			expectedKeys: []string{
				"kopia/logpaste/.storageconfig",
				"kopia/logpaste/kopia.blobcfg",
				"kopia/logpaste/kopia.repository",
			},
			description: "Kopia lists its repository directory to find config files",
		},
		{
			name:         "exact config file",
			prefix:       "kopia/logpaste/kopia.blobcfg",
			expectedKeys: []string{"kopia/logpaste/kopia.blobcfg"},
			description:  "Kopia checks for specific config files",
		},
		{
			name:   "restic index directory",
			prefix: "restic/index/",
			expectedKeys: []string{
				"restic/index/index-001.json",
			},
			description: "Restic lists index files (similar pattern to Kopia)",
		},
		{
			name:   "deeply nested path",
			prefix: "data/a/b/c/",
			expectedKeys: []string{
				"data/a/b/c/deeply-nested.dat",
			},
			description: "Tests deeply nested paths don't get doubled",
		},
		{
			name:   "backup directory",
			prefix: "backups/",
			expectedKeys: []string{
				"backups/namespace1/backup.tar",
			},
			description: "Velero lists backup directories",
		},
		{
			name:   "root level single file",
			prefix: "single-file",
			expectedKeys: []string{
				"single-file.txt",
			},
			description: "Files at bucket root should work correctly",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// List objects with prefix
			listResp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
				Bucket: aws.String(bucketName),
				Prefix: aws.String(tc.prefix),
			})
			require.NoError(t, err)

			// Extract returned keys
			var actualKeys []string
			for _, obj := range listResp.Contents {
				key := *obj.Key
				actualKeys = append(actualKeys, key)

				// Critical assertion: verify no path doubling
				// The bug would cause "kopia/logpaste/kopia.blobcfg" to become
				// "kopia/logpaste/kopia/logpaste/kopia.blobcfg"
				assert.False(t, hasDoubledPath(key),
					"Path doubling detected! Key '%s' should not have repeated path segments", key)

				// Also verify we can actually GET the object using the listed key
				getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(key),
				})
				require.NoError(t, err, "Should be able to GET object using listed key: %s", key)
				getResp.Body.Close()
			}

			// Verify we got the expected keys
			assert.ElementsMatch(t, tc.expectedKeys, actualKeys,
				"%s: Listed keys should match expected", tc.description)
		})
	}
}

// TestVeleroKopiaGetAfterList simulates Kopia's pattern of listing and then getting objects
// This specifically tests the bug where listed paths couldn't be retrieved
func TestVeleroKopiaGetAfterList(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Create a nested object like Kopia does
	objectKey := "kopia/appwrite/kopia.repository"
	objectContent := "kopia-repository-config-data"

	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(objectContent),
	})
	require.NoError(t, err)

	// List objects (this is where the bug manifested - returned wrong Key)
	listResp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("kopia/appwrite/"),
	})
	require.NoError(t, err)
	require.Len(t, listResp.Contents, 1, "Should list exactly one object")

	listedKey := *listResp.Contents[0].Key

	// The bug caused listedKey to be "kopia/appwrite/kopia/appwrite/kopia.repository"
	// instead of "kopia/appwrite/kopia.repository"
	assert.Equal(t, objectKey, listedKey,
		"Listed key should match the original key without path doubling")

	// Now try to GET the object using the listed key
	// With the bug, this would fail because the doubled path doesn't exist
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(listedKey),
	})
	require.NoError(t, err, "Should be able to GET object using the key returned from ListObjects")
	defer getResp.Body.Close()

	// Verify content matches
	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, objectContent, string(body),
		"Retrieved content should match original")
}

// TestVeleroMultipleVersionsWithNestedPaths tests listing when objects have multiple versions
func TestVeleroMultipleVersionsWithNestedPaths(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	objectKey := "kopia/logpaste/kopia.blobcfg"

	// Create multiple versions of the same object (Kopia updates config files)
	versions := []string{"version-1", "version-2", "version-3"}
	for _, content := range versions {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(content),
		})
		require.NoError(t, err)
	}

	// List objects - should return only the latest version in regular listing
	listResp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("kopia/logpaste/"),
	})
	require.NoError(t, err)

	// Should only see one object (the latest version)
	require.Len(t, listResp.Contents, 1, "Should list only one object (latest version)")

	listedKey := *listResp.Contents[0].Key
	assert.Equal(t, objectKey, listedKey,
		"Listed key should match original without path doubling")
	assert.False(t, hasDoubledPath(listedKey),
		"Path should not be doubled")

	// Verify we can GET the latest version
	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(listedKey),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	body, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, "version-3", string(body),
		"Should get the latest version content")
}

// TestVeleroListVersionsWithNestedPaths tests ListObjectVersions with nested paths
func TestVeleroListVersionsWithNestedPaths(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	objectKey := "backups/velero/test-backup/backup.json"

	// Create multiple versions
	for i := 1; i <= 3; i++ {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   strings.NewReader(strings.Repeat("x", i*100)),
		})
		require.NoError(t, err)
	}

	// List all versions
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("backups/velero/"),
	})
	require.NoError(t, err)

	// Should have 3 versions
	require.Len(t, listResp.Versions, 3, "Should have 3 versions")

	// Verify all version keys are correct (not doubled)
	for _, version := range listResp.Versions {
		key := *version.Key
		assert.Equal(t, objectKey, key,
			"Version key should match original")
		assert.False(t, hasDoubledPath(key),
			"Version key should not have doubled path: %s", key)

		// Verify we can GET each version by its version ID
		getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(key),
			VersionId: version.VersionId,
		})
		require.NoError(t, err, "Should be able to GET version %s", *version.VersionId)
		func() {
			defer getResp.Body.Close()
		}()
	}
}

// hasDoubledPath checks if a path has repeated segments indicating the path doubling bug
// For example: "kopia/logpaste/kopia/logpaste/file.txt" would return true
func hasDoubledPath(path string) bool {
	parts := strings.Split(path, "/")
	if len(parts) < 4 {
		return false
	}

	// Check for repeated consecutive segments
	// The bug pattern: [prefix]/[subdir]/[prefix]/[subdir]/...
	for i := 0; i < len(parts)-2; i++ {
		// Check if we have a repeated pair of segments
		for j := i + 2; j < len(parts)-1; j++ {
			if parts[i] == parts[j] && i+1 < len(parts) && j+1 < len(parts) && parts[i+1] == parts[j+1] {
				return true
			}
		}
	}

	return false
}
