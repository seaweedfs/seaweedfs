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
	"github.com/k0kubun/pp"
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

// Default test configuration - should match s3tests.conf
var defaultConfig = &S3TestConfig{
	Endpoint:      "http://localhost:8333", // Default SeaweedFS S3 port
	AccessKey:     "some_access_key1",
	SecretKey:     "some_secret_key1",
	Region:        "us-east-1",
	BucketPrefix:  "test-versioning-",
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

// checkVersioningStatus verifies the versioning status of a bucket
func checkVersioningStatus(t *testing.T, client *s3.Client, bucketName string, expectedStatus types.BucketVersioningStatus) {
	resp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	assert.Equal(t, expectedStatus, resp.Status)
}

// checkVersioningStatusEmpty verifies that a bucket has no versioning configuration (newly created bucket)
func checkVersioningStatusEmpty(t *testing.T, client *s3.Client, bucketName string) {
	resp, err := client.GetBucketVersioning(context.TODO(), &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	// AWS S3 returns an empty versioning configuration (no Status field) for buckets that have never had versioning configured, such as newly created buckets.
	assert.Empty(t, resp.Status, "Newly created bucket should have empty versioning status")
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

// headObject gets object metadata
func headObject(t *testing.T, client *s3.Client, bucketName, key string) *s3.HeadObjectOutput {
	resp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	return resp
}

// TestBucketListReturnDataVersioning is the Go equivalent of test_bucket_list_return_data_versioning
func TestBucketListReturnDataVersioning(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Enable versioning
	enableVersioning(t, client, bucketName)
	checkVersioningStatus(t, client, bucketName, types.BucketVersioningStatusEnabled)

	// Create test objects
	keyNames := []string{"bar", "baz", "foo"}
	objectData := make(map[string]map[string]interface{})

	for _, keyName := range keyNames {
		// Put the object
		putResp := putObject(t, client, bucketName, keyName, keyName) // content = key name

		// Get object metadata
		headResp := headObject(t, client, bucketName, keyName)

		// Store expected data for later comparison
		objectData[keyName] = map[string]interface{}{
			"ETag":          *headResp.ETag,
			"LastModified":  *headResp.LastModified,
			"ContentLength": headResp.ContentLength,
			"VersionId":     *headResp.VersionId,
		}

		// Verify version ID was returned
		require.NotNil(t, putResp.VersionId)
		require.NotEmpty(t, *putResp.VersionId)
		assert.Equal(t, *putResp.VersionId, *headResp.VersionId)
	}

	// List object versions
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Verify we have the expected number of versions
	assert.Len(t, resp.Versions, len(keyNames))

	// Check each version matches our stored data
	versionsByKey := make(map[string]types.ObjectVersion)
	for _, version := range resp.Versions {
		versionsByKey[*version.Key] = version
	}

	for _, keyName := range keyNames {
		version, exists := versionsByKey[keyName]
		require.True(t, exists, "Expected version for key %s", keyName)

		expectedData := objectData[keyName]

		// Compare ETag
		assert.Equal(t, expectedData["ETag"], *version.ETag)

		// Compare Size
		assert.Equal(t, expectedData["ContentLength"], version.Size)

		// Compare VersionId
		assert.Equal(t, expectedData["VersionId"], *version.VersionId)

		// Compare LastModified (within reasonable tolerance)
		expectedTime := expectedData["LastModified"].(time.Time)
		actualTime := *version.LastModified
		timeDiff := actualTime.Sub(expectedTime)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}
		assert.True(t, timeDiff < time.Minute, "LastModified times should be close")

		// Verify this is marked as the latest version
		assert.True(t, *version.IsLatest)

		// Verify it's not a delete marker
		// (delete markers should be in resp.DeleteMarkers, not resp.Versions)
	}

	// Verify no delete markers
	assert.Empty(t, resp.DeleteMarkers)

	t.Logf("Successfully verified %d versioned objects", len(keyNames))
}

// TestVersioningBasicWorkflow tests basic versioning operations
func TestVersioningBasicWorkflow(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	// Initially, versioning should be unset/empty (not suspended) for newly created buckets
	// This matches AWS S3 behavior where new buckets have no versioning status
	checkVersioningStatusEmpty(t, client, bucketName)

	// Enable versioning
	enableVersioning(t, client, bucketName)
	checkVersioningStatus(t, client, bucketName, types.BucketVersioningStatusEnabled)

	// Put same object multiple times to create versions
	key := "test-object"
	version1 := putObject(t, client, bucketName, key, "content-v1")
	version2 := putObject(t, client, bucketName, key, "content-v2")
	version3 := putObject(t, client, bucketName, key, "content-v3")

	// Verify each put returned a different version ID
	require.NotEqual(t, *version1.VersionId, *version2.VersionId)
	require.NotEqual(t, *version2.VersionId, *version3.VersionId)
	require.NotEqual(t, *version1.VersionId, *version3.VersionId)

	// List versions
	resp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Should have 3 versions
	assert.Len(t, resp.Versions, 3)

	// Only the latest should be marked as latest
	latestCount := 0
	for _, version := range resp.Versions {
		if *version.IsLatest {
			latestCount++
			assert.Equal(t, *version3.VersionId, *version.VersionId)
		}
	}
	assert.Equal(t, 1, latestCount, "Only one version should be marked as latest")

	t.Logf("Successfully created and verified %d versions", len(resp.Versions))
}

// TestVersioningDeleteMarkers tests delete marker creation
func TestVersioningDeleteMarkers(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Put an object
	key := "test-delete-marker"
	putResp := putObject(t, client, bucketName, key, "content")
	require.NotNil(t, putResp.VersionId)

	// Delete the object (should create delete marker)
	deleteResp, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp.VersionId)

	// List versions to see the delete marker
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Should have 1 version and 1 delete marker
	assert.Len(t, listResp.Versions, 1)
	assert.Len(t, listResp.DeleteMarkers, 1)

	// The delete marker should be the latest
	deleteMarker := listResp.DeleteMarkers[0]
	assert.True(t, *deleteMarker.IsLatest)
	assert.Equal(t, *deleteResp.VersionId, *deleteMarker.VersionId)

	// The original version should not be latest
	version := listResp.Versions[0]
	assert.False(t, *version.IsLatest)
	assert.Equal(t, *putResp.VersionId, *version.VersionId)

	t.Logf("Successfully created and verified delete marker")
}

// TestVersioningConcurrentOperations tests concurrent versioning operations
func TestVersioningConcurrentOperations(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	// Create bucket and enable versioning
	createBucket(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)
	enableVersioning(t, client, bucketName)

	// Concurrently create multiple objects
	numObjects := 10
	objectKey := "concurrent-test"

	// Channel to collect version IDs
	versionIds := make(chan string, numObjects)
	errors := make(chan error, numObjects)

	// Launch concurrent puts
	for i := 0; i < numObjects; i++ {
		go func(index int) {
			content := fmt.Sprintf("content-%d", index)
			resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(objectKey),
				Body:   strings.NewReader(content),
			})
			if err != nil {
				errors <- err
				return
			}
			versionIds <- *resp.VersionId
		}(i)
	}

	// Collect results
	var collectedVersionIds []string
	for i := 0; i < numObjects; i++ {
		select {
		case versionId := <-versionIds:
			t.Logf("Received Version ID %d: %s", i, versionId)
			collectedVersionIds = append(collectedVersionIds, versionId)
		case err := <-errors:
			t.Fatalf("Concurrent put failed: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatalf("Timeout waiting for concurrent operations")
		}
	}

	// Verify all version IDs are unique
	versionIdSet := make(map[string]bool)
	for _, versionId := range collectedVersionIds {
		assert.False(t, versionIdSet[versionId], "Version ID should be unique: %s", versionId)
		versionIdSet[versionId] = true
	}

	// List versions and verify count
	listResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	pp.Println(listResp)
	require.NoError(t, err)
	assert.Len(t, listResp.Versions, numObjects)

	t.Logf("Successfully created %d concurrent versions with unique IDs", numObjects)
}
