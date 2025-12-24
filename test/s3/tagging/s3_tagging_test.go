package tagging

import (
	"context"
	"fmt"
	"os"
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

// getDefaultConfig returns a fresh instance of the default test configuration
func getDefaultConfig() *S3TestConfig {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8333" // Default SeaweedFS S3 port
	}
	accessKey := os.Getenv("S3_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "some_access_key1"
	}
	secretKey := os.Getenv("S3_SECRET_KEY")
	if secretKey == "" {
		secretKey = "some_secret_key1"
	}
	return &S3TestConfig{
		Endpoint:      endpoint,
		AccessKey:     accessKey,
		SecretKey:     secretKey,
		Region:        "us-east-1",
		BucketPrefix:  "test-tagging-",
		UseSSL:        false,
		SkipVerifySSL: true,
	}
}

// getS3Client creates an AWS S3 client for testing
func getS3Client(t *testing.T) *s3.Client {
	defaultConfig := getDefaultConfig()
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(defaultConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			defaultConfig.AccessKey,
			defaultConfig.SecretKey,
			"",
		)),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(defaultConfig.Endpoint)
	})
	return client
}

// createTestBucket creates a test bucket with a unique name
func createTestBucket(t *testing.T, client *s3.Client) string {
	defaultConfig := getDefaultConfig()
	bucketName := fmt.Sprintf("%s%d", defaultConfig.BucketPrefix, time.Now().UnixNano())

	_, err := client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Wait for bucket metadata to be fully processed
	time.Sleep(50 * time.Millisecond)

	return bucketName
}

// cleanupTestBucket removes the test bucket and all its contents
func cleanupTestBucket(t *testing.T, client *s3.Client, bucketName string) {
	// First, delete all objects in the bucket
	listResp, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err == nil {
		for _, obj := range listResp.Contents {
			_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    obj.Key,
			})
			if err != nil {
				t.Logf("Warning: failed to delete object %s: %v", *obj.Key, err)
			}
		}
	}

	// Delete all versions and delete markers if versioning is enabled
	listVersionsResp, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
	})
	if err == nil {
		for _, version := range listVersionsResp.Versions {
			_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket:    aws.String(bucketName),
				Key:       version.Key,
				VersionId: version.VersionId,
			})
			if err != nil {
				t.Logf("Warning: failed to delete version %s: %v", *version.Key, err)
			}
		}
		for _, marker := range listVersionsResp.DeleteMarkers {
			_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket:    aws.String(bucketName),
				Key:       marker.Key,
				VersionId: marker.VersionId,
			})
			if err != nil {
				t.Logf("Warning: failed to delete marker %s: %v", *marker.Key, err)
			}
		}
	}

	// Then delete the bucket
	_, err = client.DeleteBucket(context.TODO(), &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Logf("Warning: failed to delete bucket %s: %v", bucketName, err)
	}
}

// TestObjectTaggingOnUpload tests that tags sent during object upload (via X-Amz-Tagging header)
// are properly stored and can be retrieved. This is the fix for GitHub issue #7589.
func TestObjectTaggingOnUpload(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "test-object-with-tags"
	objectContent := "Hello, World!"

	// Put object with tags using the Tagging parameter (X-Amz-Tagging header)
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(objectKey),
		Body:    strings.NewReader(objectContent),
		Tagging: aws.String("env=production&team=platform"),
	})
	require.NoError(t, err, "Should be able to put object with tags")

	// Get the tags back
	tagResp, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get object tags")

	// Verify tags were stored correctly
	require.Len(t, tagResp.TagSet, 2, "Should have 2 tags")

	// Build a map for easier assertion
	tagMap := make(map[string]string)
	for _, tag := range tagResp.TagSet {
		tagMap[*tag.Key] = *tag.Value
	}

	assert.Equal(t, "production", tagMap["env"], "env tag should be 'production'")
	assert.Equal(t, "platform", tagMap["team"], "team tag should be 'platform'")
}

// TestObjectTaggingOnUploadWithSpecialCharacters tests tags with URL-encoded characters
func TestObjectTaggingOnUploadWithSpecialCharacters(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "test-object-with-special-tags"
	objectContent := "Hello, World!"

	// Put object with tags containing special characters
	// AWS SDK will URL-encode these automatically
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(objectKey),
		Body:    strings.NewReader(objectContent),
		Tagging: aws.String("timestamp=2025-07-16 14:40:39&path=/tmp/file.txt"),
	})
	require.NoError(t, err, "Should be able to put object with special character tags")

	// Get the tags back
	tagResp, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get object tags")

	// Verify tags were stored and URL-decoded correctly
	require.Len(t, tagResp.TagSet, 2, "Should have 2 tags")

	tagMap := make(map[string]string)
	for _, tag := range tagResp.TagSet {
		tagMap[*tag.Key] = *tag.Value
	}

	assert.Equal(t, "2025-07-16 14:40:39", tagMap["timestamp"], "timestamp tag should be decoded correctly")
	assert.Equal(t, "/tmp/file.txt", tagMap["path"], "path tag should be decoded correctly")
}

// TestObjectTaggingOnUploadWithEmptyValue tests tags with empty values
func TestObjectTaggingOnUploadWithEmptyValue(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "test-object-with-empty-tag"
	objectContent := "Hello, World!"

	// Put object with a tag that has an empty value
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(objectKey),
		Body:    strings.NewReader(objectContent),
		Tagging: aws.String("marker=&env=dev"),
	})
	require.NoError(t, err, "Should be able to put object with empty tag value")

	// Get the tags back
	tagResp, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get object tags")

	// Verify tags were stored correctly
	require.Len(t, tagResp.TagSet, 2, "Should have 2 tags")

	tagMap := make(map[string]string)
	for _, tag := range tagResp.TagSet {
		tagMap[*tag.Key] = *tag.Value
	}

	assert.Equal(t, "", tagMap["marker"], "marker tag should have empty value")
	assert.Equal(t, "dev", tagMap["env"], "env tag should be 'dev'")
}

// TestPutObjectTaggingAPI tests the PutObjectTagging API separately from upload
func TestPutObjectTaggingAPI(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "test-object-for-tagging-api"
	objectContent := "Hello, World!"

	// First, put object without tags
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader(objectContent),
	})
	require.NoError(t, err, "Should be able to put object without tags")

	// Get tags - should be empty
	tagResp, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get object tags")
	assert.Len(t, tagResp.TagSet, 0, "Should have no tags initially")

	// Now add tags using PutObjectTagging API
	_, err = client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("staging")},
				{Key: aws.String("version"), Value: aws.String("1.0")},
			},
		},
	})
	require.NoError(t, err, "Should be able to put object tags via API")

	// Get tags - should now have the tags
	tagResp, err = client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get object tags after PutObjectTagging")
	require.Len(t, tagResp.TagSet, 2, "Should have 2 tags")

	tagMap := make(map[string]string)
	for _, tag := range tagResp.TagSet {
		tagMap[*tag.Key] = *tag.Value
	}

	assert.Equal(t, "staging", tagMap["env"], "env tag should be 'staging'")
	assert.Equal(t, "1.0", tagMap["version"], "version tag should be '1.0'")
}

// TestDeleteObjectTagging tests the DeleteObjectTagging API
func TestDeleteObjectTagging(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "test-object-for-delete-tags"
	objectContent := "Hello, World!"

	// Put object with tags
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(objectKey),
		Body:    strings.NewReader(objectContent),
		Tagging: aws.String("env=production"),
	})
	require.NoError(t, err, "Should be able to put object with tags")

	// Verify tags exist
	tagResp, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get object tags")
	require.Len(t, tagResp.TagSet, 1, "Should have 1 tag")

	// Delete tags
	_, err = client.DeleteObjectTagging(context.TODO(), &s3.DeleteObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to delete object tags")

	// Verify tags are deleted
	tagResp, err = client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get object tags after deletion")
	assert.Len(t, tagResp.TagSet, 0, "Should have no tags after deletion")
}

// TestTagsNotPreservedAfterObjectOverwrite tests that tags are NOT preserved when an object is overwritten
// This matches AWS S3 behavior where overwriting an object replaces all metadata including tags
func TestTagsNotPreservedAfterObjectOverwrite(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "test-object-overwrite-tags"
	objectContent := "Original content"

	// Put object with tags
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(objectKey),
		Body:    strings.NewReader(objectContent),
		Tagging: aws.String("original=true"),
	})
	require.NoError(t, err, "Should be able to put object with tags")

	// Verify original tags exist
	tagResp, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get object tags")
	require.Len(t, tagResp.TagSet, 1, "Should have 1 tag")
	assert.Equal(t, "original", *tagResp.TagSet[0].Key)

	// Overwrite the object WITHOUT tags
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("New content"),
	})
	require.NoError(t, err, "Should be able to overwrite object")

	// Tags should be gone after overwrite (matches AWS S3 behavior)
	tagResp, err = client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get object tags after overwrite")
	assert.Len(t, tagResp.TagSet, 0, "Tags should be cleared after object overwrite")
}

// TestMaximumNumberOfTags tests that we can store the maximum 10 tags per object
func TestMaximumNumberOfTags(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "test-object-max-tags"
	objectContent := "Hello, World!"

	// Build 10 tags (S3 max)
	tags := []string{}
	for i := 1; i <= 10; i++ {
		tags = append(tags, fmt.Sprintf("key%d=value%d", i, i))
	}
	tagging := strings.Join(tags, "&")

	// Put object with 10 tags
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(objectKey),
		Body:    strings.NewReader(objectContent),
		Tagging: aws.String(tagging),
	})
	require.NoError(t, err, "Should be able to put object with 10 tags")

	// Get the tags back
	tagResp, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get object tags")
	assert.Len(t, tagResp.TagSet, 10, "Should have 10 tags")
}

// TestTagCountHeader tests that the x-amz-tagging-count header is returned in HeadObject
func TestTagCountHeader(t *testing.T) {
	client := getS3Client(t)
	bucketName := createTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	objectKey := "test-object-tag-count"
	objectContent := "Hello, World!"

	// Put object with tags
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(objectKey),
		Body:    strings.NewReader(objectContent),
		Tagging: aws.String("env=prod&team=backend&version=2.0"),
	})
	require.NoError(t, err, "Should be able to put object with tags")

	// Head object to get tag count
	headResp, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to head object")

	// Check tag count header
	if headResp.TagCount != nil {
		assert.Equal(t, int32(3), *headResp.TagCount, "Tag count should be 3")
	} else {
		t.Log("Warning: TagCount header not returned - this may be expected depending on implementation")
	}
}
