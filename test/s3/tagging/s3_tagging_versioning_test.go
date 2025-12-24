package tagging

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This is the fix for GitHub issue #7868 where tagging failed with "no entry is found in filer store"
// TestPutObjectTaggingOnVersionedBucket tests setting tags on objects in a versioned bucket
func TestPutObjectTaggingOnVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Put object in versioned bucket
	objectKey := "versioned-object-with-tags"
	objectContent := "Hello, Versioned World!"
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Body:   strings.NewReader(objectContent),
		Key:    aws.String(objectKey),
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "Should be able to put object in versioned bucket")

	// Set tags on the object in versioned bucket
	_, err = client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{
					Key:   aws.String("env"),
					Value: aws.String("production"),
				},
				{
					Key:   aws.String("team"),
					Value: aws.String("platform"),
				},
			},
		},
	})
	require.NoError(t, err, "Should be able to put tags on versioned object")

	// Get the tags back
	tagResp, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get tags from versioned object")

	// Verify tags
	assert.Len(t, tagResp.TagSet, 2, "Should have 2 tags")
	tagMap := make(map[string]string)
	for _, tag := range tagResp.TagSet {
		tagMap[*tag.Key] = *tag.Value
	}
	assert.Equal(t, "production", tagMap["env"], "env tag should be 'production'")
	assert.Equal(t, "platform", tagMap["team"], "team tag should be 'platform'")
}

// TestPutObjectTaggingOnSpecificVersionInVersionedBucket tests setting tags on a specific version
func TestPutObjectTaggingOnSpecificVersionInVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Create multiple versions of the object
	objectKey := "multi-version-object"
	version1Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Body:   strings.NewReader("Version 1"),
		Key:    aws.String(objectKey),
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	// Small delay to ensure different version IDs
	time.Sleep(50 * time.Millisecond)
	versionId1 := *version1Resp.VersionId

	version2Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Body:   strings.NewReader("Version 2"),
		Key:    aws.String(objectKey),
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)
	versionId2 := *version2Resp.VersionId

	// Set tags on version 1
	_, err = client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{
					Key:   aws.String("version"),
					Value: aws.String("v1"),
				},
			},
		},
	})
	require.NoError(t, err, "Should be able to put tags on specific version 1")

	// Set tags on version 2
	_, err = client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{
					Key:   aws.String("version"),
					Value: aws.String("v2"),
				},
			},
		},
	})
	require.NoError(t, err, "Should be able to put tags on specific version 2")

	// Get tags from version 1
	tagResp1, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
	})
	require.NoError(t, err, "Should be able to get tags from version 1")
	assert.Len(t, tagResp1.TagSet, 1, "Version 1 should have 1 tag")
	assert.Equal(t, "v1", *tagResp1.TagSet[0].Value, "Version 1 tag value should be 'v1'")

	// Get tags from version 2
	tagResp2, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
	})
	require.NoError(t, err, "Should be able to get tags from version 2")
	assert.Len(t, tagResp2.TagSet, 1, "Version 2 should have 1 tag")
	assert.Equal(t, "v2", *tagResp2.TagSet[0].Value, "Version 2 tag value should be 'v2'")
}

// TestDeleteObjectTaggingOnVersionedBucket tests deleting tags from versioned objects
func TestDeleteObjectTaggingOnVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Put object with tags in versioned bucket
	objectKey := "versioned-object-tag-delete"
	objectContent := "Hello, Delete Tags!"
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Body:    strings.NewReader(objectContent),
		Key:     aws.String(objectKey),
		Bucket:  aws.String(bucketName),
		Tagging: aws.String("env=dev&purpose=testing"),
	})
	require.NoError(t, err)

	// Verify tags exist
	tagResp, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.Len(t, tagResp.TagSet, 2, "Should have 2 tags before deletion")

	// Delete tags from the versioned object
	_, err = client.DeleteObjectTagging(context.TODO(), &s3.DeleteObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to delete tags from versioned object")

	// Verify tags are deleted
	tagResp, err = client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err)
	assert.Len(t, tagResp.TagSet, 0, "Should have 0 tags after deletion")
}

// TestDeleteObjectTaggingOnSpecificVersionInVersionedBucket tests deleting tags on a specific version
func TestDeleteObjectTaggingOnSpecificVersionInVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Create two versions with tags
	objectKey := "versioned-multi-delete-tags"
	version1Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Body:    strings.NewReader("Version 1"),
		Key:     aws.String(objectKey),
		Bucket:  aws.String(bucketName),
		Tagging: aws.String("version=v1&keep=true"),
	})
	require.NoError(t, err)

	// Small delay to ensure different version IDs
	time.Sleep(50 * time.Millisecond)
	versionId1 := *version1Resp.VersionId

	version2Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Body:    strings.NewReader("Version 2"),
		Key:     aws.String(objectKey),
		Bucket:  aws.String(bucketName),
		Tagging: aws.String("version=v2&keep=true"),
	})
	require.NoError(t, err)
	versionId2 := *version2Resp.VersionId

	// Delete tags from version 1 only
	_, err = client.DeleteObjectTagging(context.TODO(), &s3.DeleteObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
	})
	require.NoError(t, err, "Should be able to delete tags from version 1")

	// Verify version 1 has no tags
	tagResp1, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
	})
	require.NoError(t, err)
	assert.Len(t, tagResp1.TagSet, 0, "Version 1 should have 0 tags after deletion")

	// Verify version 2 still has tags
	tagResp2, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
	})
	require.NoError(t, err)
	assert.Len(t, tagResp2.TagSet, 2, "Version 2 should still have 2 tags")
}

// TestGetObjectTaggingOnVersionedBucket tests retrieving tags from versioned objects
func TestGetObjectTaggingOnVersionedBucket(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Create multiple versions
	objectKey := "versioned-object-get-tags"
	version1Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Body:    strings.NewReader("Version 1"),
		Key:     aws.String(objectKey),
		Bucket:  aws.String(bucketName),
		Tagging: aws.String("v=1&stage=dev"),
	})
	require.NoError(t, err)

	versionId1 := *version1Resp.VersionId
	time.Sleep(50 * time.Millisecond)

	version2Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Body:    strings.NewReader("Version 2"),
		Key:     aws.String(objectKey),
		Bucket:  aws.String(bucketName),
		Tagging: aws.String("v=2&stage=prod"),
	})
	require.NoError(t, err)

	versionId2 := *version2Resp.VersionId

	// Get tags from specific versions
	tagResp1, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
	})
	require.NoError(t, err, "Should be able to get tags from version 1")
	assert.Len(t, tagResp1.TagSet, 2, "Version 1 should have 2 tags")
	tagMap1 := make(map[string]string)
	for _, tag := range tagResp1.TagSet {
		tagMap1[*tag.Key] = *tag.Value
	}
	assert.Equal(t, "1", tagMap1["v"], "Version 1 should have v=1")
	assert.Equal(t, "dev", tagMap1["stage"], "Version 1 should have stage=dev")

	tagResp2, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
	})
	require.NoError(t, err, "Should be able to get tags from version 2")
	assert.Len(t, tagResp2.TagSet, 2, "Version 2 should have 2 tags")
	tagMap2 := make(map[string]string)
	for _, tag := range tagResp2.TagSet {
		tagMap2[*tag.Key] = *tag.Value
	}
	assert.Equal(t, "2", tagMap2["v"], "Version 2 should have v=2")
	assert.Equal(t, "prod", tagMap2["stage"], "Version 2 should have stage=prod")

	// Get tags from latest version (should be version 2)
	tagRespLatest, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	require.NoError(t, err, "Should be able to get tags from latest version")
	assert.Len(t, tagRespLatest.TagSet, 2, "Latest version should have 2 tags")
	tagMapLatest := make(map[string]string)
	for _, tag := range tagRespLatest.TagSet {
		tagMapLatest[*tag.Key] = *tag.Value
	}
	assert.Equal(t, "2", tagMapLatest["v"], "Latest version should have v=2")
	assert.Equal(t, "prod", tagMapLatest["stage"], "Latest version should have stage=prod")
}

// TestModifyTagsOnVersionedObject tests changing tags on different versions independently
func TestModifyTagsOnVersionedObject(t *testing.T) {
	client := getS3Client(t)
	bucketName := createVersionedTestBucket(t, client)
	defer cleanupTestBucket(t, client, bucketName)

	// Create version 1
	objectKey := "versioned-modify-tags"
	version1Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Body:   strings.NewReader("Version 1"),
		Key:    aws.String(objectKey),
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	versionId1 := *version1Resp.VersionId
	time.Sleep(50 * time.Millisecond)

	// Create version 2
	version2Resp, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
		Body:   strings.NewReader("Version 2"),
		Key:    aws.String(objectKey),
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err)

	versionId2 := *version2Resp.VersionId

	// Add tags to version 1
	_, err = client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{
					Key:   aws.String("status"),
					Value: aws.String("old"),
				},
			},
		},
	})
	require.NoError(t, err)

	// Modify tags on version 1 (replace status and add new tag)
	_, err = client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{
					Key:   aws.String("status"),
					Value: aws.String("archived"),
				},
				{
					Key:   aws.String("archived-date"),
					Value: aws.String("2024-01-01"),
				},
			},
		},
	})
	require.NoError(t, err)

	// Add tags to version 2
	_, err = client.PutObjectTagging(context.TODO(), &s3.PutObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{
					Key:   aws.String("status"),
					Value: aws.String("current"),
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify final state
	tagResp1, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId1),
	})
	require.NoError(t, err)
	assert.Len(t, tagResp1.TagSet, 2, "Version 1 should have 2 tags after modification")

	tagResp2, err := client.GetObjectTagging(context.TODO(), &s3.GetObjectTaggingInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(objectKey),
		VersionId: aws.String(versionId2),
	})
	require.NoError(t, err)
	assert.Len(t, tagResp2.TagSet, 1, "Version 2 should have 1 tag")
}

// createVersionedTestBucket creates a test bucket with versioning enabled
func createVersionedTestBucket(t *testing.T, client *s3.Client) string {
	bucketName := createTestBucket(t, client)

	// Enable versioning on the bucket
	_, err := client.PutBucketVersioning(context.TODO(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err, "Should be able to enable versioning")

	// Wait for versioning configuration to be applied
	time.Sleep(100 * time.Millisecond)

	return bucketName
}
